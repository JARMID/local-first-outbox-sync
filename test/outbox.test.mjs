import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalOutboxStore, IdempotentRemoteServer } from "../src/outbox.mjs";

function tempLogPath(name) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "outbox-sync-"));
  return path.join(dir, `${name}.ndjson`);
}

test("records offline sales during Wi-Fi outage, survives restart, and syncs once on reconnect", async () => {
  const logFile = tempLogPath("pos_outage");
  const server = new IdempotentRemoteServer();
  server.online = false;

  const store = new LocalOutboxStore(logFile, "caisse-01");
  store.enqueue("POS_SALE", { ticketId: "T-001", totalMinor: 1330 });
  store.enqueue("POS_SALE", { ticketId: "T-002", totalMinor: 2450 });
  store.enqueue("POS_SALE", { ticketId: "T-003", totalMinor: 650 });

  // Sync attempt while offline fails safely without dropping any ticket
  const offlineSync = await store.syncPending((rec) => server.receive(rec));
  assert.equal(offlineSync.synced, 0);
  assert.equal(offlineSync.remainingPending, 3);

  // Simulate abrupt power loss & torn final line on disk, then reboot terminal
  fs.appendFileSync(logFile, '{"kind":"ENQUEUE","broken_json', "utf8");
  const rebooted = new LocalOutboxStore(logFile, "caisse-01");
  assert.equal(rebooted.getPending().length, 3);

  // Network returns
  server.online = true;
  const onlineSync = await rebooted.syncPending((rec) => server.receive(rec));
  assert.equal(onlineSync.synced, 3);
  assert.equal(onlineSync.remainingPending, 0);
  assert.equal(server.committed.size, 3);
});

test("dropped ACK after server commit never creates duplicate order on retry", async () => {
  const logFile = tempLogPath("dropped_ack");
  const server = new IdempotentRemoteServer();
  const store = new LocalOutboxStore(logFile, "caisse-02");

  store.enqueue("POS_SALE", { ticketId: "T-100", totalMinor: 8200 });
  store.enqueue("POS_SALE", { ticketId: "T-101", totalMinor: 1200 });

  // First ticket commits on server, then Wi-Fi drops before ACK returns to client
  server.dropNextAck = true;
  const pass1 = await store.syncPending((rec) => server.receive(rec));
  assert.equal(pass1.failed, 1);
  assert.equal(pass1.remainingPending, 2);
  assert.equal(server.committed.size, 1);

  // Retry after reconnect: T-100 gets HTTP 409 (acknowledged cleanly), T-101 gets HTTP 201
  const pass2 = await store.syncPending((rec) => server.receive(rec));
  assert.equal(pass2.duplicatesSafe, 1);
  assert.equal(pass2.synced, 1);
  assert.equal(pass2.remainingPending, 0);
  assert.equal(server.committed.size, 2);
});
