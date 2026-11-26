import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  buildIdempotencyKey,
  LocalOutboxStore,
  IdempotentRemoteServer,
} from "../src/outbox.mjs";

function tempLogPath(name) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "outbox-sync-"));
  return path.join(dir, `${name}.ndjson`);
}

test("buildIdempotencyKey is invariant to object key ordering", () => {
  const k1 = buildIdempotencyKey({
    deviceId: "caisse-01",
    sequence: 1,
    opType: "POS_SALE",
    payload: { totalMinor: 1330, ticketId: "T-001", meta: { b: 2, a: 1 } },
  });
  const k2 = buildIdempotencyKey({
    deviceId: "caisse-01",
    sequence: 1,
    opType: "POS_SALE",
    payload: { meta: { a: 1, b: 2 }, ticketId: "T-001", totalMinor: 1330 },
  });
  assert.equal(k1, k2);
  assert.equal(k1.length, 64);
});

test("survives Wi-Fi outage, truncates trailing torn power-loss bytes, and keeps subsequent appends intact", async () => {
  const logFile = tempLogPath("pos_outage");
  const server = new IdempotentRemoteServer();
  server.online = false;

  const store = new LocalOutboxStore(logFile, "caisse-01");
  store.enqueue("POS_SALE", { ticketId: "T-001", totalMinor: 1330 });
  store.enqueue("POS_SALE", { ticketId: "T-002", totalMinor: 2450 });
  store.enqueue("POS_SALE", { ticketId: "T-003", totalMinor: 650 });

  const offlineSync = await store.syncPending((rec) => server.receive(rec));
  assert.equal(offlineSync.synced, 0);
  assert.equal(offlineSync.remainingPending, 3);

  // Simulate abrupt power loss & torn final line on disk, then reboot terminal
  fs.appendFileSync(logFile, '{"kind":"ENQUEUE","broken_json', "utf8");
  const rebooted = new LocalOutboxStore(logFile, "caisse-01");
  assert.equal(rebooted.getPending().length, 3);

  // Enqueue a 4th ticket AFTER recovery and reboot AGAIN to prove torn bytes were truncated cleanly
  rebooted.enqueue("POS_SALE", { ticketId: "T-004", totalMinor: 990 });
  const rebootedAgain = new LocalOutboxStore(logFile, "caisse-01");
  assert.equal(rebootedAgain.getPending().length, 4);

  // Network returns
  server.online = true;
  const onlineSync = await rebootedAgain.syncPending((rec) => server.receive(rec));
  assert.equal(onlineSync.synced, 4);
  assert.equal(onlineSync.remainingPending, 0);
  assert.equal(server.committed.size, 4);
});

test("dropped ACK after commit never duplicates, non-duplicate 409 is not dropped, and permanent 422 dead-letters safely", async () => {
  const logFile = tempLogPath("dropped_ack");
  const server = new IdempotentRemoteServer();
  const store = new LocalOutboxStore(logFile, "caisse-02");

  const t1 = store.enqueue("POS_SALE", { ticketId: "T-100", totalMinor: 8200 });
  store.enqueue("POS_SALE", { ticketId: "T-101", totalMinor: 1200 });

  // First ticket commits on server, then Wi-Fi drops before ACK returns
  server.dropNextAck = true;
  const pass1 = await store.syncPending((rec) => server.receive(rec));
  assert.equal(pass1.failed, 1);
  assert.equal(pass1.remainingPending, 2);
  assert.equal(server.committed.size, 1);

  // Non-duplicate 409 (business conflict) must NOT mark record synced
  const conflictPass = await store.syncPending(async () => ({ status: 409, duplicate: false }));
  assert.equal(conflictPass.failed, 1);
  assert.equal(conflictPass.remainingPending, 2);

  // Reusing same idempotencyKey with a mutated payload returns HTTP 422 on server
  const tamperedRes = server.receive({ ...t1, payload: { ticketId: "T-100", totalMinor: 1 } });
  assert.equal(tamperedRes.status, 422);
  assert.equal(tamperedRes.error, "idempotency_key_payload_mismatch");

  // Retry cleanly against real server: T-100 gets 409 duplicate=true, T-101 gets 201
  const pass2 = await store.syncPending((rec) => server.receive(rec));
  assert.equal(pass2.duplicatesSafe, 1);
  assert.equal(pass2.synced, 1);
  assert.equal(pass2.remainingPending, 0);
  assert.equal(server.committed.size, 2);

  // Permanent 422 payload gets quarantined to DEAD_LETTER so later valid tickets still flush
  store.enqueue("BAD_OP", { malformed: true });
  store.enqueue("POS_SALE", { ticketId: "T-102", totalMinor: 500 });
  const mixedPass = await store.syncPending((rec) => {
    if (rec.opType === "BAD_OP") return { status: 422, error: "schema_invalid" };
    return server.receive(rec);
  });
  assert.equal(mixedPass.deadLettered, 1);
  assert.equal(mixedPass.synced, 1);
  assert.equal(mixedPass.remainingPending, 0);
  assert.equal(store.getDeadLetters().length, 1);
});

test("single-flight mutex prevents overlapping concurrent syncPending calls from racing", async () => {
  const logFile = tempLogPath("mutex_test");
  const server = new IdempotentRemoteServer();
  const store = new LocalOutboxStore(logFile, "caisse-03");
  store.enqueue("POS_SALE", { ticketId: "T-200", totalMinor: 1500 });

  let calls = 0;
  const slowTransport = async (rec) => {
    calls += 1;
    await new Promise((r) => setTimeout(r, 30));
    return server.receive(rec);
  };

  const [first, second] = await Promise.all([
    store.syncPending(slowTransport),
    store.syncPending(slowTransport),
  ]);
  assert.equal(first.busy, false);
  assert.equal(first.synced, 1);
  assert.equal(second.busy, true);
  assert.equal(calls, 1);
});
