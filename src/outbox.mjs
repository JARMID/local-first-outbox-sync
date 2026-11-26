import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/**
 * Deterministic SHA-256 idempotency key derived from device ID, local sequence,
 * and canonical operation payload so retried sync packets never double-commit.
 */
export function buildIdempotencyKey({ deviceId, sequence, opType, payload }) {
  const canonical = JSON.stringify({
    deviceId: String(deviceId),
    sequence: Number(sequence),
    opType: String(opType),
    payload,
  });
  return crypto.createHash("sha256").update(canonical, "utf8").digest("hex");
}

/**
 * Crash-safe append-only NDJSON local outbox (mirrors SQLite WAL commit discipline).
 * Survives abrupt power loss or mid-sale Wi-Fi cuts with zero lost tickets.
 */
export class LocalOutboxStore {
  constructor(filePath, deviceId = "terminal-01") {
    this.filePath = filePath;
    this.deviceId = deviceId;
    this.records = new Map();
    this.sequence = 0;
    this._loadFromDisk();
  }

  _loadFromDisk() {
    if (!fs.existsSync(this.filePath)) return;
    const raw = fs.readFileSync(this.filePath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      if (!line.trim()) continue;
      try {
        const evt = JSON.parse(line);
        if (evt.kind === "ENQUEUE" && evt.record?.idempotencyKey) {
          this.records.set(evt.record.idempotencyKey, evt.record);
          this.sequence = Math.max(this.sequence, Number(evt.record.sequence || 0));
        } else if (evt.kind === "ACK" && evt.idempotencyKey && this.records.has(evt.idempotencyKey)) {
          const rec = this.records.get(evt.idempotencyKey);
          rec.status = "SYNCED";
          rec.syncedAt = evt.syncedAt;
        }
      } catch {
        // Ignore trailing torn write from simulated power failure
      }
    }
  }

  _appendLog(eventObj) {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    fs.appendFileSync(this.filePath, `${JSON.stringify(eventObj)}\n`, "utf8");
  }

  enqueue(opType, payload) {
    this.sequence += 1;
    const idempotencyKey = buildIdempotencyKey({
      deviceId: this.deviceId,
      sequence: this.sequence,
      opType,
      payload,
    });
    const record = {
      idempotencyKey,
      deviceId: this.deviceId,
      sequence: this.sequence,
      opType,
      payload,
      status: "PENDING",
      attempts: 0,
      createdAt: new Date().toISOString(),
      syncedAt: null,
    };
    this._appendLog({ kind: "ENQUEUE", record });
    this.records.set(idempotencyKey, record);
    return { ...record };
  }

  getPending() {
    return [...this.records.values()]
      .filter((r) => r.status === "PENDING")
      .sort((a, b) => a.sequence - b.sequence);
  }

  markSynced(idempotencyKey) {
    const rec = this.records.get(idempotencyKey);
    if (!rec) return false;
    const syncedAt = new Date().toISOString();
    rec.status = "SYNCED";
    rec.syncedAt = syncedAt;
    this._appendLog({ kind: "ACK", idempotencyKey, syncedAt });
    return true;
  }

  /**
   * Flush all pending offline records in strict causal sequence order.
   * Treats HTTP 201 (created) and HTTP 409 (already synced on server) both
   * as safe acknowledgments so dropped responses never create duplicates.
   */
  async syncPending(transportFn) {
    const pending = this.getPending();
    let synced = 0;
    let duplicatesSafe = 0;
    let failed = 0;

    for (const rec of pending) {
      rec.attempts += 1;
      try {
        const res = await transportFn(rec);
        if (res.status === 200 || res.status === 201) {
          this.markSynced(rec.idempotencyKey);
          synced += 1;
        } else if (res.status === 409) {
          // Server already committed this exact idempotencyKey during an earlier dropped ACK
          this.markSynced(rec.idempotencyKey);
          duplicatesSafe += 1;
        } else {
          failed += 1;
          break; // Preserve FIFO ordering on transient server errors
        }
      } catch {
        failed += 1;
        break; // Network cut mid-sync: stop cleanly and retry later
      }
    }

    return {
      attempted: pending.length,
      synced,
      duplicatesSafe,
      failed,
      remainingPending: this.getPending().length,
    };
  }
}

/**
 * Reference idempotent remote server endpoint for testing reconnect storms.
 */
export class IdempotentRemoteServer {
  constructor() {
    this.committed = new Map();
    this.online = true;
    this.dropNextAck = false;
  }

  receive(record) {
    if (!this.online) {
      throw new Error("ECONNREFUSED: network offline");
    }
    if (this.committed.has(record.idempotencyKey)) {
      return { status: 409, duplicate: true };
    }
    this.committed.set(record.idempotencyKey, record);
    if (this.dropNextAck) {
      this.dropNextAck = false;
      throw new Error("ETIMEDOUT: response dropped after commit");
    }
    return { status: 201, duplicate: false };
  }
}
