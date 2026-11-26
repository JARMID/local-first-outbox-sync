import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/**
 * Recursively sort plain-object keys so semantically identical payloads
 * always hash to the exact same canonical JSON representation.
 */
export function canonicalizeValue(value) {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    return value.map(canonicalizeValue);
  }
  if (value !== null && typeof value === "object") {
    const sorted = Object.create(null);
    for (const key of Object.keys(value).sort()) {
      if (value[key] !== undefined) {
        sorted[key] = canonicalizeValue(value[key]);
      }
    }
    return sorted;
  }
  return value;
}

export function hashCanonicalPayload(payload) {
  const serialized = JSON.stringify(canonicalizeValue(payload)) ?? "null";
  return crypto.createHash("sha256").update(serialized, "utf8").digest("hex");
}

/**
 * Deterministic SHA-256 idempotency key derived from device ID, local sequence,
 * and canonical operation payload so retried sync packets never double-commit.
 */
export function buildIdempotencyKey({ deviceId, sequence, opType, payload }) {
  if (typeof deviceId !== "string" || !deviceId.trim()) {
    throw new Error("deviceId must be a non-empty string");
  }
  if (!Number.isSafeInteger(sequence) || sequence <= 0) {
    throw new RangeError("sequence must be a positive safe integer");
  }
  if (typeof opType !== "string" || !opType.trim()) {
    throw new Error("opType must be a non-empty string");
  }
  const canonical = JSON.stringify({
    deviceId: deviceId.trim(),
    opType: opType.trim(),
    payload: canonicalizeValue(payload),
    sequence,
  });
  return crypto.createHash("sha256").update(canonical, "utf8").digest("hex");
}

/**
 * Crash-safe append-only NDJSON local outbox (mirrors SQLite WAL commit discipline).
 * Truncates trailing torn power-loss bytes on startup, fsyncs every append before
 * mutating in-memory state, and guards sync flushes with a single-flight mutex.
 */
export class LocalOutboxStore {
  constructor(filePath, deviceId = "terminal-01") {
    if (typeof filePath !== "string" || !filePath.trim()) {
      throw new Error("filePath is required");
    }
    if (typeof deviceId !== "string" || !deviceId.trim()) {
      throw new Error("deviceId must be a non-empty string");
    }
    this.filePath = filePath;
    this.deviceId = deviceId.trim();
    this.records = new Map();
    this.sequence = 0;
    this._syncInFlight = false;
    this._loadFromDisk();
  }

  _loadFromDisk() {
    if (!fs.existsSync(this.filePath)) return;
    const buf = fs.readFileSync(this.filePath);
    let offset = 0;
    let validEndOffset = 0;
    let needsTruncate = false;

    while (offset < buf.length) {
      const nlIdx = buf.indexOf(0x0a, offset); // '\n'
      const lineEnd = nlIdx === -1 ? buf.length : nlIdx;
      const lineStr = buf.subarray(offset, lineEnd).toString("utf8").replace(/\r$/, "");
      const nextOffset = nlIdx === -1 ? buf.length : nlIdx + 1;

      if (!lineStr.trim()) {
        validEndOffset = nextOffset;
        offset = nextOffset;
        continue;
      }

      try {
        const evt = JSON.parse(lineStr);
        if (evt.kind === "ENQUEUE" && evt.record?.idempotencyKey) {
          this.records.set(evt.record.idempotencyKey, structuredClone(evt.record));
          this.sequence = Math.max(this.sequence, Number(evt.record.sequence || 0));
        } else if (evt.kind === "ACK" && evt.idempotencyKey && this.records.has(evt.idempotencyKey)) {
          const rec = this.records.get(evt.idempotencyKey);
          rec.status = "SYNCED";
          rec.syncedAt = evt.syncedAt;
        } else if (
          evt.kind === "DEAD_LETTER" &&
          evt.idempotencyKey &&
          this.records.has(evt.idempotencyKey)
        ) {
          const rec = this.records.get(evt.idempotencyKey);
          rec.status = "DEAD_LETTER";
          rec.deadLetterReason = evt.reason || "permanent_4xx";
          rec.deadLetteredAt = evt.deadLetteredAt;
        }
        validEndOffset = nextOffset;
        if (nlIdx === -1) {
          // Valid JSON without trailing newline: normalize so next append starts on a clean line
          needsTruncate = true;
        }
      } catch {
        // Trailing torn write from abrupt power failure: stop and truncate back to validEndOffset
        needsTruncate = true;
        break;
      }
      offset = nextOffset;
    }

    if (needsTruncate) {
      const fd = fs.openSync(this.filePath, "r+");
      try {
        fs.ftruncateSync(fd, validEndOffset);
        if (validEndOffset > 0 && buf[validEndOffset - 1] !== 0x0a) {
          fs.writeSync(fd, Buffer.from("\n", "utf8"), 0, 1, validEndOffset);
        }
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
    }
  }

  _appendLog(eventObj) {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    const line = `${JSON.stringify(eventObj)}\n`;
    const fd = fs.openSync(this.filePath, "a");
    try {
      fs.writeSync(fd, line, null, "utf8");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  }

  enqueue(opType, payload) {
    const nextSequence = this.sequence + 1;
    const clonedPayload = structuredClone(payload);
    const idempotencyKey = buildIdempotencyKey({
      deviceId: this.deviceId,
      sequence: nextSequence,
      opType,
      payload: clonedPayload,
    });
    const record = {
      idempotencyKey,
      deviceId: this.deviceId,
      sequence: nextSequence,
      opType: opType.trim(),
      payload: clonedPayload,
      status: "PENDING",
      attempts: 0,
      createdAt: new Date().toISOString(),
      syncedAt: null,
    };
    // Persist to disk BEFORE mutating in-memory state
    this._appendLog({ kind: "ENQUEUE", record });
    this.sequence = nextSequence;
    this.records.set(idempotencyKey, record);
    return structuredClone(record);
  }

  getPending() {
    return [...this.records.values()]
      .filter((r) => r.status === "PENDING")
      .sort((a, b) => a.sequence - b.sequence)
      .map((r) => structuredClone(r));
  }

  getDeadLetters() {
    return [...this.records.values()]
      .filter((r) => r.status === "DEAD_LETTER")
      .sort((a, b) => a.sequence - b.sequence)
      .map((r) => structuredClone(r));
  }

  markSynced(idempotencyKey) {
    const rec = this.records.get(idempotencyKey);
    if (!rec) return false;
    const syncedAt = new Date().toISOString();
    // Persist ACK to disk BEFORE mutating in-memory record
    this._appendLog({ kind: "ACK", idempotencyKey, syncedAt });
    rec.status = "SYNCED";
    rec.syncedAt = syncedAt;
    return true;
  }

  markDeadLetter(idempotencyKey, reason = "permanent_4xx") {
    const rec = this.records.get(idempotencyKey);
    if (!rec) return false;
    const deadLetteredAt = new Date().toISOString();
    this._appendLog({ kind: "DEAD_LETTER", idempotencyKey, reason, deadLetteredAt });
    rec.status = "DEAD_LETTER";
    rec.deadLetterReason = reason;
    rec.deadLetteredAt = deadLetteredAt;
    return true;
  }

  /**
   * Flush all pending offline records in strict causal sequence order.
   * Guarded by a single-flight mutex so overlapping timer/reconnect events
   * cannot race or double-send records.
   */
  async syncPending(transportFn) {
    if (this._syncInFlight) {
      return {
        busy: true,
        attempted: 0,
        synced: 0,
        duplicatesSafe: 0,
        deadLettered: 0,
        failed: 0,
        remainingPending: this.getPending().length,
      };
    }

    this._syncInFlight = true;
    try {
      const pending = this.getPending();
      let attempted = 0;
      let synced = 0;
      let duplicatesSafe = 0;
      let deadLettered = 0;
      let failed = 0;

      for (const snap of pending) {
        const liveRec = this.records.get(snap.idempotencyKey);
        if (!liveRec || liveRec.status !== "PENDING") continue;
        liveRec.attempts += 1;
        attempted += 1;

        try {
          const res = await transportFn(structuredClone(liveRec));
          if (res?.status === 200 || res?.status === 201) {
            this.markSynced(liveRec.idempotencyKey);
            synced += 1;
          } else if (res?.status === 409 && res?.duplicate === true) {
            // Only treat 409 as already-committed when server explicitly confirms duplicate=true
            this.markSynced(liveRec.idempotencyKey);
            duplicatesSafe += 1;
          } else if (res?.status === 400 || res?.status === 422) {
            // Quarantine permanently invalid payloads so they never head-of-line block valid sales
            this.markDeadLetter(liveRec.idempotencyKey, res.error || `http_${res.status}`);
            deadLettered += 1;
          } else {
            failed += 1;
            break; // Preserve FIFO ordering on transient 5xx / 429 / non-duplicate 409 conflicts
          }
        } catch {
          failed += 1;
          break; // Network cut mid-sync: stop cleanly and retry later
        }
      }

      return {
        busy: false,
        attempted,
        synced,
        duplicatesSafe,
        deadLettered,
        failed,
        remainingPending: this.getPending().length,
      };
    } finally {
      this._syncInFlight = false;
    }
  }
}

/**
 * Reference idempotent remote server endpoint for testing reconnect storms.
 * Rejects idempotencyKey reuse with a mismatched opType or payload using HTTP 422.
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
    const payloadHash = hashCanonicalPayload({ opType: record.opType, payload: record.payload });
    if (this.committed.has(record.idempotencyKey)) {
      const existing = this.committed.get(record.idempotencyKey);
      if (existing.payloadHash !== payloadHash) {
        return { status: 422, duplicate: false, error: "idempotency_key_payload_mismatch" };
      }
      return { status: 409, duplicate: true };
    }
    this.committed.set(record.idempotencyKey, {
      record: structuredClone(record),
      payloadHash,
    });
    if (this.dropNextAck) {
      this.dropNextAck = false;
      throw new Error("ETIMEDOUT: response dropped after commit");
    }
    return { status: 201, duplicate: false };
  }
}
