# local-first-outbox-sync

![Node >=20](https://img.shields.io/badge/node-%3E%3D20-339933.svg) ![Zero Dependencies](https://img.shields.io/badge/dependencies-0-10b981.svg)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Zero-dependency local-first transactional outbox queue and idempotent reconnect sync engine designed for retail counters, mobile field apps, and unstable 4G/Wi-Fi environments.

## The problem it solves

Cloud-only point-of-sale and COD checkout apps fail in two ways when a connection drops:

1. **Blocked writes:** The register freezes at peak hour while waiting on a remote HTTP response.
2. **Duplicate writes on retry:** A request succeeds on the server, the ACK drops on shaky 4G, and the client retries the same ticket twice, double-charging inventory or shipping two orders.

This engine persists every local write to an append-only journal with a deterministic SHA-256 `idempotencyKey`, survives torn writes during abrupt power loss, and reconciles HTTP `409` responses as clean already-committed acknowledgments when connectivity returns.

## Architecture

```mermaid
flowchart LR
    A["Local Sale / Action"] --> B["Append-Only Outbox Log"]
    B --> C{"Network Online?"}
    C -- "No (Wi-Fi OFF)" --> D["Continue Local Operations (0 Lag)"]
    C -- "Yes" --> E["FIFO Sync Worker"]
    E --> F["Remote Server (Unique idempotencyKey)"]
    F -- "201 Created" --> G["Write Local ACK"]
    F -- "409 Already Committed (Dropped ACK)" --> G
    F -- "Timeout / 5xx" --> H["Stop Batch & Preserve Order"]
```

## Quick Start

```bash
git clone https://github.com/JARMID/local-first-outbox-sync.git
cd local-first-outbox-sync
npm test
```

## License

MIT © [Khalef Abdelmadjid (@JARMID)](https://github.com/JARMID)
