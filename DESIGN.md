# The Ledger: PostgreSQL Schema Design Justification

> [!IMPORTANT]
> **This document is MERGED into [FINAL_REPORT.md](FINAL_REPORT.md).** Please refer to the final report for the consolidated architectural and design analysis.

This document justifies every column across the four foundational Phase 1 tables. Because Event Sourcing uses events as the sole source of truth in a CQRS architecture, the schema must be robust, deterministic, and rigorously defended.

---

## 1. Table: `events`
The append-only log of domain events. **Explicit Immutability Enforcement**: The `events` table assumes strictly append-only behavior. While applications enforce this conventionally, production environments will solidify this immutability via role permissions (e.g., `REVOKE UPDATE, DELETE ON events FROM application_user`).

*   **`event_id`** (`UUID PRIMARY KEY DEFAULT gen_random_uuid()`)
    Uniquely identifies an event across the entire system. Prevents theoretical duplicates during complex troubleshooting.
*   **`stream_id`** (`TEXT NOT NULL`)
    Identifies the aggregate instance this event belongs to (e.g., `loan-123`). Required for recovering a specific aggregate's state via deterministic replay.
*   **`stream_position`** (`BIGINT NOT NULL`)
    Strictly increasing sequence number within a stream. Ensures deterministic replay and is the core of optimistic concurrency control (paired with `stream_id` in a unique constraint to guarantee no gaps or duplicates). **Note: The `stream_position` assignment within the transactional iteration is fully authoritative for local stream ordering DB-level guarantees.**
*   **`global_position`** (`BIGINT GENERATED ALWAYS AS IDENTITY`)
    Provides a consistent total ordering of all committed events. Crucial for asynchronous projections to track their read watermarks (`last_position`) efficiently on a global scale.
*   **`event_type`** (`TEXT NOT NULL`)
    Identifies the semantic meaning of the opaque payload (e.g., `CreditAnalysisCompleted`). Used by aggregates to route to the correct `apply_*` handler and preserves the canonical event catalogue.
*   **`event_version`** (`SMALLINT NOT NULL DEFAULT 1`)
    Tracks schema evolution for a specific `event_type`. Informs the read-path deserializer which upcaster hook to apply, preserving backwards compatibility.
*   **`payload`** (`JSONB NOT NULL`)
    The opaque, schema-agnostic domain event data. `JSONB` provides indexing support and schema flexibility without DDL migrations, crucial for supporting diverse multi-aggregate streams.
*   **`metadata`** (`JSONB NOT NULL DEFAULT '{}'::jsonb`)
    Stores out-of-band context such as `correlation_id` (for UI auto-refresh and causal tracing), `causation_id`, and idempotency keys, keeping domain payloads pure.
*   **`recorded_at`** (`TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()`)
    The authoritative server-side timestamp of ingestion. `clock_timestamp()` prevents transaction-start stagnation, providing true chronological capture for temporal queries and audit trails.

---

## 2. Table: `event_streams`
Stream metadata and current version tracking for concurrency locks.

*   **`stream_id`** (`TEXT PRIMARY KEY`)
    The canonical identifier of the aggregate instance. Used for existence checks and stream-level locking (`SELECT ... FOR UPDATE`).
*   **`aggregate_type`** (`TEXT NOT NULL`)
    Identifies the domain model this stream represents (e.g., `LoanApplication`). Useful for database analytics and system-wide aggregate introspection.
*   **`current_version`** (`BIGINT NOT NULL DEFAULT 0`)
    The highest `stream_position` appended to this stream. Optimistic concurrency control strictly requires checking `expected_version` against this column, avoiding expensive `MAX()` aggregations over the `events` table.
*   **`created_at`** (`TIMESTAMPTZ NOT NULL DEFAULT NOW()`)
    The timestamp when the stream was first initialized. Used for aging algorithms, stream lifespan analytics, and general auditing.
*   **`archived_at`** (`TIMESTAMPTZ`)
    If set, explicitly marks the stream as historical. The `EventStore` raises a domain error on append attempts here, permanently blocking writes while allowing reads for compliance audits.
*   **`metadata`** (`JSONB NOT NULL DEFAULT '{}'::jsonb`)
    Stores stream-level metadata (e.g., access control tags or aggregate snapshots) independent of individual events.

---

## 3. Table: `projection_checkpoints`
Persistent tracking for asynchronous projection daemons.

*   **`projection_name`** (`TEXT PRIMARY KEY`)
    The unique identifier for a specific read-model daemon (e.g., `loan_dashboard`). Atomically updated during projection runs.
*   **`last_position`** (`BIGINT NOT NULL DEFAULT 0`)
    The `global_position` watermark of the last successfully processed event. Enables the projection to resume reliably after crashes without reprocessing the entire log, ensuring exactly-once projection effects.
*   **`updated_at`** (`TIMESTAMPTZ NOT NULL DEFAULT NOW()`)
    Tracks the freshness of the projection. Necessary for calculating projection lag observability metrics.

---

## 4. Table: `outbox`
Transactional outbox pattern for guaranteed message delivery.

*   **`id`** (`UUID PRIMARY KEY DEFAULT gen_random_uuid()`)
    Uniquely identifies the outbox message. Required for idempotent publishing retries by background workers.
*   **`event_id`** (`UUID NOT NULL REFERENCES events(event_id)`)
    Links the message back to the core event. Ensures referential integrity and facilitates direct payload fetching if needed during complex dispatching.
*   **`destination`** (`TEXT NOT NULL`)
    The logical routing target (e.g., `event_bus`, `kafka_topic`) mapped by the publisher dispatcher.
*   **`payload`** (`JSONB NOT NULL`)
    The pre-serialized message structure. Decouples the publisher worker from application decoding logic.
*   **`created_at`** (`TIMESTAMPTZ NOT NULL DEFAULT NOW()`)
    The exact time the outbox row was committed. Useful for calculating dispatch latency and SLA breaches.
*   **`published_at`** (`TIMESTAMPTZ`)
    Marks successful delivery when set, effectively fulfilling the outbox intention. Claimed by background workers using `SELECT ... FOR UPDATE SKIP LOCKED`.
*   **`attempts`** (`SMALLINT NOT NULL DEFAULT 0`)
    Tracks delivery retry counts. Vital for executing exponential backoff logic and routing poison messages to a dead-letter queue.
