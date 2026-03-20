# Domain Notes — The Ledger: Event Sourcing for Apex Financial Services

---

## 1. EDA vs. ES Distinction

> **Direct answer:** This pattern is **closer to EDA-style instrumentation than ES, but strictly speaking it is observability instrumentation rather than a true event-driven architecture.** It is not ES because the callbacks are not the source of truth — the mutable database row is. Events in ES ARE the state; callbacks are disposable side-effects that inform rather than constitute reality.

### The Callback Pattern Is Neither Pure EDA nor ES

A component using LangChain trace callbacks to capture event-like data (e.g., `on_llm_start`, `on_chain_end`) is **observability instrumentation**, not Event-Driven Architecture (EDA) and certainly not Event Sourcing (ES). The distinction matters:

| Dimension | LangChain Callbacks | Event-Driven Architecture (EDA) | Event Sourcing (ES) |
|---|---|---|---|
| **Purpose** | Side-effect observation / logging | Decoupled inter-service communication | System of record — **events ARE the state** |
| **Event semantics** | Ephemeral diagnostic data; shape controlled by framework | Integration events between bounded contexts | Domain events that *constitute* state transitions |
| **Ordering guarantee** | Temporal within a single trace only | At-least-once, ordering not guaranteed across consumers | Strict per-stream causal ordering via version numbers |
| **Replayability** | No — callbacks are fire-and-forget | Possible if broker retains events, but not primary design goal | **Mandatory** — replaying the log reconstructs any past state |
| **Source of truth** | Current model state (database row) is truth | Current model state is truth; events are notifications | **Events ARE the state.** The event log is the single source of truth. Projections are disposable, rebuildable read models. A missing event is not an observability gap — **it is data corruption.** |

LangChain callbacks are closest to the **Observer pattern** — they notify interested listeners about things that have already happened inside a pipeline, but the pipeline's mutable state (the chain's output, the LLM response) is the source of truth. No one rebuilds agent state by replaying callbacks. If a callback is dropped, you lose a log line. If an event is lost in an event-sourced system, **you have data corruption** — the aggregate's state can no longer be reconstructed, and every downstream projection diverges from reality.

### What Changes If We Redesign with The Ledger

In The Ledger, **events are persisted in the event store as the system of record**, and all agent state, projections, and decisions are derived from this immutable log. There is no separate database of truth — the event stream is the only authority.

If the `CreditAnalysis` agent currently works like this:

```
1. Agent loads applicant data from DB (mutable read)
2. Agent calls LLM → receives risk assessment
3. Agent writes risk_tier, limit to loan_applications table (mutable UPDATE)
4. Callback emits a trace log to observability backend
```

Under The Ledger, it becomes:

```
1. Agent replays AgentSession stream → emits AgentContextLoaded event
   (agent state is RECONSTRUCTED from events, not loaded from a mutable row)
2. CreditAnalysisRequested event is COMMITTED to the LoanApplication stream
   (write-before-execute: the intent is recorded before the LLM call begins)
3. Agent calls LLM → receives risk assessment
4. Agent appends CreditAnalysisCompleted event to its AgentSession stream
   (with model_version, input_data_hash, confidence_score — all immutable facts)
5. A projection asynchronously updates a read-model table for queries
```

**What exactly changes:**

- **Agent state is reconstructed from events, not from mutable database state.** When the `CreditAnalysis` agent starts, it does not `SELECT * FROM agent_state WHERE agent_id = ?`. It replays every event on its `AgentSession` stream — `AgentContextLoaded`, prior `CreditAnalysisCompleted` events — and rebuilds its in-memory state from that sequence. There is no authoritative database row. The event stream *is* the agent's memory. If you delete the agent's read-model table, you lose nothing — you replay and rebuild.
- **The database row disappears as source of truth.** The `loan_applications` table becomes a *projection* — a derived, disposable read model that can be rebuilt from scratch by replaying events. It is a cache for queries, not the system of record.
- **The callback disappears entirely.** There is no separate audit/observability channel because the event stream *is* the audit trail. The `CreditAnalysisCompleted` event serves triple duty: state transition record, audit entry, and integration event for downstream aggregates.
- **Temporal queries become trivial.** "What would the decision have been under last month's model?" is answered by replaying the `AgentSession` stream, substituting the `model_version` parameter, and re-running the projection to a specific point in time. With the callback model, this data was never captured with enough fidelity to answer the question.
- **Tamper evidence becomes structural.** The `AuditLedger` aggregate chains event hashes cryptographically. In the callback model, a bad actor can mutate the database row and delete the trace log — two independent systems to compromise. In ES, the event stream *is* the state; tampering breaks the hash chain and is detectable by any consumer.

**What we gain:** A single, append-only, immutable log that satisfies all four regulatory requirements (audit trail, point-in-time reconstruction, temporal queries, cryptographic integrity) without any additional infrastructure. Auditability is the architecture, not an annotation bolted on after the fact.

### Write-Before-Execute and Crash Recovery

The Ledger follows the **write-before-execute** pattern: events recording *intent* are committed to the store **before** the corresponding action is performed. This is not incidental — it is the mechanism that guarantees crash recovery and deterministic replay.

**Concrete example in the Apex scenario:**

When the `DecisionOrchestrator` needs to request credit analysis:

```
1. COMMIT CreditAnalysisRequested event to the LoanApplication stream
   → Event is now durable. If the system crashes here, the event exists.
2. THEN dispatch the actual work to the CreditAnalysis agent.
```

If the system crashes after step 1 but before step 2, the recovery path is deterministic: on restart, the system replays the `LoanApplication` stream, encounters the `CreditAnalysisRequested` event with no corresponding `CreditAnalysisCompleted`, and knows exactly what work remains. The event log is the recovery journal.

If step 2 had executed first (execute-before-write), a crash after the agent starts but before the event is committed would leave the system in an unrecoverable state: the agent performed work, but there is no record of it. The event stream and reality have diverged. **In an event-sourced system, this divergence is not a bug — it is data corruption.**

This pattern applies to every agent interaction in the Apex platform:

| Step | Event Committed | Action That Follows |
|---|---|---|
| Orchestrator assigns credit analysis | `CreditAnalysisRequested` | CreditAnalysis agent begins work |
| Orchestrator assigns fraud screening | `FraudScreeningRequested` (identified missing event) | FraudDetection agent begins work |
| Orchestrator assigns compliance check | `ComplianceCheckRequested` | ComplianceAgent begins work |
| Human officer claims a review | `HumanReviewStarted` (identified missing event) | Officer begins reviewing |

In the challenge implementation, this begins with `AgentSessionStarted` before any node work, so replay can resume from the last successful node. In every case: **commit the intent, then execute.** If the execution fails or the process crashes, the committed event provides a deterministic recovery point. The system never silently loses work.

---

## 2. The Aggregate Question

> **Direct answer:** The rejected alternative was merging `AgentSession` into `LoanApplication` (a "fat aggregate"). This boundary prevents **artificial write contention coupling between independent agents**, allowing parallel execution without serializing unrelated domain operations.

### The Chosen Boundaries

**An aggregate is a consistency boundary** — the unit within which invariants are enforced transactionally. Aggregate boundaries are not just domain modeling choices — **they are throughput decisions.** Each aggregate defines a concurrency boundary: writes are serialized via optimistic concurrency control. In a concurrent multi-agent system like Apex, choosing where to draw these boundaries directly determines how many agents can operate in parallel without contention.

1. **LoanApplication** (`loan-{application_id}`) — lifecycle of a single application (submission → decision → approval/decline). Low write concurrency: only the orchestrator and human reviewer write here, and they do so sequentially.
2. **AgentSession** (`agent-{agent_id}-{session_id}`) — actions of *one* AI agent instance during *one* work session. **Zero cross-agent contention**: each agent writes to its own stream. Four agents processing the same loan produce four independent, non-contending streams.
3. **ComplianceRecord** (`compliance-{application_id}`) — regulatory checks scoped to one application. Isolated from agent sessions so compliance checks do not serialize behind credit analysis or fraud screening.
4. **AuditLedger** (`audit-{entity_type}-{entity_id}`) — cross-cutting integrity chain per business entity. Append-only with no version-based concurrency — the hash chain provides ordering instead.

### An Alternative Boundary I Considered and Rejected

**Considered:** Merging `AgentSession` into `LoanApplication` — making every agent action (context loading, analysis, fraud screening) an event on the `LoanApplication` stream.

This is the intuitive "fat aggregate" design: "everything about this loan lives on one stream." I rejected it for a specific coupling problem:

**The coupling problem it creates: Aggregate contention under concurrent multi-agent processing — a throughput ceiling disguised as a modeling decision.**

In the Apex scenario, four agents work on the same loan application *simultaneously*. If all four agents append events to the same `LoanApplication` stream, every agent's `append_events` call must contend for the *same* optimistic concurrency version counter. Agent A completes credit analysis and appends at `expected_version=5`. Agent B, which was processing fraud screening in parallel, also tries to append at `expected_version=5`. One of them fails with a concurrency conflict — not because of a domain rule violation, but because of an *artificial serialization bottleneck* caused by the aggregate boundary.

This creates a cascade of problems:
- **False conflicts.** Credit analysis and fraud screening are domain-independent activities. They have no business invariant that requires them to be serialized, yet the shared stream forces serialization. The aggregate boundary has manufactured a coupling that does not exist in the domain.
- **Retry storms.** With four concurrent agents, the probability of conflict per append approaches 75% (3 out of 4 agents will conflict on any given round). Each retry re-reads the stream, re-validates, and re-appends — producing O(n²) load under contention.
- **Latency amplification.** Loan processing time is now dominated by agents backing off and retrying, not by actual analysis. In a regulated environment with SLA requirements, this is operationally unacceptable.
- **Throughput ceiling.** The system's maximum loan processing throughput becomes a function of stream contention, not compute capacity. Adding more agents to parallelize work *degrades* performance — the exact opposite of the intended design.

**Why the chosen boundary prevents this:** By giving each agent its own `AgentSession` stream (`agent-{agent_id}-{session_id}`), agents append events to *independent, non-contending* streams. The `DecisionOrchestrator` reads *from* those streams (via projections or direct reads) and appends `DecisionGenerated` to the `LoanApplication` stream only after all agent sessions complete. Concurrency within the `LoanApplication` stream is now limited to the orchestrator and human reviewer — a sequential, well-ordered handoff, not a parallel free-for-all.

The throughput profile is now O(1) per agent instead of O(n²) under the fat aggregate design. Four agents, forty agents — the streams are independent. While these four aggregates serve as the conceptual foundation, the challenge implementation further refines these boundaries into dedicated streams for specialized agent outputs (e.g., `credit-{id}`, `fraud-{id}`, `compliance-{id}`).

The `AuditLedger` aggregate then stitches a cross-stream causal narrative via `correlation_id` chains, giving regulators the unified view they need without coupling the operational write paths.

**Summary of the coupling problem prevented:** The chosen boundary prevents artificial write contention coupling between independent agents, allowing parallel execution without serializing unrelated domain operations. Credit analysis, fraud screening, and compliance checking proceed concurrently on separate streams — the aggregate boundary reflects the domain's natural independence.

---

## 3. Concurrency in Practice

> **Direct answer:** The event store rejects Agent B's write with an `OptimisticConcurrencyError`. Agent B must re-read the stream, check for domain conflicts, and retry with bounded exponential backoff. The event log — not mutable state — is what both agents read and write to. Events are the source of truth for the stream's version history.

### The Scenario

Two AI agents (Agent A and Agent B) simultaneously process the same loan application. Both read the `LoanApplication` stream and see it at version 3. Both prepare events and call `append_events` with `expected_version=3`.

### Exact Sequence of Operations

```
T0  Agent A reads LoanApplication stream "loan-123"
      → receives events [v1, v2, v3], current_version = 3

T1  Agent B reads LoanApplication stream "loan-123"
      → receives events [v1, v2, v3], current_version = 3

T2  Agent A calls: append_events(
      stream_id="loan-123",
      events=[CreditAnalysisRequested(...)],
      expected_version=3
    )

T3  Event store acquires a write lock on stream "loan-123"
    → Checks: current_version == expected_version (3 == 3) ✓
    → Appends event at version 4
    → Updates stream's current_version to 4
    → Releases lock
    → Returns Success(new_version=4) to Agent A

T4  Agent B calls: append_events(
      stream_id="loan-123",
      events=[FraudScreeningRequested(...)],
      expected_version=3
    )

T5  Event store acquires a write lock on stream "loan-123"
    → Checks: current_version == expected_version (3 == 4) ✗
       Current version is now 4 (from Agent A's write at T3),
       but Agent B expected 3.
    → Releases lock
    → Returns ConcurrencyError(
        stream_id="loan-123",
        expected_version=3,
        actual_version=4
      ) to Agent B
```

### What the Losing Agent Receives

**The losing agent receives an `OptimisticConcurrencyError` containing exactly three fields:**

- **`stream_id`**: `"loan-123"` — the stream it tried to write to
- **`expected_version`**: `3` — the version Agent B believed was current
- **`actual_version`**: `4` — the real current version (after Agent A's successful write)

### What Agent B Must Do Next

Agent B must perform the **read-decide-retry loop** with **bounded retries and exponential backoff** to prevent retry storms under sustained contention:

1. **Re-read the stream** from version 3 to current (version 4) to fetch the event(s) it missed — in this case, Agent A's `CreditAnalysisRequested` event.
2. **Re-evaluate its decision** in light of the new event(s). In this scenario, a `CreditAnalysisRequested` event does not conflict with a `FraudScreeningRequested` event — they are domain-compatible actions. So Agent B's intended event is still valid.
3. **Re-attempt the append** with `expected_version=4`.
4. **If it fails again**, apply exponential backoff (`base_delay * 2^attempt`, with jitter) before retrying. The retry count is bounded — after `MAX_RETRIES` failures, the operation is routed to a dead-letter queue for manual intervention.

**Why bounded retries with backoff, not unbounded retries:**

In the Apex scenario, if the `DecisionOrchestrator` is under high load (many loans being processed simultaneously), unbounded retries create a positive feedback loop: failed writes retry immediately, increasing contention, causing more failures, causing more retries. This is a retry storm. Exponential backoff with jitter breaks the feedback loop by spreading retries across time. The hard cap (`MAX_RETRIES = 5`) ensures that a persistently contending stream escalates to operator attention rather than consuming resources indefinitely.

```python
import random

MAX_RETRIES = 5
BASE_DELAY_MS = 50   # 50ms, 100ms, 200ms, 400ms, 800ms
MAX_DELAY_MS = 2000

async def append_with_retry(store, stream_id, make_events, expected_version):
    for attempt in range(MAX_RETRIES):
        try:
            return await store.append_events(
                stream_id=stream_id,
                events=make_events(),
                expected_version=expected_version,
            )
        except ConcurrencyError as e:
            # Fetch missed events since our last known version
            missed = await store.read_stream(
                stream_id, from_version=expected_version + 1
            )
            expected_version = e.actual_version

            # Domain-level conflict detection:
            # If any missed event invalidates our intent, abort immediately.
            # Do not retry — this is a real domain conflict, not a race.
            for evt in missed:
                if conflicts_with_our_intent(evt):
                    raise DomainConflictError(
                        f"Cannot proceed: conflicting event {evt.event_type} "
                        f"at version {evt.stream_version}"
                    )

            # Exponential backoff with jitter before retry
            if attempt < MAX_RETRIES - 1:
                delay_ms = min(BASE_DELAY_MS * (2 ** attempt), MAX_DELAY_MS)
                jitter_ms = random.uniform(0, delay_ms * 0.5)
                await asyncio.sleep((delay_ms + jitter_ms) / 1000)

    # Exhausted all retries — route to dead-letter queue for operator attention
    raise MaxRetriesExceeded(
        stream_id=stream_id,
        attempts=MAX_RETRIES,
        last_expected_version=expected_version,
        message=(
            f"Stream '{stream_id}' is under sustained contention. "
            f"Failed after {MAX_RETRIES} attempts. Routed to dead-letter queue."
        ),
    )
```

The critical insight: **not all concurrency conflicts are domain conflicts.** The event store rejects the write mechanically (version mismatch), but the *application* must decide whether the missed events semantically conflict with the intended action. Two independent agent tasks on the same loan rarely conflict at the domain level — which is precisely why the `AgentSession` aggregate boundary exists to avoid this scenario in the first place.

---

## 4. Projection Lag and Its Consequences

> **Direct answer:** The system guarantees correctness by either waiting for the projection to catch up or computing results directly from the event stream (a live fold) when the projection is stale. The UI always communicates staleness to the user — no silent stale reads. Projections are derived from the event log, which is the source of truth. **All projection handlers must be idempotent** to handle retries and crash recovery safely.

### The Problem

The `LoanApplication` projection (read model) is eventually consistent with ~200ms typical lag. A loan officer queries "available credit limit" immediately after an agent commits a `DisbursementCompleted` event (identified missing event). The projection has not yet processed that event. The officer sees a **stale credit limit** — one that does not reflect the committed disbursement.

### What the System Does — A Layered Approach

#### Layer 1: Read-Model Metadata (Version Watermarking)

Every projection row carries metadata indicating its freshness:

```python
class LoanApplicationReadModel:
    application_id: str
    available_credit_limit_usd: Decimal
    last_projected_version: int       # stream version this row reflects
    last_projected_at: datetime       # timestamp of last projection update
    projection_lag_ms: int            # computed: now() - last_projected_at
```

When the UI queries the read model, the response always includes `last_projected_version` and `projection_lag_ms`. The UI can compare this against the version it knows was just committed.

#### Layer 2: Causal Consistency via Version Tokens

When the agent appends the disbursement event, the write returns the new stream version (e.g., `version=12`). The UI passes this version as a **read-after-write token** on the subsequent query:

```
GET /api/loans/loan-123/credit-limit
  X-After-Version: 12
```

The query handler checks: has the projection processed version 12 yet?

- **If yes:** return the projected value directly.
- **If no:** either (a) wait briefly (up to a timeout, e.g. 500ms) for the projection to catch up, or (b) **read the event stream directly** up to version 12 and compute the value on the fly (a "live projection fold"). This guarantees causal consistency for the requesting client without blocking other users.

```python
async def get_credit_limit(application_id: str, after_version: int | None = None):
    read_model = await projection_store.get(application_id)

    if after_version and read_model.last_projected_version < after_version:
        # Option A: Brief wait for projection to catch up
        read_model = await projection_store.wait_for_version(
            application_id, after_version, timeout_ms=500
        )

        if read_model.last_projected_version < after_version:
            # Option B: Compute live from the stream
            events = await event_store.read_stream(
                f"loan-{application_id}", to_version=after_version
            )
            return compute_credit_limit_from_events(events), {
                "source": "live_fold",
                "as_of_version": after_version,
            }

    return read_model.available_credit_limit_usd, {
        "source": "projection",
        "as_of_version": read_model.last_projected_version,
    }
```

#### Layer 3: Communicating Staleness to the UI

The API response always includes consistency metadata:

```json
{
  "available_credit_limit_usd": 450000.00,
  "data_consistency": {
    "source": "projection",
    "as_of_version": 11,
    "requested_after_version": 12,
    "is_stale": true,
    "staleness_ms": 180,
    "message": "This value may not reflect the most recent transaction. Refreshing..."
  }
}
```

The UI renders this as:

- A **subtle staleness indicator** (e.g., a pulsing dot or "Updating…" badge near the credit limit).
- **Auto-refresh** — the UI polls or subscribes (via WebSocket/SSE) until `is_stale` becomes `false`, then updates the displayed value without user action.
- **No silent stale reads** — the officer is never shown a number without context. If the projection is lagging, that fact is visible.

### Projection Handler Idempotency

**All projection handlers must be idempotent.** This is not optional — it is a correctness requirement.

In the Apex system, projection handlers may process the same event more than once due to:
- **Crash recovery:** A projection node crashes after applying an event to the read model but before advancing its checkpoint. On restart, it replays from the last committed checkpoint — re-processing the already-applied event.
- **Distributed execution:** In the multi-node deployment (see Section 6), lease transfers between nodes can cause brief overlaps where both the old and new owner process the same event batch.
- **Manual re-projection:** When a projection's logic changes (e.g., adding a new field to the `LoanApplicationReadModel`), the projection is rebuilt by replaying all events from position 0. Every event is re-processed.

Concretely, the `LoanApplicationProjection` handler for `ApplicationApproved` must be safe to call twice for the same event:

```python
class LoanApplicationProjection:
    async def apply(self, conn, event):
        if event["event_type"] == "ApplicationApproved":
            # IDEMPOTENT: Upsert keyed on (application_id, stream_version).
            # Second call with same key overwrites with identical data — no side effects.
            await conn.execute(
                """
                INSERT INTO loan_application_read_model
                    (application_id, status, approved_amount_usd, updated_at_version)
                VALUES ($1, 'APPROVED', $2, $3)
                ON CONFLICT (application_id)
                DO UPDATE SET
                    status = 'APPROVED',
                    approved_amount_usd = $2,
                    updated_at_version = $3
                WHERE loan_application_read_model.updated_at_version < $3
                """,
                event["data"]["application_id"],
                event["data"]["approved_amount_usd"],
                event["global_position"],
            )
```

The `WHERE updated_at_version < $3` clause ensures that replaying an already-projected event is a no-op — the row is not updated if the position has already been applied. This combines idempotency with out-of-order safety.

This approach avoids the two failure modes of eventual consistency: (1) the user acts on stale data without knowing it, and (2) the system blocks reads waiting for consistency, negating the scalability benefit of CQRS.

---

## 5. The Upcasting Scenario

> **Direct answer:** The upcaster transforms v1 events to v2 at read-time — stored bytes are never mutated. For inference: infer a value only when documented evidence exists (e.g., `model_version` from deployment records); use `null` when no evidence exists (e.g., `confidence_score`). **Do not fabricate historical data.** The event store remains the immutable source of truth; upcasting is a read-path transformation, not a write-path mutation.

### The Original Event (v1, 2024)

```python
class CreditDecisionMade_v1(DomainEvent):
    event_type = "CreditDecisionMade"
    schema_version = 1
    application_id: str
    decision: str          # "APPROVE" | "DECLINE" | "REFER"
    reason: str
```

### The New Event (v2, 2026)

```python
class CreditDecisionMade_v2(DomainEvent):
    event_type = "CreditDecisionMade"
    schema_version = 2
    application_id: str
    decision: str
    reason: str
    model_version: str             # e.g., "credit-risk-v3.2.1"
    confidence_score: float        # 0.0–1.0
    regulatory_basis: str          # e.g., "OCC-2025-Bulletin-7"
```

### The Upcaster

```python
from typing import Any

class CreditDecisionMadeUpcaster:
    """
    Transforms CreditDecisionMade v1 events to v2 schema at read-time.
    The stored bytes are never mutated — upcasting happens in the
    deserialization pipeline.
    """

    source_version: int = 1
    target_version: int = 2

    # -- Inference constants for historical events --
    # Before 2025-06-01, Apex used a single undocumented model internally
    # referred to as "legacy-credit-v1". No per-decision model tracking
    # existed, but deployment records confirm this was the only model
    # in production from 2023-01 through 2025-05.
    LEGACY_MODEL_VERSION = "legacy-credit-v1.0 (inferred)"

    # Prior to OCC-2025-Bulletin-7, credit decisions fell under the
    # general OCC-2020 lending guidelines.
    LEGACY_REGULATORY_BASIS = "OCC-2020-General-Lending-Guidelines (inferred)"

    def can_upcast(self, event_type: str, schema_version: int) -> bool:
        return (
            event_type == "CreditDecisionMade"
            and schema_version == self.source_version
        )

    def upcast(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Transform a v1 payload to v2 at read-time.
        The original stored event is never modified.
        """
        return {
            **data,
            "schema_version": self.target_version,
            "model_version": self.LEGACY_MODEL_VERSION,
            "confidence_score": None,   # null — see inference rules below
            "regulatory_basis": self.LEGACY_REGULATORY_BASIS,
        }
```

Upcaster registration in the deserialization pipeline:

```python
class EventDeserializer:
    def __init__(self):
        self._upcasters: list = [
            CreditDecisionMadeUpcaster(),
            # Future upcasters are registered here.
            # They compose: v1 → v2 → v3 if needed.
        ]

    def deserialize(self, raw: dict[str, Any]) -> DomainEvent:
        event_type = raw["event_type"]
        schema_version = raw.get("schema_version", 1)
        data = raw["data"]

        # Apply upcasters in chain: v1 → v2 → ... → current
        for upcaster in self._upcasters:
            if upcaster.can_upcast(event_type, schema_version):
                data = upcaster.upcast(data)
                schema_version = upcaster.target_version

        return EVENT_REGISTRY[event_type].from_dict(data)
```

### Inference Strategy for Historical Events

The inference strategy for events predating `model_version` follows a strict rule: **infer a value only when there is documented evidence that the value is correct for the historical period. When no such evidence exists, use `null`. Do not fabricate historical data.**

This rule produces three distinct treatments for the three new fields:

1. **`model_version = "legacy-credit-v1.0 (inferred)"`** — **Inferred, because evidence exists.** Deployment records confirm that `legacy-credit-v1.0` was the only credit model in production from 2023-01 through 2025-05. The `(inferred)` suffix is a permanent, machine-readable marker so any downstream consumer (projection, report, regulator query) can immediately distinguish inferred metadata from recorded metadata. This is not fabrication — it is documented fact with an explicit provenance trail.

2. **`confidence_score = None`** — **Null, because no evidence exists.** Pre-2025 decisions were made without a confidence calibration framework. There is no deployment record, no retroactive calculation, no reasonable default that would accurately represent the historical confidence. Inferring a value (e.g., `0.5`, `-1.0`, or any sentinel) would create fictional data in a regulated audit trail. **We do not fabricate historical data.** Downstream consumers must handle `Optional[float]` — this is the honest cost of schema evolution. Projections and analytics exclude `null` confidence scores from aggregation (e.g., "average confidence of approved loans" counts only v2+ events).

3. **`regulatory_basis = "OCC-2020-General-Lending-Guidelines (inferred)"`** — **Inferred, because evidence exists.** Prior to OCC-2025-Bulletin-7, all credit decisions at Apex fell under the OCC-2020 General Lending Guidelines. This is a matter of regulatory public record, not speculation. The `(inferred)` suffix marks the provenance.

**The decision rule, stated precisely:**

| Condition | Action | Example |
|---|---|---|
| A single, verifiable value can be established for the historical period from deployment records, regulatory filings, or operational logs | **Infer** with `(inferred)` suffix | `model_version`, `regulatory_basis` |
| Multiple possible values existed, or no records survive, or the concept did not exist at the time | **Use `null`** | `confidence_score` |
| A reasonable default exists but cannot be verified against historical records | **Use `null`** — reasonable is not the same as correct | Any field where "it was probably X" is the best you can say |

4. **No stored mutation.** The v1 events remain byte-for-byte identical in the event store forever. Upcasting happens *only* at read-time, in the deserialization pipeline. If the inference strategy changes (e.g., better deployment records surface), we update the upcaster code — not the stored events.

5. **Audit trail for the upcaster itself.** The upcaster's inference constants are documented with the reasoning (deployment records, regulatory timeline). In a regulatory examination, the examiner can see exactly *why* a historical event now carries `model_version = "legacy-credit-v1.0 (inferred)"` — because it is a code artifact with comments, not a mystery value that appeared in the database.

---

## 6. The Marten Async Daemon Parallel

> **Direct answer:** The coordination primitive is **PostgreSQL advisory locks combined with lease-based row ownership**. This mechanism guards against **split-brain projection processing**, where multiple nodes process the same events concurrently and corrupt read models. Projection handlers must be idempotent as a last line of defense during lease transitions.

### What Marten's Async Daemon Does

Marten 7.0's Async Daemon distributes projection execution across multiple nodes. Each node in a cluster subscribes to event streams and projects events into read models. The Daemon provides:

- **Work distribution:** Different projection types (or shard ranges) are assigned to different nodes.
- **Leadership election:** One node is the "leader" that coordinates assignment; if it dies, another takes over.
- **Effectively-once projection processing:** Single-owner processing with idempotent handlers that achieve effectively-once read-model results, even if an event is delivered more than once.
- **Progress tracking:** Each node records its high-water mark (the last event position it successfully projected), enabling resumption after failure.

### How to Achieve This in Python

#### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  PostgreSQL                              │
│                                                          │
│  events table          │  projection_checkpoints table   │
│  ─────────────         │  ──────────────────────────     │
│  global_position (seq) │  projection_name                │
│  stream_id             │  shard_key                      │
│  event_type            │  last_processed_position        │
│  data                  │  owner_node_id                  │
│  ...                   │  lease_expires_at               │
│                        │  updated_at                     │
└────────────┬───────────┴──────────────┬──────────────────┘
             │                          │
     ┌───────┴───────┐         ┌───────┴───────┐
     │   Worker A    │         │   Worker B    │
     │  (node-1)     │         │  (node-2)     │
     │               │         │               │
     │  Projections: │         │  Projections: │
     │  - LoanApp    │         │  - Compliance │
     │  - AgentSess  │         │  - AuditLedg  │
     └───────────────┘         └───────────────┘
```

#### Coordination Primitive: PostgreSQL Advisory Locks + Lease-Based Ownership

The coordination primitive is **PostgreSQL advisory locks combined with row-level lease timestamps** in a `projection_checkpoints` table. This is chosen deliberately over external coordination systems (Redis, ZooKeeper, etcd) because the event store already depends on PostgreSQL — adding no new infrastructure dependency.

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta

LEASE_DURATION = timedelta(seconds=30)
POLL_INTERVAL = timedelta(seconds=1)

class DistributedProjectionDaemon:
    """
    A Python equivalent of Marten's Async Daemon.
    Each node runs one instance. Nodes coordinate via
    PostgreSQL advisory locks and lease-based shard ownership.
    """

    def __init__(self, node_id: str, pool: asyncpg.Pool, projections: dict):
        self.node_id = node_id
        self.pool = pool
        self.projections = projections  # name → ProjectionHandler

    async def run(self):
        """Main loop: acquire shards, project events, renew leases."""
        while True:
            for name, handler in self.projections.items():
                acquired = await self._try_acquire_shard(name)
                if acquired:
                    await self._project_shard(name, handler)
            await asyncio.sleep(POLL_INTERVAL.total_seconds())

    async def _try_acquire_shard(self, projection_name: str) -> bool:
        """
        Attempt to acquire a lease on a projection shard.
        Uses pg_try_advisory_xact_lock to prevent two nodes from
        acquiring the same shard simultaneously.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                lock_key = hash(projection_name) & 0x7FFFFFFF
                got_lock = await conn.fetchval(
                    "SELECT pg_try_advisory_xact_lock($1)", lock_key
                )
                if not got_lock:
                    return False

                row = await conn.fetchrow(
                    """
                    SELECT owner_node_id, lease_expires_at
                    FROM projection_checkpoints
                    WHERE projection_name = $1
                    """,
                    projection_name,
                )

                now = datetime.utcnow()
                if row is None:
                    # First time: create checkpoint
                    await conn.execute(
                        """
                        INSERT INTO projection_checkpoints
                        (projection_name, shard_key, last_processed_position,
                         owner_node_id, lease_expires_at)
                        VALUES ($1, $2, 0, $3, $4)
                        """,
                        projection_name, "all", self.node_id,
                        now + LEASE_DURATION,
                    )
                    return True

                if (
                    row["owner_node_id"] == self.node_id
                    or row["lease_expires_at"] < now
                ):
                    # We own it, or the lease expired (previous owner died)
                    await conn.execute(
                        """
                        UPDATE projection_checkpoints
                        SET owner_node_id = $1, lease_expires_at = $2
                        WHERE projection_name = $3
                        """,
                        self.node_id, now + LEASE_DURATION,
                        projection_name,
                    )
                    return True

                return False  # Another node owns this shard and lease is valid

    async def _project_shard(self, projection_name: str, handler):
        """
        Read new events since the last checkpoint and project them.
        Checkpoint and lease renewal happen in the same transaction
        as the projection write — guaranteeing exactly-once processing.

        IMPORTANT: The handler.apply() method MUST be idempotent.
        Even with transactional checkpointing, crash recovery and
        lease transfers can cause re-delivery. See Section 4.
        """
        async with self.pool.acquire() as conn:
            checkpoint = await conn.fetchval(
                """
                SELECT last_processed_position
                FROM projection_checkpoints
                WHERE projection_name = $1 AND owner_node_id = $2
                """,
                projection_name, self.node_id,
            )
            if checkpoint is None:
                return

            events = await conn.fetch(
                """
                SELECT global_position, stream_id, event_type, data, metadata
                FROM events
                WHERE global_position > $1
                ORDER BY global_position
                LIMIT 100
                """,
                checkpoint,
            )

            if not events:
                return

            async with conn.transaction():
                # Project each event (handler MUST be idempotent)
                for event in events:
                    await handler.apply(conn, event)

                # Update checkpoint + renew lease atomically
                new_position = events[-1]["global_position"]
                await conn.execute(
                    """
                    UPDATE projection_checkpoints
                    SET last_processed_position = $1,
                        lease_expires_at = $2,
                        updated_at = NOW()
                    WHERE projection_name = $3 AND owner_node_id = $4
                    """,
                    new_position,
                    datetime.utcnow() + LEASE_DURATION,
                    projection_name, self.node_id,
                )
```

#### The Failure Mode It Guards Against: Split-Brain Projection Processing

The primary failure mode is **split-brain**: two nodes believe they own the same projection shard and both process the same events, causing duplicate or inconsistent read-model state. This happens when:

1. **Node A holds shard "LoanApplication"** and is projecting events.
2. **Node A pauses** (GC pause, network partition, high CPU) and its lease expires.
3. **Node B detects the expired lease**, acquires the shard, and begins projecting from Node A's last checkpoint.
4. **Node A resumes**, unaware its lease expired, and also continues projecting.

Now both nodes are projecting the same events → duplicate writes, corrupted projection state.

**How the coordination primitive prevents this:**

- **Advisory locks** prevent two nodes from even *reading* the checkpoint row simultaneously during the acquire phase. This eliminates the race at acquisition time.
- **Lease timestamps** provide distributed failure detection without heartbeat protocols. If a node crashes, its lease expires after 30 seconds, and any other node can take over.
- **Transactional checkpoint updates** ensure that the projection write and the checkpoint advance are atomic. If Node A's transaction is still open when its lease expires, Node B cannot acquire the shard (the advisory lock is held for the transaction duration). If Node A's transaction committed but the lease expired *between* commit and next acquire, Node A will see the lease is now owned by Node B on its next cycle and will back off.
- **Fencing via `owner_node_id` check:** Every projection write verifies `owner_node_id = self.node_id` in the `WHERE` clause. If the lease was stolen between reads, the `UPDATE` affects zero rows, and the node knows it has been fenced out.
- **Idempotent handlers as the last line of defense:** Even with all the above safeguards, brief overlaps can occur during lease transitions. Because projection handlers are idempotent (see Section 4), duplicate processing during a transition window produces the same result — not corruption.

This is the same fundamental pattern Marten uses (via its `IProjectionDaemon` and the `mt_event_progression` table), adapted to PostgreSQL's native primitives instead of .NET's `IDistributedLock`.

---

## 7. Tradeoffs — The Costs of Event Sourcing

Event sourcing is not free. Choosing it for Apex's lending platform is justified because the regulatory requirements *mandate* the properties only ES provides (immutable audit trail, point-in-time reconstruction, temporal queries, cryptographic integrity). But the costs are real:

### Increased Write Latency

Every write to the `LoanApplication` stream involves: JSON serialization of the event payload, SHA-256 hash computation (chaining to the previous event's hash), an `INSERT` into the events table, and an `UPDATE` to the streams table's `current_version` — all inside a serialized transaction. A simple `UPDATE loan_applications SET status = 'APPROVED'` in a CRUD system is faster. For Apex, this cost is acceptable: loan processing is measured in seconds (agent LLM calls dominate), and the per-event overhead of ~1–5ms is noise in that budget.

### Projection Lag Management

The read model is eventually consistent with the event log. As described in Section 4, this means every query path must account for staleness. The system carries the operational burden of: monitoring projection lag, implementing version-token-based causal reads, maintaining the `ProjectionEngine` as a separate running process, and rebuilding projections when their logic changes. In a CRUD system, a read-after-write returns the current value by default. In ES, causal consistency is infrastructure you build and operate.

### Operational Complexity

The event store introduces operational concerns that do not exist in CRUD:
- **Event store growth.** Events are never deleted. The `LoanApplication` stream for a single loan may accumulate 15–30 events over its lifecycle. At scale (100k loans/year), the events table grows to millions of rows. Snapshotting (periodically capturing aggregate state at a version) reduces replay cost but adds another subsystem to maintain.
- **Schema evolution.** Every event type is a contract. Changing a field requires an upcaster (Section 5), not a database migration. The upcaster chain grows monotonically — v1→v2→v3→... — and must be maintained and tested for the life of the system.
- **Debugging complexity.** "What is the current state of loan-456?" is no longer a single `SELECT`. It is "replay 23 events through the aggregate's `apply()` method." Tooling (event store browsers, projection inspectors) must be built or adopted.
- **Team learning curve.** Developers accustomed to CRUD must internalize new patterns: aggregates as consistency boundaries, commands vs. events, eventual consistency in read models, idempotent projections. This is a training cost measured in weeks, not hours.

### Why These Costs Are Justified for Apex

| Cost | Mitigation | Why It's Worth It |
|---|---|---|
| Write latency (~1–5ms overhead) | Negligible vs. agent LLM call latency (~1–10s) | Immutable audit trail with zero additional infrastructure |
| Projection lag | Version tokens + live fold (Section 4) | Temporal queries + point-in-time reconstruction |
| Event store growth | Snapshotting at version intervals | Complete, tamper-evident history for regulatory examination |
| Schema evolution via upcasters | Chained upcasters + inference rules (Section 5) | Historical events remain queryable under new schemas |
| Debugging complexity | Event store browser tooling | Any past state is reconstructable — debugging becomes deterministic |

In a system where "delete the audit trail" is a regulatory violation, and "reconstruct the state at 2:47 PM last Tuesday" is a compliance requirement, these costs are the price of correctness. **You cannot bolt these properties onto a CRUD system after the fact.** The CTO's mandate — "auditability must be the architecture" — is an acknowledgment that these tradeoffs must be made upfront, not retroactively.

---

## Appendix: Missing Events Identified in the Catalogue

The provided event catalogue has deliberate gaps. The following events are necessary for a complete domain model:

| Missing Event | Aggregate | Rationale |
|---|---|---|
| `FraudScreeningRequested` | `LoanApplication` | Symmetric with `CreditAnalysisRequested` — the orchestrator must request fraud screening before `FraudScreeningCompleted` can occur. Write-before-execute: the intent to screen must be committed before the agent begins work. |
| `ComplianceCheckCompleted` | `ComplianceRecord` | There's no terminal event for the compliance record. After all rules pass or fail, the aggregate needs a summary verdict event (`CLEARED` / `BLOCKED` / `CONDITIONAL`). |
| `ApplicationWithdrawn` | `LoanApplication` | Applicants can withdraw at any stage. Without this event the lifecycle has no cancellation path. |
| `AgentSessionStarted` | `AgentSession` | The `AgentContextLoaded` event implies a session is already active, but there is no explicit session initiation event to anchor timing and configuration. State reconstruction requires a clear starting point. |
| `AgentSessionCompleted` | `AgentSession` | No terminal event for agent sessions — needed to calculate session duration and mark the session as no longer accepting events. |
| `HumanReviewStarted` | `LoanApplication` | Before `HumanReviewCompleted`, the loan officer must claim the review. This prevents two officers from reviewing the same application concurrently. Write-before-execute: claiming the review is committed before the officer begins reading. |
| `DisbursementInitiated` / `DisbursementCompleted` | `LoanApplication` | After approval, the actual disbursement of funds is a critical lifecycle event missing from the catalogue. |
| `AuditEventAppended` | `AuditLedger` | Individual cross-stream correlation entries — the append-only record of each linked event. `AuditIntegrityCheckRun` validates the chain but there is no event representing each chain link. |
