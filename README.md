# Event Ledger System: Apex Financial Services

The Ledger is an event-sourced, append-only system of record for the Apex high-frequency lending platform. It enforces strict domain invariants and ensures an immutable audit trail for all loan and agent operations.

## 🚀 Features
- **Event-Sourced Architecture**: 7 core aggregates with full replay functionality.
- **Gas Town Memory**: AI agent session tracking with node-level crash recovery.
- **Optimistic Concurrency**: Database-level write contention resolution.
- **Snapshot Integration**: High-performance aggregate reconstruction using snapshots.
- **Transactional Outbox**: Guaranteed event delivery for downstream projections.

## 🤖 Phase 2 AI Agents (Score 5: Master Thinker)

The system includes a sophisticated AI agent chain implemented with **LangGraph**, adhering to the highest architectural standards for resilience and auditability.

### Core Agents
- **Credit Analysis Agent**: Performs deep risk assessment using LLMs with automated financial data integration.
- **Fraud Detection Agent**: Screens for anomalies and suspicious historical patterns.
- **Compliance Agent**: Executes 6+ deterministic jurisdiction and AML rules.
- **Decision Orchestrator**: Synthesizes all agent outputs into a final approval or decline.

### Master Thinker Features
- **Strict Idempotency**: Agents audit existing domain streams to prevent duplicate writes on retry.
- **Write-Before-Execute (WBE)**: `AgentSessionStarted` and initiation events are persisted **before** any logic or tool calls.
- **Optimistic Concurrency (OCC)**: Built-in retry loops resolve database write contention automatically.
- **Policy Authority**: Deterministic Python rules override LLM recommendations (e.g., blocking hallucinations).
- **Session Recovery**: Failed agent sessions are automatically detected and resume from the last known good state.

## 🛠 Installation

The project uses `uv` for lightning-fast dependency management.

1. **Clone the repository**:
   ```bash
   git clone https://github.com/richh-s/event-ledger-system.git
   cd event-ledger-system
   ```

2. **Sync dependencies**:
   ```bash
   uv sync
   ```

3. **Set up Environment**:
   Duplicate `.env.example` as `.env` and configure your credentials.

## 🗄 Migrations & Seed Data

1. **Initialize Schema**:
   ```bash
   uv run python manual_test.py --migrate
   ```
2. **Ingest Seed Data**:
   ```bash
   uv run python ingest_seed_data.py
   ```

## 🧪 Running Tests

### Phase 1: Core Ledger
```bash
uv run pytest
```

### Phase 2: AI Agent Chain (Score 5 Audit)
To run the full 8-step agent chain with advanced proofs (OCC, WBE, Idempotency, etc.):
```bash
uv run python manual_test_phase2_ai.py
```

## 📊 Phase 3 CQRS Projections & Read-Side (Spec-Aligned)

The read-side of The Ledger implements a strict CQRS split, using an asynchronous projection daemon to maintain optimized views for high-performance consumption.

### Exactly 3 Projections
- **ApplicationSummary**: Real-time state of all loan applications (PII, status, metrics).
- **ComplianceAuditView**: Detailed rule-level regulatory trail with **Snapshot-based temporal queries** (`?as_of`).
- **AgentPerformanceLedger**: Incremental aggregation of agent/model accuracy and throughput.

### Async Projection Daemon
- **Independent Checkpoints**: Each projection tracks its own `global_position` without global locks.
- **Fault-Tolerant**: Isolated failures are moved to a **Dead Letter Queue (DLQ)** while the daemon continues.
- **Graceful Shutdown**: Flag-based lifecycle prevents transaction rollbacks during shutdown.

## 🔌 MCP Integration

The system exposes exactly **8 tools** (commands) and **6 resources** (queries) via the Model Context Protocol.

### 6 Read-Side Resources (Spec-Compliant)
- `ledger://applications/{id}`: Current application state.
- `ledger://applications/{id}/compliance`: Per-rule regulatory verdicts (supports `?as_of`).
- `ledger://applications/{id}/audit-trail`: Raw event history (direct read).
- `ledger://agents/{id}/performance`: Aggregated agent/model metrics.
- `ledger://agents/{id}/sessions/{session_id}`: Detailed trace of a specific session (direct read).
- `ledger://ledger/health`: Projection lag, DLQ status, and system health.

## 🛡 Business Rules Enforced
1. **Rule #1**: Rigid state machine for `LoanApplication`.
2. **Rule #2**: `AgentSessionStarted` must be the first event for any AI session.
3. **Rule #3**: No duplicate credit analysis per application (Strict Idempotency).
4. **Rule #4**: Fraud score range validation (0.0–1.0).
5. **Rule #5**: Compliance hard blocks prevent further evaluations.
6. **Rule #6**: Application approval depends on compliance and credit status.
7. **Rule #7**: Quality flags from extraction propagate to decision nodes.
8. **Rule #8**: System rules (Python) override AI hallucinations.
