# Event Ledger System: Apex Financial Services

The Ledger is an event-sourced, append-only system of record for the Apex high-frequency lending platform. It enforces strict domain invariants and ensures an immutable audit trail for all loan and agent operations.

## 🚀 Features
- **Event-Sourced Architecture**: 7 core aggregates with full replay functionality.
- **Gas Town Memory**: AI agent session tracking with node-level crash recovery.
- **Optimistic Concurrency**: Database-level write contention resolution.
- **Snapshot Integration**: High-performance aggregate reconstruction using snapshots.
- **Transactional Outbox**: Guaranteed event delivery for downstream projections.

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
   Duplicate `.env.example` as `.env` and configure your PostgreSQL credentials.

## 🗄 Migrations

Apply the PostgreSQL schema to initialize the Ledger tables:

```bash
uv run python manual_test.py --migrate
```
*(Alternatively, run `psql -f src/schema.sql` against your target database).*

## 🧪 Running Tests

The test suite covers domain logic, concurrency, and snapshotting.

```bash
uv run pytest
```

Specific test files:
- `tests/test_aggregates.py`: Domain rules and state machines.
- `tests/test_concurrency.py`: Double-decision race condition test.
- `tests/test_gas_town.py`: Agent session tracking.
- `tests/test_snapshots.py`: Snapshot creation and restoration.

## 🛡 Business Rules Enforced
1. **Rule #1**: Rigid state machine for `LoanApplication`.
2. **Rule #2**: `AgentSessionStarted` must be the first event for any AI session.
3. **Rule #3**: No duplicate credit analysis per application.
4. **Rule #4**: Fraud score range validation (0.0–1.0).
5. **Rule #5**: Compliance hard blocks prevent further evaluations.
6. **Rule #6**: Application approval depends on compliance and credit status.
7. **Rule #7**: Quality flags from extraction propagate to decision nodes.
