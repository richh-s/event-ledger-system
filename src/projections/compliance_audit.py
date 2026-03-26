from __future__ import annotations
import json
import logging
from src.projections.base import BaseProjection
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

# Snapshot every N compliance events per application
_SNAPSHOT_INTERVAL = 10


class ComplianceAuditViewProjection(BaseProjection):
    """
    Projection 2: ComplianceAuditView — regulatory audit trail.
    
    Tracks every compliance rule evaluation per application with
    regulation versioning. Supports temporal queries via snapshots:
    get_compliance_at(application_id, timestamp) returns the compliance
    state as it existed at a past moment.
    
    Consumed events:
      - ComplianceCheckCompleted
      - ComplianceRulePassed
      - ComplianceRuleFailed
      - ComplianceRuleNoted
    """

    @property
    def projection_name(self) -> str:
        return "compliance_audit_view"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        p = event.payload
        app_id = p.get("application_id")
        if not app_id:
            return

        event_type = event.event_type

        # ── ComplianceRulePassed ──
        if event_type == "ComplianceRulePassed":
            await self._upsert_rule(
                conn, app_id,
                rule_id=p["rule_id"],
                rule_version=p.get("rule_version", "unknown"),
                result="PASS",
                is_hard_block=False,
                recorded_at=event.recorded_at,
                global_position=event.global_position,
                metadata=self._build_meta(event)
            )

        # ── ComplianceRuleFailed ──
        elif event_type == "ComplianceRuleFailed":
            await self._upsert_rule(
                conn, app_id,
                rule_id=p["rule_id"],
                rule_version=p.get("rule_version", "unknown"),
                result="FAIL",
                is_hard_block=p.get("is_hard_block", False),
                recorded_at=event.recorded_at,
                global_position=event.global_position,
                metadata=self._build_meta(event)
            )

        # ── ComplianceRuleNoted ──
        elif event_type == "ComplianceRuleNoted":
            await self._upsert_rule(
                conn, app_id,
                rule_id=p["rule_id"],
                rule_version="noted",
                result="NOTED",
                is_hard_block=False,
                recorded_at=event.recorded_at,
                global_position=event.global_position,
                metadata=self._build_meta(event)
            )

        # ── ComplianceCheckCompleted (summary row) ──
        elif event_type == "ComplianceCheckCompleted":
            overall = p.get("overall_verdict", "UNKNOWN")
            reg_v = p.get("regulation_version") or p.get("regulation_set_version", "v1.0")
            await self._upsert_rule(
                conn, app_id,
                rule_id="__overall__",
                rule_version=reg_v,
                result=overall,
                is_hard_block=p.get("has_hard_block", False),
                recorded_at=event.recorded_at,
                global_position=event.global_position,
                metadata=self._build_meta(event)
            )

            # ── Snapshot strategy: save every N events ──
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM compliance_audit_view WHERE application_id = $1",
                app_id
            )
            if count and count % _SNAPSHOT_INTERVAL == 0:
                await self._save_snapshot(conn, app_id, event.global_position)
        else:
            return  # Skip unrelated events

    # ─── Internal helpers ─────────────────────────────────────────────────────

    async def _upsert_rule(
        self, conn, app_id: str, rule_id: str, rule_version: str,
        result: str, is_hard_block: bool, recorded_at, global_position, metadata: dict
    ) -> None:
        """Idempotent UPSERT with position guard — later position always wins."""
        await conn.execute(
            """
            INSERT INTO compliance_audit_view (
                application_id, rule_id, rule_version, result,
                is_hard_block, recorded_at, metadata, global_position
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            ON CONFLICT (application_id, rule_id) DO UPDATE SET
                rule_version = EXCLUDED.rule_version,
                result = EXCLUDED.result,
                is_hard_block = EXCLUDED.is_hard_block,
                recorded_at = EXCLUDED.recorded_at,
                metadata = EXCLUDED.metadata,
                global_position = EXCLUDED.global_position
            WHERE EXCLUDED.global_position > compliance_audit_view.global_position
            """,
            app_id, rule_id, rule_version, result,
            is_hard_block, recorded_at, json.dumps(metadata), global_position
        )

    async def _save_snapshot(self, conn, app_id: str, at_position: int) -> None:
        """Snapshot current compliance state for fast temporal lookups."""
        rows = await conn.fetch(
            "SELECT rule_id, rule_version, result, is_hard_block, recorded_at FROM compliance_audit_view WHERE application_id = $1",
            app_id
        )
        snapshot_data = [dict(r) for r in rows]
        await conn.execute(
            """
            INSERT INTO compliance_snapshots (application_id, global_position, snapshot_data)
            VALUES ($1, $2, $3::jsonb)
            ON CONFLICT (application_id, global_position) DO NOTHING
            """,
            app_id, at_position, json.dumps(snapshot_data)
        )

    def _build_meta(self, event: StoredEvent) -> dict:
        return {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "recorded_at": str(event.recorded_at),
        }

    async def rebuild_from_scratch(self, conn) -> None:
        """Truncate projection + snapshots, reset checkpoint to 0."""
        logger.info(f"Rebuilding {self.projection_name} from scratch...")
        await conn.execute("TRUNCATE compliance_audit_view")
        await conn.execute("TRUNCATE compliance_snapshots")
        await conn.execute(
            "UPDATE projection_checkpoints SET last_position = 0 WHERE projection_name = $1",
            self.projection_name
        )

    # ─── Temporal Query (called by MCP resource) ──────────────────────────────

    @staticmethod
    async def get_compliance_at(conn, application_id: str, as_of_position: int) -> list[dict]:
        """
        Snapshot-based temporal query.
        1. Find latest snapshot <= as_of_position
        2. Replay events from snapshot_position+1 → as_of_position
        3. Merge and return
        """
        # 1. Load snapshot
        snap_row = await conn.fetchrow(
            "SELECT * FROM compliance_snapshots WHERE application_id = $1 AND global_position <= $2 ORDER BY global_position DESC LIMIT 1",
            application_id, as_of_position
        )

        if snap_row:
            base_data = json.loads(snap_row["snapshot_data"])
            replay_from = snap_row["global_position"] + 1
        else:
            base_data = []
            replay_from = 0

        # 2. Replay events after snapshot up to as_of
        rows = await conn.fetch(
            """
            SELECT * FROM compliance_audit_view 
            WHERE application_id = $1 AND global_position >= $2 AND global_position <= $3
            ORDER BY global_position ASC
            """,
            application_id, replay_from, as_of_position
        )
        # Merge: snapshot base + later rows (later rows overwrite)
        rules = {r["rule_id"]: r for r in base_data}
        for r in rows:
            rules[r["rule_id"]] = dict(r)

        return list(rules.values())
