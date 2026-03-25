from __future__ import annotations
import logging
from src.projections.base import BaseProjection
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


class AgentPerformanceLedgerProjection(BaseProjection):
    """
    Projection 3: AgentPerformanceLedger — aggregated per agent + model_version.
    
    Tracks: analyses_completed, avg_confidence, avg_duration_ms,
    approve_rate, decline_rate, refer_rate, human_override_rate.
    
    Updated incrementally from events:
      - AgentSessionStarted  → initialize/touch record
      - AgentNodeExecuted    → increment analyses, running avg of duration
      - AgentSessionCompleted → update confidence from final output
      - CreditAnalysisCompleted / FraudScreeningCompleted / ComplianceCheckCompleted
                              → track verdicts for rate calculation
      - DecisionGenerated     → approve/decline/refer classification
      - HumanReviewCompleted  → human override tracking
    """

    @property
    def projection_name(self) -> str:
        return "agent_performance_ledger"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        p = event.payload
        event_type = event.event_type

        # Extract agent identity — varies by event type
        agent_id = p.get("agent_id") or p.get("agent_name")
        model_v = p.get("model_version")

        # For node-level events, try session-based lookup
        if not agent_id and event_type in ("AgentNodeExecuted", "AgentSessionCompleted"):
            agent_id = p.get("agent_type")
        if not model_v and event_type in ("AgentNodeExecuted", "AgentSessionCompleted"):
            model_v = p.get("model_version") or "unknown"

        if not agent_id or not model_v:
            return

        # ── Initialize or touch row ──
        await conn.execute(
            """
            INSERT INTO agent_performance_ledger (agent_id, model_version, first_seen_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                last_seen_at = EXCLUDED.first_seen_at
            """,
            agent_id, model_v, event.recorded_at
        )

        # ── AgentNodeExecuted → increment analyses + running avg duration ──
        if event_type == "AgentNodeExecuted":
            duration = p.get("duration_ms", 0)
            confidence = p.get("confidence_score", 0)
            await conn.execute(
                """
                UPDATE agent_performance_ledger SET
                    analyses_completed = analyses_completed + 1,
                    avg_duration_ms = CASE 
                        WHEN analyses_completed = 0 THEN $3::FLOAT
                        ELSE (avg_duration_ms * analyses_completed + $3::FLOAT) / (analyses_completed + 1)
                    END,
                    avg_confidence_score = CASE
                        WHEN $4::FLOAT = 0 THEN avg_confidence_score
                        WHEN analyses_completed = 0 THEN $4::FLOAT
                        ELSE (avg_confidence_score * analyses_completed + $4::FLOAT) / (analyses_completed + 1)
                    END
                WHERE agent_id = $1 AND model_version = $2
                """,
                agent_id, model_v, float(duration), float(confidence)
            )

        # ── DecisionGenerated → approve/decline/refer classification ──
        elif event_type == "DecisionGenerated":
            verdict = p.get("verdict", "").upper()
            is_approve = 1 if verdict == "APPROVE" else 0
            is_decline = 1 if verdict == "DECLINE" else 0
            is_refer = 1 if verdict in ("REFER", "MANUAL_REVIEW") else 0
            await conn.execute(
                """
                UPDATE agent_performance_ledger SET
                    decisions_generated = decisions_generated + 1,
                    approve_rate = CASE
                        WHEN decisions_generated = 0 THEN $3::FLOAT
                        ELSE (approve_rate * decisions_generated + $3::FLOAT) / (decisions_generated + 1)
                    END,
                    decline_rate = CASE
                        WHEN decisions_generated = 0 THEN $4::FLOAT
                        ELSE (decline_rate * decisions_generated + $4::FLOAT) / (decisions_generated + 1)
                    END,
                    refer_rate = CASE
                        WHEN decisions_generated = 0 THEN $5::FLOAT
                        ELSE (refer_rate * decisions_generated + $5::FLOAT) / (decisions_generated + 1)
                    END
                WHERE agent_id = $1 AND model_version = $2
                """,
                agent_id, model_v, float(is_approve), float(is_decline), float(is_refer)
            )

        # ── HumanReviewCompleted → override tracking ──
        elif event_type == "HumanReviewCompleted":
            is_override = 1 if p.get("is_override") or p.get("overridden_decision") else 0
            if is_override:
                await conn.execute(
                    """
                    UPDATE agent_performance_ledger SET
                        human_override_rate = CASE
                            WHEN decisions_generated = 0 THEN 0
                            ELSE (human_override_rate * decisions_generated + 1.0) / decisions_generated
                        END
                    WHERE agent_id = $1 AND model_version = $2
                    """,
                    agent_id, model_v
                )

    async def rebuild_from_scratch(self, conn) -> None:
        """Truncate + reset checkpoint."""
        logger.info(f"Rebuilding {self.projection_name} from scratch...")
        await conn.execute("TRUNCATE agent_performance_ledger")
        await conn.execute(
            "UPDATE projection_checkpoints SET last_position = 0 WHERE projection_name = $1",
            self.projection_name
        )
