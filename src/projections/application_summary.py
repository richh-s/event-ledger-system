from __future__ import annotations
import logging
from src.projections.base import BaseProjection
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

class ApplicationSummaryProjection(BaseProjection):
    """Projection 1: Current state of all loan applications."""
    @property
    def projection_name(self) -> str:
        return "application_summary"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        p = event.payload
        app_id = p.get('application_id')
        if not app_id: return
            
        # Event to state mapping
        state_map = {
            "ApplicationSubmitted": "SUBMITTED",
            "DocumentUploaded": "DOCUMENTS_UPLOADED",
            "CreditAnalysisCompleted": "CREDIT_COMPLETE",
            "FraudScreeningCompleted": "FRAUD_COMPLETE",
            "ComplianceCheckCompleted": "COMPLIANCE_COMPLETE",
            "DecisionGenerated": "DECISION_PENDING",
            "ApplicationApproved": "APPROVED",
            "ApplicationDeclined": "DECLINED"
        }
        
        event_type = event.event_type
        current_state = p.get('state') or state_map.get(event_type)
        
        # Check if we need to skip meta-events for unknown applications
        exists = await conn.fetchval("SELECT 1 FROM application_summary WHERE application_id = $1", app_id)
        
        if not exists and not current_state:
            return

        final_decision_at = event.recorded_at if event_type in ["ApplicationApproved", "ApplicationDeclined"] else None
        
        await conn.execute(
            """
            INSERT INTO application_summary (
                application_id, state, applicant_id, requested_amount_usd, 
                approved_amount_usd, last_event_type, last_event_at, last_event_position, 
                final_decision_at, updated_at
            ) 
            VALUES (
                $1, $2, $3, $4, 
                $5, $6, $7, $8, 
                $9, NOW()
            )
            ON CONFLICT (application_id) DO UPDATE SET
                state = COALESCE(EXCLUDED.state, application_summary.state),
                applicant_id = COALESCE(EXCLUDED.applicant_id, application_summary.applicant_id),
                requested_amount_usd = COALESCE(EXCLUDED.requested_amount_usd, application_summary.requested_amount_usd),
                last_event_type = EXCLUDED.last_event_type,
                last_event_at = EXCLUDED.last_event_at,
                last_event_position = EXCLUDED.last_event_position,
                final_decision_at = COALESCE(EXCLUDED.final_decision_at, application_summary.final_decision_at),
                updated_at = EXCLUDED.updated_at
            """,
            app_id, 
            current_state or "INITIALIZING",
            p.get('applicant_id'), 
            p.get('requested_amount_usd'),
            p.get('approved_amount_usd'),
            event_type,
            event.recorded_at,
            event.global_position,
            final_decision_at
        )

        sess_id = p.get('session_id') or p.get('orchestrator_session_id')
        if sess_id:
            await conn.execute(
                """
                UPDATE application_summary 
                SET agent_sessions_completed = ARRAY_APPEND(agent_sessions_completed, $2)
                WHERE application_id = $1 AND NOT ($2 = ANY(agent_sessions_completed))
                """,
                app_id, sess_id
            )

    async def rebuild_from_scratch(self, conn) -> None:
        """Truncate + reset checkpoint to 0."""
        logger.info(f"Rebuilding {self.projection_name} from scratch...")
        await conn.execute("TRUNCATE application_summary")
        await conn.execute(
            "UPDATE projection_checkpoints SET last_position = 0 WHERE projection_name = $1",
            self.projection_name
        )
