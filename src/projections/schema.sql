-- ============================================================================
-- Phase 3: Projection Tables (3 Projections + Infrastructure)
-- ============================================================================

-- Projection 1: ApplicationSummary — Current state of every application
CREATE TABLE IF NOT EXISTS application_summary (
    application_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    applicant_id TEXT,
    requested_amount_usd DECIMAL(18, 2),
    approved_amount_usd DECIMAL(18, 2),
    risk_tier TEXT,
    fraud_score FLOAT,
    compliance_status TEXT,
    decision TEXT,
    agent_sessions_completed TEXT[] DEFAULT '{}',
    last_event_type TEXT,
    last_event_at TIMESTAMPTZ,
    last_event_position BIGINT,
    human_reviewer_id TEXT,
    final_decision_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Projection 2: ComplianceAuditView — Per-rule regulatory audit trail
CREATE TABLE IF NOT EXISTS compliance_audit_view (
    application_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    result TEXT NOT NULL,
    is_hard_block BOOLEAN DEFAULT FALSE,
    recorded_at TIMESTAMPTZ NOT NULL,
    metadata JSONB,
    global_position BIGINT NOT NULL,
    PRIMARY KEY (application_id, rule_id)
);

-- Compliance Snapshots — For temporal query acceleration
CREATE TABLE IF NOT EXISTS compliance_snapshots (
    application_id TEXT NOT NULL,
    global_position BIGINT NOT NULL,
    snapshot_data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (application_id, global_position)
);

-- Projection 3: AgentPerformanceLedger — Aggregated metrics per model version
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id TEXT,
    model_version TEXT,
    analyses_completed BIGINT DEFAULT 0,
    decisions_generated BIGINT DEFAULT 0,
    avg_confidence_score FLOAT DEFAULT 0.0,
    avg_duration_ms FLOAT DEFAULT 0.0,
    approve_rate FLOAT DEFAULT 0.0,
    decline_rate FLOAT DEFAULT 0.0,
    refer_rate FLOAT DEFAULT 0.0,
    human_override_rate FLOAT DEFAULT 0.0,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (agent_id, model_version)
);

-- ============================================================================
-- Infrastructure Tables
-- ============================================================================

-- Projection Checkpoints: Progress tracking
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_position BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dead Letter Queue: Fault tolerance
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id SERIAL PRIMARY KEY,
    projection_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT,
    global_position BIGINT,
    error_message TEXT,
    payload JSONB,
    retry_count INT DEFAULT 0,
    failed_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Indexes
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_audit_app_id ON compliance_audit_view(application_id);
CREATE INDEX IF NOT EXISTS idx_audit_position ON compliance_audit_view(global_position);
CREATE INDEX IF NOT EXISTS idx_snap_app_pos ON compliance_snapshots(application_id, global_position);
CREATE INDEX IF NOT EXISTS idx_as_applicant_id ON application_summary(applicant_id);
CREATE INDEX IF NOT EXISTS idx_perf_agent ON agent_performance_ledger(agent_id);
