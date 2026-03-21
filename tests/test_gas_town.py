import pytest
from datetime import datetime
from src.aggregates.agent_session import AgentSessionAggregate, PreconditionFailedError
from src.models.events import (
    AgentType,
    AgentSessionStarted,
    AgentNodeExecuted,
)

def test_gas_town_work_without_start_fails():
    agg = AgentSessionAggregate("ag-s1")
    
    with pytest.raises(PreconditionFailedError, match="not started"):
        agg.record_node_execution(AgentNodeExecuted(
            session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, node_name="x",
            node_sequence=1, input_keys=[], output_keys=[], llm_called=False, duration_ms=10, executed_at=datetime.now()
        ))

def test_gas_town_work_after_start_succeeds():
    agg = AgentSessionAggregate("ag-s1")
    agg.start_session(AgentSessionStarted(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, agent_id="1", application_id="test",
        model_version="1", langgraph_graph_version="1", context_source="ctx", context_token_count=10,
        started_at=datetime.now()
    ))
    
    assert agg.is_started
    
    agg.record_node_execution(AgentNodeExecuted(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, node_name="x",
        node_sequence=1, input_keys=[], output_keys=[], llm_called=False, duration_ms=10, executed_at=datetime.now()
    ))
    
    assert agg.nodes_executed == 1

def test_gas_town_tool_called():
    agg = AgentSessionAggregate("ag-s1")
    from src.models.events import AgentToolCalled
    agg.start_session(AgentSessionStarted.model_construct(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, agent_id="1", application_id="test",
        model_version="1", langgraph_graph_version="1", context_source="ctx", context_token_count=10,
        started_at=datetime.now(), event_type="AgentSessionStarted", event_version=1
    ))
    agg.record_tool_call(AgentToolCalled.model_construct(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, tool_name="search", tool_args="{}",
        tool_input_summary="in", tool_output_summary="out", tool_duration_ms=10,
        called_at=datetime.now(), event_type="AgentToolCalled", event_version=1
    ))
    assert agg.tool_calls == 1

def test_gas_town_failure_stores_last_node():
    agg = AgentSessionAggregate("ag-s1")
    agg.start_session(AgentSessionStarted.model_construct(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, agent_id="1", application_id="test",
        model_version="1", langgraph_graph_version="1", context_source="ctx", context_token_count=10,
        started_at=datetime.now(), event_type="AgentSessionStarted", event_version=1
    ))
    agg.record_node_execution(AgentNodeExecuted.model_construct(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, node_name="node_val",
        node_sequence=1, input_keys=[], output_keys=[], llm_called=False, duration_ms=10, 
        executed_at=datetime.now(), event_type="AgentNodeExecuted", event_version=1
    ))
    from src.models.events import AgentSessionFailed
    agg.fail_session(AgentSessionFailed.model_construct(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, error_code="CRASH", error_message="boom",
        failed_at=datetime.now(), event_type="AgentSessionFailed", event_version=1
    ))
    assert agg.is_failed == True
    assert agg.get_last_successful_node() == "node_val"

def test_gas_town_recovery():
    agg = AgentSessionAggregate("ag-s1")
    agg.start_session(AgentSessionStarted.model_construct(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, agent_id="1", application_id="test",
        model_version="1", langgraph_graph_version="1", context_source="ctx", context_token_count=10, 
        started_at=datetime.now(), event_type="AgentSessionStarted", event_version=1
    ))
    from src.models.events import AgentSessionRecovered
    agg.recover_session(AgentSessionRecovered.model_construct(
        session_id="s1", agent_type=AgentType.CREDIT_ANALYSIS, 
        source_session_id="old_s", recovered_at=datetime.now(),
        event_type="AgentSessionRecovered", event_version=1
    ))
    assert agg.is_failed == False
