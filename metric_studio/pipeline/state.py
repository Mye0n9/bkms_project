from typing import Any, TypedDict


class AgentState(TypedDict, total=False):
    raw_query: str
    intent: str
    metric_id: str | None
    requires_freeform: bool
    resolved_params: dict[str, Any]
    unresolved_params: list[str]
    metric_spec: dict | None
    sql: str | None
    result: Any
    execution_error: str | None
    is_connection_error: bool
    conversation: list[dict]
    verbose: bool
