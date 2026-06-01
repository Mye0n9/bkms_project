from pathlib import Path
import yaml
from pipeline.state import AgentState

CATALOG_PATH = Path(__file__).parent.parent / "catalog" / "metrics.yaml"


def load_catalog() -> list[dict]:
    with open(CATALOG_PATH) as f:
        return yaml.safe_load(f)


def find_metric(catalog: list[dict], metric_id: str) -> dict | None:
    return next((m for m in catalog if m["id"] == metric_id), None)


def get_unresolved_params(metric: dict, resolved: dict) -> list[str]:
    return [
        name
        for name, defn in metric["params"].items()
        if defn.get("clarify_if_missing") and name not in resolved
    ]


def specify(state: AgentState) -> AgentState:
    catalog = load_catalog()
    metric = find_metric(catalog, state["metric_id"])
    if metric is None:
        raise ValueError(f"Unknown metric_id: {state['metric_id']}")

    unresolved = get_unresolved_params(metric, state.get("resolved_params", {}))
    if unresolved:
        raise ValueError(f"Unresolved params: {unresolved}")

    spec: dict = {
        "id": metric["id"],
        "display_name": metric["display_name"],
        "strategy": metric["strategy"],
        "resolved_params": state.get("resolved_params", {}),
    }
    if metric["strategy"] == "pg_function":
        spec["function"] = metric["function"]
    else:
        spec["table"] = metric["table"]

    return {**state, "metric_spec": spec, "unresolved_params": []}
