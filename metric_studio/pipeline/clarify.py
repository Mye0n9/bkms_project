from pipeline.state import AgentState


def clarify(state: AgentState, catalog: list[dict]) -> AgentState:
    if not state.get("unresolved_params"):
        return state

    metric = next((m for m in catalog if m["id"] == state["metric_id"]), None)
    if metric is None:
        return state

    resolved = dict(state.get("resolved_params", {}))
    remaining = list(state.get("unresolved_params", []))

    while remaining:
        param_name = remaining[0]
        param_def = metric["params"][param_name]
        print(f"[CLARIFY] {param_def['question']}")
        user_input = input("> ").strip()

        param_type = param_def.get("type", "string")
        try:
            if param_type == "integer":
                resolved[param_name] = int(user_input)
            elif param_type == "numeric":
                resolved[param_name] = float(user_input)
            elif param_type == "enum":
                valid = param_def.get("values", [])
                if user_input not in valid:
                    print(f"Please choose from: {', '.join(valid)}")
                    continue
                resolved[param_name] = user_input
            else:
                resolved[param_name] = user_input
        except (ValueError, TypeError):
            print(f"Invalid input. Expected {param_type}.")
            continue

        remaining.pop(0)

    return {**state, "resolved_params": resolved, "unresolved_params": []}
