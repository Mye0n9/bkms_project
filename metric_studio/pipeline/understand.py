from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, Field
from pipeline.state import AgentState
from pipeline.specify import load_catalog, find_metric, get_unresolved_params
from pipeline.llm import get_llm

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


class UnderstandOutput(BaseModel):
    intent: str = Field(description="One sentence: what the user wants to find")
    metric_id: str | None = Field(description="Matched metric id, or null")
    resolved_params: dict = Field(
        default_factory=dict,
        description="Parameter values already present in the query",
    )
    requires_freeform: bool = Field(
        default=False,
        description=(
            "True when the query is a legitimate financial screening request "
            "but doesn't map to a single catalog pattern (e.g. it combines "
            "multiple conditions or uses concepts not in the catalog)"
        ),
    )


def understand(state: AgentState, catalog: list[dict] | None = None) -> AgentState:
    if catalog is None:
        catalog = load_catalog()

    env = Environment(loader=FileSystemLoader(str(PROMPTS_DIR)))
    template = env.get_template("understand.jinja2")

    catalog_summary = [
        {"id": m["id"], "display_name": m["display_name"], "aliases": m["aliases"]}
        for m in catalog
    ]
    prompt_text = template.render(
        query=state["raw_query"],
        catalog=catalog_summary,
        conversation=state.get("conversation", []),
    )

    verbose = state.get("verbose", False)
    if verbose:
        print("\n" + "="*60)
        print("[VERBOSE] UNDERSTAND — prompt sent to LLM:")
        print("-"*60)
        print(prompt_text)
        print("="*60)

    llm = get_llm()
    output: UnderstandOutput = llm.with_structured_output(
        UnderstandOutput, method="function_calling"
    ).invoke(prompt_text)

    if verbose:
        print("\n[VERBOSE] UNDERSTAND — LLM structured output:")
        print(f"  intent:           {output.intent}")
        print(f"  metric_id:        {output.metric_id}")
        print(f"  resolved_params:  {output.resolved_params}")
        print(f"  requires_freeform:{output.requires_freeform}")
        print("="*60 + "\n")

    unresolved: list[str] = []
    if output.metric_id:
        metric = find_metric(catalog, output.metric_id)
        if metric:
            unresolved = get_unresolved_params(metric, output.resolved_params)

    return {
        **state,
        "intent": output.intent,
        "metric_id": output.metric_id,
        "resolved_params": output.resolved_params,
        "unresolved_params": unresolved,
        "requires_freeform": output.requires_freeform,
    }
