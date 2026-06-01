from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from langchain_anthropic import ChatAnthropic
from pydantic import BaseModel, Field
from pipeline.state import AgentState
from pipeline.specify import load_catalog, find_metric, get_unresolved_params
from config import settings

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


class UnderstandOutput(BaseModel):
    intent: str = Field(description="One sentence: what the user wants to find")
    metric_id: str | None = Field(description="Matched metric id, or null")
    resolved_params: dict = Field(
        default_factory=dict,
        description="Parameter values already present in the query",
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

    llm = ChatAnthropic(
        model=settings.llm_model,
        api_key=settings.anthropic_api_key,
    )
    output: UnderstandOutput = llm.with_structured_output(UnderstandOutput).invoke(prompt_text)

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
    }
