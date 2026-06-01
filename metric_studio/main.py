import sys
from pathlib import Path
import yaml
from rich.console import Console

from pipeline.state import AgentState
from pipeline.understand import understand
from pipeline.clarify import clarify
from pipeline.specify import specify
from pipeline.generate import generate
from pipeline.execute import execute
from pipeline.present import present

console = Console()
CATALOG_PATH = Path(__file__).parent / "catalog" / "metrics.yaml"


def load_catalog() -> list[dict]:
    with open(CATALOG_PATH) as f:
        return yaml.safe_load(f)


def run_query(raw_query: str, conversation: list, catalog: list) -> AgentState:
    state: AgentState = {
        "raw_query": raw_query,
        "intent": "",
        "metric_id": None,
        "resolved_params": {},
        "unresolved_params": [],
        "metric_spec": None,
        "sql": None,
        "result": None,
        "execution_error": None,
        "conversation": conversation,
    }

    console.print("[dim]Analyzing query...[/dim]")
    state = understand(state, catalog)

    if state["metric_id"] is None:
        console.print("[yellow]Could not match your query to a known metric pattern.[/yellow]")
        console.print("Available: " + ", ".join(m["display_name"] for m in catalog))
        return state

    state = clarify(state, catalog)

    try:
        state = specify(state)
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        return state

    console.print("[dim]Generating SQL...[/dim]")
    state = generate(state)

    console.print("[dim]Executing query...[/dim]")
    state = execute(state)

    if state["execution_error"]:
        console.print("[yellow]Query error — attempting self-correction...[/yellow]")
        state = generate(state)
        state = execute(state)

    if state["execution_error"]:
        console.print(f"[red]Execution failed: {state['execution_error']}[/red]")
        return state

    present(state)
    return state


def main() -> None:
    catalog = load_catalog()
    conversation: list[dict] = []

    console.print("[bold]Metric Studio[/bold] — NL2SQL Agent for Securities Time-Series")
    console.print("Type your query, or 'exit' to quit.\n")

    while True:
        try:
            raw_query = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nGoodbye.")
            break

        if raw_query.lower() in ("exit", "quit", "q"):
            console.print("Goodbye.")
            break

        if not raw_query:
            continue

        state = run_query(raw_query, conversation, catalog)

        if state.get("sql"):
            conversation.append({"role": "user", "content": raw_query})
            conversation.append({"role": "assistant", "content": state["sql"]})

        console.print()
        try:
            action = input("[r] Refine  [n] New query  [q] Quit: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break

        if action == "q":
            console.print("Goodbye.")
            break
        elif action == "n":
            conversation = []


if __name__ == "__main__":
    main()
