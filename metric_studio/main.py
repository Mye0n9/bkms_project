import argparse
import sys
from pathlib import Path
import yaml
from rich.console import Console

from pipeline.state import AgentState
from pipeline.understand import understand
from pipeline.clarify import clarify
from pipeline.specify import specify
from pipeline.generate import generate, generate_freeform
from pipeline.execute import execute
from pipeline.present import present

console = Console()
CATALOG_PATH = Path(__file__).parent / "catalog" / "metrics.yaml"


def load_catalog() -> list[dict]:
    with open(CATALOG_PATH) as f:
        return yaml.safe_load(f)


def run_query(raw_query: str, conversation: list, catalog: list, verbose: bool = False) -> AgentState:
    state: AgentState = {
        "raw_query": raw_query,
        "intent": "",
        "metric_id": None,
        "requires_freeform": False,
        "resolved_params": {},
        "unresolved_params": [],
        "metric_spec": None,
        "sql": None,
        "result": None,
        "execution_error": None,
        "is_connection_error": False,
        "conversation": conversation,
        "verbose": verbose,
    }

    console.print("[dim]Analyzing query...[/dim]")
    state = understand(state, catalog)

    if state["metric_id"] is None:
        if not state.get("requires_freeform"):
            console.print("[yellow]Could not match your query to a known metric pattern.[/yellow]")
            console.print("Available: " + ", ".join(m["display_name"] for m in catalog))
            return state
        console.print(
            "[dim]No preset pattern matches exactly — generating custom SQL for this request...[/dim]"
        )
        generate_fn = generate_freeform
    else:
        state = clarify(state, catalog)

        try:
            state = specify(state)
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
            return state

        generate_fn = generate

    console.print("[dim]Generating SQL...[/dim]")
    state = generate_fn(state)

    console.print("[dim]Executing query...[/dim]")
    state = execute(state)

    if state["execution_error"] and not state.get("is_connection_error"):
        console.print("[yellow]Query error — attempting self-correction...[/yellow]")
        state = generate_fn(state)
        state = execute(state)

    if state["execution_error"]:
        console.print(f"[red]Execution failed: {state['execution_error']}[/red]")
        return state

    present(state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description="Metric Studio NL2SQL Agent")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print LLM prompts and outputs at each pipeline stage")
    args = parser.parse_args()

    catalog = load_catalog()
    conversation: list[dict] = []

    console.print("[bold]Metric Studio[/bold] — NL2SQL Agent for Securities Time-Series")
    if args.verbose:
        console.print("[dim](verbose mode on — printing LLM prompts and outputs)[/dim]")
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

        state = run_query(raw_query, conversation, catalog, verbose=args.verbose)

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
