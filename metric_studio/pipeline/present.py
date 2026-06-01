import pandas as pd
from rich.console import Console
from rich.table import Table
from pipeline.state import AgentState

console = Console()
MAX_DISPLAY_ROWS = 100


def present(state: AgentState) -> None:
    df: pd.DataFrame = state.get("result")

    if df is None or df.empty:
        console.print("[yellow]No results returned.[/yellow]")
        return

    table = Table(show_header=True, header_style="bold cyan", border_style="dim")
    for col in df.columns:
        table.add_column(str(col), no_wrap=False)

    display_df = df.head(MAX_DISPLAY_ROWS)
    for _, row in display_df.iterrows():
        table.add_row(*[str(v) for v in row])

    console.print(table)
    suffix = f" (showing first {MAX_DISPLAY_ROWS})" if len(df) > MAX_DISPLAY_ROWS else ""
    console.print(f"[dim]{len(df)} rows returned{suffix}.[/dim]")
