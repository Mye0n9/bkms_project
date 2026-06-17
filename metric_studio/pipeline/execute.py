import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from pipeline.state import AgentState
from config import settings


def execute(state: AgentState) -> AgentState:
    verbose = state.get("verbose", False)
    if verbose:
        print("\n" + "="*60)
        print("[VERBOSE] EXECUTE — SQL being run:")
        print("-"*60)
        print(state["sql"])
        print("="*60 + "\n")

    engine = create_engine(settings.db_url)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(state["sql"]))
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        if verbose:
            print(f"[VERBOSE] EXECUTE — {len(df)} rows returned.\n")
        return {**state, "result": df, "execution_error": None}
    except OperationalError as exc:
        if verbose:
            print(f"[VERBOSE] EXECUTE — error: {exc}\n")
        return {**state, "result": None, "execution_error": str(exc), "is_connection_error": True}
    except Exception as exc:
        if verbose:
            print(f"[VERBOSE] EXECUTE — error: {exc}\n")
        return {**state, "result": None, "execution_error": str(exc), "is_connection_error": False}
