import pandas as pd
from sqlalchemy import create_engine, text
from pipeline.state import AgentState
from config import settings


def execute(state: AgentState) -> AgentState:
    engine = create_engine(settings.db_url)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(state["sql"]))
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        return {**state, "result": df, "execution_error": None}
    except Exception as exc:
        return {**state, "result": None, "execution_error": str(exc)}
