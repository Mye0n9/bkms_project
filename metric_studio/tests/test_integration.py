"""
Integration test: wires all stages together with mocked LLM and DB.
Verifies that a full query round-trip produces a DataFrame result.
"""
import pandas as pd
from unittest.mock import MagicMock, patch
from main import run_query
from pipeline.understand import UnderstandOutput

CATALOG = [
    {
        "id": "rsi_overbought",
        "display_name": "RSI Overbought",
        "aliases": ["overbought"],
        "strategy": "pg_function",
        "function": "calc_rsi",
        "params": {
            "window":    {"type": "integer", "clarify_if_missing": False, "default": 14},
            "threshold": {"type": "numeric", "clarify_if_missing": False, "default": 70},
        },
    }
]

FAKE_SQL = "SELECT t.ticker FROM public.calc_rsi(14, CURRENT_DATE) r JOIN public.tickers t ON t.ticker_id = r.ticker_id WHERE r.rsi_value > 70 LIMIT 100;"

FAKE_DF = pd.DataFrame({
    "ticker": ["AAPL"],
    "xymd": ["2026-05-30"],
    "rsi_value": [74.3],
    "clos": [189.50],
})


def _mock_understand():
    output = UnderstandOutput(
        intent="Find overbought stocks",
        metric_id="rsi_overbought",
        resolved_params={"window": 14, "threshold": 70},
    )
    structured = MagicMock()
    structured.invoke.return_value = output
    llm = MagicMock()
    llm.with_structured_output.return_value = structured
    return llm


def _mock_generate():
    response = MagicMock()
    response.content = FAKE_SQL
    llm = MagicMock()
    llm.invoke.return_value = response
    return llm


def _mock_db_engine():
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [tuple(FAKE_DF.iloc[0])]
    mock_result.keys.return_value = list(FAKE_DF.columns)
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value = mock_result
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def test_full_pipeline_returns_dataframe():
    with (
        patch("pipeline.understand.ChatAnthropic", return_value=_mock_understand()),
        patch("pipeline.generate.ChatAnthropic", return_value=_mock_generate()),
        patch("pipeline.execute.create_engine", return_value=_mock_db_engine()),
    ):
        state = run_query("Find overbought stocks", [], CATALOG)

    assert state["result"] is not None
    assert isinstance(state["result"], pd.DataFrame)
    assert state["execution_error"] is None
    assert state["result"].iloc[0]["ticker"] == "AAPL"
