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
        patch("pipeline.understand.get_llm", return_value=_mock_understand()),
        patch("pipeline.generate.get_llm", return_value=_mock_generate()),
        patch("pipeline.execute.create_engine", return_value=_mock_db_engine()),
    ):
        state = run_query("Find overbought stocks", [], CATALOG)

    assert state["result"] is not None
    assert isinstance(state["result"], pd.DataFrame)
    assert state["execution_error"] is None
    assert state["result"].iloc[0]["ticker"] == "AAPL"


FREEFORM_SQL = (
    "WITH swings AS (SELECT ticker_id, xymd, clos FROM public.daily_prices "
    "WHERE xymd > CURRENT_DATE - 90) SELECT t.ticker, s.xymd, s.clos FROM swings s "
    "JOIN public.tickers t ON t.ticker_id = s.ticker_id LIMIT 100;"
)


def _mock_understand_freeform():
    output = UnderstandOutput(
        intent="Find tickers with a structure break during a bullish bias and RSI under 50",
        metric_id=None,
        resolved_params={},
        requires_freeform=True,
    )
    structured = MagicMock()
    structured.invoke.return_value = output
    llm = MagicMock()
    llm.with_structured_output.return_value = structured
    return llm


def _mock_generate_freeform():
    response = MagicMock()
    response.content = FREEFORM_SQL
    llm = MagicMock()
    llm.invoke.return_value = response
    return llm


def test_full_pipeline_freeform_path_returns_dataframe():
    complex_query = (
        "Based on recent 90 days, find a ticker that BOS during Bullish Bias "
        "and has RSI less than 50."
    )
    with (
        patch("pipeline.understand.get_llm", return_value=_mock_understand_freeform()),
        patch("pipeline.generate.get_llm", return_value=_mock_generate_freeform()),
        patch("pipeline.execute.create_engine", return_value=_mock_db_engine()),
    ):
        state = run_query(complex_query, [], CATALOG)

    assert state["metric_id"] is None
    assert state["metric_spec"] is None
    assert state["sql"] == FREEFORM_SQL
    assert state["execution_error"] is None
    assert isinstance(state["result"], pd.DataFrame)


def test_full_pipeline_no_match_no_freeform_skips_generation():
    output = UnderstandOutput(
        intent="Unrelated request",
        metric_id=None,
        resolved_params={},
        requires_freeform=False,
    )
    structured = MagicMock()
    structured.invoke.return_value = output
    llm = MagicMock()
    llm.with_structured_output.return_value = structured

    with patch("pipeline.understand.get_llm", return_value=llm):
        state = run_query("What's the weather today?", [], CATALOG)

    assert state["metric_id"] is None
    assert state["sql"] is None
    assert state["result"] is None
