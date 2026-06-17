from unittest.mock import MagicMock, patch
from pipeline.generate import generate, generate_freeform

RSI_SPEC = {
    "id": "rsi_overbought",
    "display_name": "RSI Overbought",
    "strategy": "pg_function",
    "function": "calc_rsi",
    "resolved_params": {"window": 14, "threshold": 70},
}

EXPECTED_SQL = "SELECT t.ticker, r.xymd, r.rsi_value FROM public.calc_rsi(14, CURRENT_DATE) r JOIN public.tickers t ON t.ticker_id = r.ticker_id WHERE r.rsi_value > 70 LIMIT 100;"


def _mock_llm_response(sql: str):
    mock_response = MagicMock()
    mock_response.content = sql
    mock_instance = MagicMock()
    mock_instance.invoke.return_value = mock_response
    return mock_instance


def test_generate_returns_sql():
    with patch("pipeline.generate.get_llm", return_value=_mock_llm_response(EXPECTED_SQL)):
        state = {"metric_spec": RSI_SPEC, "execution_error": None, "conversation": []}
        result = generate(state)
    assert result["sql"] == EXPECTED_SQL
    assert result["execution_error"] is None


def test_generate_strips_markdown_fences():
    fenced = f"```sql\n{EXPECTED_SQL}\n```"
    with patch("pipeline.generate.get_llm", return_value=_mock_llm_response(fenced)):
        state = {"metric_spec": RSI_SPEC, "execution_error": None, "conversation": []}
        result = generate(state)
    assert "```" not in result["sql"]
    assert "SELECT" in result["sql"]


FREEFORM_SQL = (
    "WITH swings AS (SELECT ticker_id, xymd, clos FROM public.daily_prices "
    "WHERE xymd > CURRENT_DATE - 90) SELECT t.ticker, s.xymd, s.clos FROM swings s "
    "JOIN public.tickers t ON t.ticker_id = s.ticker_id LIMIT 100;"
)


def test_generate_freeform_returns_sql_using_full_schema():
    with patch("pipeline.generate.get_llm", return_value=_mock_llm_response(FREEFORM_SQL)):
        state = {
            "raw_query": "Based on recent 90 days, find a ticker that BOS during "
            "Bullish Bias and has RSI less than 50.",
            "intent": "Find tickers with a structure break during a bullish bias and RSI under 50",
            "execution_error": None,
        }
        result = generate_freeform(state)
    assert result["sql"] == FREEFORM_SQL
    assert result["execution_error"] is None


def test_generate_freeform_strips_markdown_fences():
    fenced = f"```sql\n{FREEFORM_SQL}\n```"
    with patch("pipeline.generate.get_llm", return_value=_mock_llm_response(fenced)):
        state = {"raw_query": "complex query", "intent": "complex intent", "execution_error": None}
        result = generate_freeform(state)
    assert "```" not in result["sql"]
    assert "SELECT" in result["sql"]
