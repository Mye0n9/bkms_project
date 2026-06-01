from unittest.mock import MagicMock, patch
from pipeline.generate import generate

RSI_SPEC = {
    "id": "rsi_overbought",
    "display_name": "RSI Overbought",
    "strategy": "pg_function",
    "function": "calc_rsi",
    "resolved_params": {"window": 14, "threshold": 70},
}

EXPECTED_SQL = "SELECT t.ticker, r.xymd, r.rsi_value FROM public.calc_rsi(14, CURRENT_DATE) r JOIN public.tickers t ON t.ticker_id = r.ticker_id WHERE r.rsi_value > 70 LIMIT 100;"


def _mock_anthropic(sql: str):
    mock_response = MagicMock()
    mock_response.content = sql
    mock_instance = MagicMock()
    mock_instance.invoke.return_value = mock_response
    return mock_instance


def test_generate_returns_sql():
    with patch("pipeline.generate.ChatAnthropic", return_value=_mock_anthropic(EXPECTED_SQL)):
        state = {"metric_spec": RSI_SPEC, "execution_error": None, "conversation": []}
        result = generate(state)
    assert result["sql"] == EXPECTED_SQL
    assert result["execution_error"] is None


def test_generate_strips_markdown_fences():
    fenced = f"```sql\n{EXPECTED_SQL}\n```"
    with patch("pipeline.generate.ChatAnthropic", return_value=_mock_anthropic(fenced)):
        state = {"metric_spec": RSI_SPEC, "execution_error": None, "conversation": []}
        result = generate(state)
    assert "```" not in result["sql"]
    assert "SELECT" in result["sql"]
