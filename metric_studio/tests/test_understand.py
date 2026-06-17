from unittest.mock import MagicMock, patch
from pipeline.understand import understand, UnderstandOutput

MOCK_CATALOG = [
    {
        "id": "rsi_overbought",
        "display_name": "RSI Overbought",
        "aliases": ["overbought", "RSI high"],
        "params": {
            "window":    {"type": "integer", "clarify_if_missing": True, "question": "Window?"},
            "threshold": {"type": "numeric", "clarify_if_missing": True, "question": "Threshold?"},
        },
    }
]


def _mock_llm(metric_id, resolved_params, requires_freeform=False):
    mock_output = UnderstandOutput(
        intent="Find overbought stocks using RSI",
        metric_id=metric_id,
        resolved_params=resolved_params,
        requires_freeform=requires_freeform,
    )
    mock_structured = MagicMock()
    mock_structured.invoke.return_value = mock_output
    mock_llm_instance = MagicMock()
    mock_llm_instance.with_structured_output.return_value = mock_structured
    return mock_llm_instance


def test_understand_matches_metric_and_extracts_params():
    with patch("pipeline.understand.get_llm", return_value=_mock_llm("rsi_overbought", {"window": 14})):
        state = {"raw_query": "Find overbought stocks RSI 14-day", "conversation": []}
        result = understand(state, catalog=MOCK_CATALOG)

    assert result["metric_id"] == "rsi_overbought"
    assert result["resolved_params"]["window"] == 14
    assert "threshold" in result["unresolved_params"]
    assert "window" not in result["unresolved_params"]


def test_understand_returns_none_metric_id_when_no_match():
    with patch("pipeline.understand.get_llm", return_value=_mock_llm(None, {})):
        state = {"raw_query": "Something completely unrelated", "conversation": []}
        result = understand(state, catalog=MOCK_CATALOG)

    assert result["metric_id"] is None
    assert result["unresolved_params"] == []
    assert result["requires_freeform"] is False


def test_understand_flags_requires_freeform_for_complex_query():
    with patch(
        "pipeline.understand.get_llm",
        return_value=_mock_llm(None, {}, requires_freeform=True),
    ):
        state = {
            "raw_query": "Based on recent 90 days, find a ticker that BOS during "
            "Bullish Bias and has RSI less than 50.",
            "conversation": [],
        }
        result = understand(state, catalog=MOCK_CATALOG)

    assert result["metric_id"] is None
    assert result["requires_freeform"] is True
    assert result["unresolved_params"] == []
