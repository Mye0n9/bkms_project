from unittest.mock import patch
from pipeline.clarify import clarify

MOCK_CATALOG = [
    {
        "id": "rsi_overbought",
        "params": {
            "window":    {"type": "integer", "clarify_if_missing": True, "question": "RSI window?"},
            "threshold": {"type": "numeric", "clarify_if_missing": True, "question": "Threshold?"},
            "direction": {"type": "enum",    "clarify_if_missing": False, "values": ["above", "below"]},
        },
    }
]


def test_clarify_resolves_integer_and_numeric():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {},
        "unresolved_params": ["window", "threshold"],
    }
    with patch("builtins.input", side_effect=["14", "70"]):
        result = clarify(state, MOCK_CATALOG)
    assert result["resolved_params"]["window"] == 14
    assert result["resolved_params"]["threshold"] == 70.0
    assert result["unresolved_params"] == []


def test_clarify_no_op_when_nothing_unresolved():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {"window": 14, "threshold": 70.0},
        "unresolved_params": [],
    }
    result = clarify(state, MOCK_CATALOG)
    assert result["resolved_params"] == {"window": 14, "threshold": 70.0}


def test_clarify_retries_invalid_enum():
    mock_catalog = [
        {
            "id": "moving_average_cross",
            "params": {
                "direction": {
                    "type": "enum",
                    "values": ["above", "below"],
                    "clarify_if_missing": True,
                    "question": "Direction?",
                }
            },
        }
    ]
    state = {
        "metric_id": "moving_average_cross",
        "resolved_params": {},
        "unresolved_params": ["direction"],
    }
    with patch("builtins.input", side_effect=["sideways", "above"]):
        result = clarify(state, mock_catalog)
    assert result["resolved_params"]["direction"] == "above"
