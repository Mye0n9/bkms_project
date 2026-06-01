import pytest
from pipeline.specify import load_catalog, find_metric, get_unresolved_params, specify


def test_catalog_loads_ten_patterns():
    catalog = load_catalog()
    assert len(catalog) == 10


def test_find_metric_by_id():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    assert metric is not None
    assert metric["id"] == "rsi_overbought"
    assert metric["strategy"] == "pg_function"


def test_find_metric_returns_none_for_unknown():
    catalog = load_catalog()
    assert find_metric(catalog, "does_not_exist") is None


def test_get_unresolved_params_all_missing():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    unresolved = get_unresolved_params(metric, {})
    assert "window" in unresolved
    assert "threshold" in unresolved


def test_get_unresolved_params_partially_resolved():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    unresolved = get_unresolved_params(metric, {"window": 14})
    assert "window" not in unresolved
    assert "threshold" in unresolved


def test_get_unresolved_params_all_resolved():
    catalog = load_catalog()
    metric = find_metric(catalog, "rsi_overbought")
    unresolved = get_unresolved_params(metric, {"window": 14, "threshold": 70})
    assert unresolved == []


def test_specify_builds_pg_function_spec():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {"window": 14, "threshold": 70},
        "unresolved_params": [],
    }
    result = specify(state)
    spec = result["metric_spec"]
    assert spec["strategy"] == "pg_function"
    assert spec["function"] == "calc_rsi"
    assert spec["resolved_params"]["window"] == 14
    assert result["unresolved_params"] == []


def test_specify_builds_precomputed_table_spec():
    state = {
        "metric_id": "moving_average_cross",
        "resolved_params": {"short_window": "50", "long_window": "200", "direction": "above"},
        "unresolved_params": [],
    }
    result = specify(state)
    spec = result["metric_spec"]
    assert spec["strategy"] == "precomputed_table"
    assert spec["table"] == "daily_ma"


def test_specify_raises_on_unresolved():
    state = {
        "metric_id": "rsi_overbought",
        "resolved_params": {},
        "unresolved_params": ["window", "threshold"],
    }
    with pytest.raises(ValueError, match="Unresolved params"):
        specify(state)


def test_specify_raises_on_unknown_metric():
    state = {
        "metric_id": "nonexistent",
        "resolved_params": {},
        "unresolved_params": [],
    }
    with pytest.raises(ValueError, match="Unknown metric_id"):
        specify(state)
