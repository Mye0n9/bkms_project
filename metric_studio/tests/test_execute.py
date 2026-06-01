import pandas as pd
from unittest.mock import MagicMock, patch
from pipeline.execute import execute


def _mock_engine(rows, columns):
    mock_result = MagicMock()
    mock_result.fetchall.return_value = rows
    mock_result.keys.return_value = columns
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value = mock_result
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn
    return mock_engine


def test_execute_returns_dataframe():
    engine = _mock_engine(
        rows=[("AAPL", "2026-05-30", 74.3, 189.50)],
        columns=["ticker", "xymd", "rsi_value", "clos"],
    )
    with patch("pipeline.execute.create_engine", return_value=engine):
        state = {"sql": "SELECT 1"}
        result = execute(state)

    assert result["execution_error"] is None
    assert isinstance(result["result"], pd.DataFrame)
    assert list(result["result"].columns) == ["ticker", "xymd", "rsi_value", "clos"]
    assert result["result"].iloc[0]["ticker"] == "AAPL"


def test_execute_captures_db_error():
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = Exception("SSL connection error")
    with patch("pipeline.execute.create_engine", return_value=mock_engine):
        state = {"sql": "SELECT 1"}
        result = execute(state)

    assert result["result"] is None
    assert "SSL connection error" in result["execution_error"]


def test_execute_returns_empty_dataframe_for_zero_rows():
    engine = _mock_engine(rows=[], columns=["ticker", "xymd", "rsi_value", "clos"])
    with patch("pipeline.execute.create_engine", return_value=engine):
        state = {"sql": "SELECT 1 WHERE FALSE"}
        result = execute(state)

    assert result["execution_error"] is None
    assert len(result["result"]) == 0
