import pandas as pd

from src.eda_report import numeric_summary


def test_numeric_summary_reports_numeric_columns_only():
    df = pd.DataFrame({"value": [1, 2, 3], "label": ["a", "b", "c"]})

    summary = numeric_summary(df)

    assert summary["value"]["median"] == 2.0
    assert "label" not in summary
