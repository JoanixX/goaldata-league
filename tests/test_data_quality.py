import pandas as pd

from src.data_quality import completeness_profile, detect_numeric_anomalies


def test_completeness_profile_flags_columns_over_threshold():
    df = pd.DataFrame({"complete": [1, 2, 3], "missing": ["NULL", 2, 3]})

    profile = completeness_profile(df, null_threshold=0.25)

    assert profile["column_profiles"]["complete"]["passes_threshold"] is True
    assert profile["column_profiles"]["missing"]["passes_threshold"] is False
    assert profile["passes_null_threshold"] is False


def test_detect_numeric_anomalies_for_invalid_formula_outputs():
    df = pd.DataFrame(
        [
            {
                "passes_completed": 55,
                "passes_attempted": 50,
                "shots": 2,
                "shots_on_target": 3,
                "pass_accuracy": 1.2,
            }
        ]
    )

    anomalies = detect_numeric_anomalies(df, "match_stats")

    assert {item["type"] for item in anomalies} == {
        "passes_completed_gt_attempted",
        "shots_on_target_gt_shots",
        "pass_accuracy_out_of_range",
    }
