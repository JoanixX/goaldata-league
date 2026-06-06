import pandas as pd

from pathlib import Path

from src.data_quality import (
    adjacent_repetition_profile,
    aggregate_target_profile,
    completeness_profile,
    detect_numeric_anomalies,
    duplicate_profile,
    write_quality_report,
)


def test_completeness_profile_flags_columns_over_threshold():
    df = pd.DataFrame({"complete": [1, 2, 3], "missing": ["NOT_AVAILABLE_IN_SOURCE", 2, 3]})

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


def test_quality_report_enforces_minimum_records_per_dataset(tmp_path: Path):
    outputs = {
        "small": pd.DataFrame({"value": [1, 2]}),
        "large": pd.DataFrame({"value": [1, 2, 3]}),
    }

    report = write_quality_report(outputs, tmp_path / "quality.json", min_records=3)

    assert report["datasets"]["small"]["passes_record_requirement"] is False
    assert report["datasets"]["large"]["passes_record_requirement"] is True
    assert report["passes_minimum_record_requirement"] is False


def test_duplicate_profile_flags_dataset_keys():
    df = pd.DataFrame({"player_id": ["p1", "p1"], "player_name": ["Neuer", "Manuel Neuer"]})

    profile = duplicate_profile(df, "players")

    assert profile["duplicate_rows"] == 1
    assert profile["key_fields"] == ["player_id"]


def test_adjacent_repetition_profile_flags_back_to_back_repeats():
    df = pd.DataFrame(
        [
            {"match_id": "m1", "season": "2024-2025", "home_score": 1},
            {"match_id": "m2", "season": "2024-2025", "home_score": 1},
        ]
    )

    profile = adjacent_repetition_profile(df, "matches")

    assert profile["exact_adjacent_repeats"] == 0
    assert profile["similar_adjacent_repeats"] == 1


def test_aggregate_target_profile_tracks_required_players_and_teams():
    outputs = {
        "players": pd.DataFrame({"player_id": ["p1", "p2"]}),
        "teams": pd.DataFrame({"team_id": ["t1"]}),
    }

    profile = aggregate_target_profile(outputs, min_unique_players=2, min_teams=2)

    assert profile["passes_unique_players_requirement"] is True
    assert profile["passes_teams_requirement"] is False
