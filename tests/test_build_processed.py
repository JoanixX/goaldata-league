from pathlib import Path

import pandas as pd

from src import build_processed


def test_parse_score_and_goal_events():
    assert build_processed.parse_score("2-1") == (2, 1)

    events = build_processed.parse_goal_events("Messi 45'; 90+1' (P); Ramos 12' (OG)", "m1")

    assert len(events) == 3
    assert events[1]["goal_type"] == "penalty"
    assert events[2]["goal_type"] == "own_goal"


def test_frame_enforces_schema_and_deduplicates():
    rows = [
        {"team_id": "team_1", "team_name": "A"},
        {"team_id": "team_1", "team_name": "A Duplicate"},
    ]

    df = build_processed.frame(rows, build_processed.TEAM_COLUMNS, ["team_id"])

    assert list(df.columns) == build_processed.TEAM_COLUMNS
    assert len(df) == 1
    assert df.iloc[0]["country"] == "NULL"


def test_parquet_ready_frame_converts_null_markers_to_nullable_types():
    df = pd.DataFrame({"home_score": [1, "NULL"], "possession_home": ["55%", "NULL"]})

    ready = build_processed.parquet_ready_frame("matches", df)

    assert pd.isna(ready.loc[1, "home_score"])
    assert pd.isna(ready.loc[0, "possession_home"])


def test_normalize_invalid_missing_values_reclassifies_failed_possession_parse():
    df = pd.DataFrame({"possession_home": [0.0, 0.55], "possession_away": [0.0, 0.45]})

    cleaned = build_processed.normalize_invalid_missing_values(df)

    assert cleaned.loc[0, "possession_home"] == "NULL"
    assert cleaned.loc[0, "possession_away"] == "NULL"
    assert cleaned.loc[1, "possession_home"] == 0.55


def test_write_outputs_creates_cleaned_structure(tmp_path: Path, monkeypatch):
    outputs = {
        "matches": pd.DataFrame(columns=build_processed.MATCH_COLUMNS),
        "players": pd.DataFrame(columns=build_processed.PLAYER_COLUMNS),
        "teams": pd.DataFrame(columns=build_processed.TEAM_COLUMNS),
        "goals": pd.DataFrame(columns=build_processed.GOAL_COLUMNS),
        "gk": pd.DataFrame(columns=build_processed.GOALKEEPER_COLUMNS),
        "match_stats": pd.DataFrame(columns=build_processed.PLAYER_MATCH_COLUMNS),
        "season_stats": pd.DataFrame(columns=build_processed.PLAYER_SEASON_COLUMNS),
    }
    monkeypatch.setattr(
        build_processed,
        "OUTPUTS",
        {
            "matches": tmp_path / "processed" / "core" / "matches_cleaned.csv",
            "players": tmp_path / "processed" / "core" / "players_cleaned.csv",
            "teams": tmp_path / "processed" / "core" / "teams_cleaned.csv",
            "goals": tmp_path / "processed" / "events" / "goals_events_cleaned.csv",
            "gk": tmp_path / "processed" / "stats" / "goalkeeper_stats_cleaned.csv",
            "match_stats": tmp_path / "processed" / "stats" / "player_match_stats_cleaned.csv",
            "season_stats": tmp_path / "processed" / "stats" / "player_season_stats_cleaned.csv",
        },
    )
    monkeypatch.setattr(build_processed, "LOGS_DIR", tmp_path / "logs")

    report = build_processed.write_outputs(outputs)

    assert (tmp_path / "processed" / "core" / "matches_cleaned.csv").exists()
    assert (tmp_path / "processed" / "core" / "matches_cleaned.parquet").exists()
    assert (tmp_path / "logs" / "data_quality_report.json").exists()
    assert report["outputs"]["matches"]["rows"] == 0
    assert report["outputs"]["matches"]["parquet_path"].endswith(".parquet")
