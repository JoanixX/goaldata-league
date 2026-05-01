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
    assert report["outputs"]["matches"]["rows"] == 0
