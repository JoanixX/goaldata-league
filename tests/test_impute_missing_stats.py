from pathlib import Path

import pandas as pd

from src import impute_missing_stats


def test_imputation_preserves_observed_values_and_fills_formulas(tmp_path: Path, monkeypatch):
    stats_path = tmp_path / "player_match_stats_cleaned.csv"
    players_path = tmp_path / "players_cleaned.csv"

    pd.DataFrame(
        [
            {
                "player_id": "p1",
                "match_id": "m1",
                "minutes_played": 90,
                "shots": 5,
                "shots_on_target": 2,
                "shots_blocked": 1,
                "shots_off_target": "NULL",
                "passes_completed": 40,
                "passes_attempted": 50,
                "pass_accuracy": "NULL",
                "touches": 70,
            }
        ]
    ).to_csv(stats_path, index=False)
    pd.DataFrame([{"player_id": "p1", "position": "Forward"}]).to_csv(players_path, index=False)

    monkeypatch.setattr(impute_missing_stats, "PLAYER_MATCH_PATH", stats_path)
    monkeypatch.setattr(impute_missing_stats, "PLAYERS_PATH", players_path)
    monkeypatch.setattr(impute_missing_stats, "PLAYER_SEASON_PATH", tmp_path / "missing_season_stats.csv")
    monkeypatch.setattr(impute_missing_stats, "MATCHES_PATH", tmp_path / "missing_matches.csv")
    monkeypatch.setattr(impute_missing_stats, "TOP_LEAGUES_STATS_PATH", tmp_path / "missing_top_leagues.csv")

    report = impute_missing_stats.impute_player_match_stats()
    df = pd.read_csv(stats_path, keep_default_na=False)

    assert str(df.loc[0, "shots_off_target"]) == "2"
    assert float(df.loc[0, "pass_accuracy"]) == 0.8
    assert str(df.loc[0, "touches"]) == "70"
    assert len(report["changes"]) == 2
