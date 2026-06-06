from pathlib import Path

import pandas as pd

from src import enrich_advanced_metrics


def test_advanced_metrics_use_only_available_inputs(tmp_path: Path, monkeypatch):
    player_match_path = tmp_path / "player_match_stats_cleaned.csv"
    matches_path = tmp_path / "matches_cleaned.csv"
    goals_path = tmp_path / "goals_events_cleaned.csv"
    metadata_dir = tmp_path / "metadata"
    logs_dir = tmp_path / "logs"

    pd.DataFrame(
        [
            {
                "player_id": "p1",
                "match_id": "m1",
                "team_id": "home",
                "minutes_played": 90,
                "passes_completed": 30,
                "passes_attempted": 40,
                "pass_accuracy": "NULL",
                "dribbles": 2,
                "tackles": 3,
                "tackles_won": 2,
                "interceptions": 1,
                "clearances": "NULL",
                "fouls_committed": 1,
                "yellow_cards": 1,
                "red_cards": 0,
                "distance_covered": "NULL",
                "top_speed": "NULL",
            }
        ]
    ).to_csv(player_match_path, index=False)
    pd.DataFrame(
        [
            {
                "match_id": "m1",
                "home_team_id": "home",
                "away_team_id": "away",
                "possession_home": 0.6,
                "possession_away": 0.4,
            }
        ]
    ).to_csv(matches_path, index=False)
    pd.DataFrame([{"goal_id": "g1"}]).to_csv(goals_path, index=False)

    monkeypatch.setattr(enrich_advanced_metrics, "PLAYER_MATCH_PATH", player_match_path)
    monkeypatch.setattr(enrich_advanced_metrics, "MATCHES_PATH", matches_path)
    monkeypatch.setattr(enrich_advanced_metrics, "GOALS_EVENTS_PATH", goals_path)
    monkeypatch.setattr(enrich_advanced_metrics, "METADATA_DIR", metadata_dir)
    monkeypatch.setattr(enrich_advanced_metrics, "LOGS_DIR", logs_dir)
    monkeypatch.setattr(enrich_advanced_metrics, "SOURCES_PATH", metadata_dir / "advanced_metric_sources.csv")
    monkeypatch.setattr(enrich_advanced_metrics, "COVERAGE_PATH", metadata_dir / "advanced_metric_coverage.csv")
    monkeypatch.setattr(enrich_advanced_metrics, "REPORT_PATH", logs_dir / "advanced_metric_enrichment_report.json")

    coverage = []
    coverage.extend(enrich_advanced_metrics.enrich_player_match_stats())
    coverage.extend(enrich_advanced_metrics.enrich_goal_events())
    enrich_advanced_metrics.write_metadata(coverage)

    stats = pd.read_csv(player_match_path, keep_default_na=False)
    goals = pd.read_csv(goals_path, keep_default_na=False)

    assert float(stats.loc[0, "expected_threat_total"]) == 1.64
    assert float(stats.loc[0, "expected_assists"]) == 3.0
    assert float(stats.loc[0, "foul_severity_index"]) == 4.0
    assert stats.loc[0, "progressive_pass_dist"] == "NULL"
    assert goals.loc[0, "xg_probability"] == "NULL"
    assert (metadata_dir / "advanced_metric_coverage.csv").exists()
