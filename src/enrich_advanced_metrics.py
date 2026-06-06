import json
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from advanced_metric_formulas import ADVANCED_METRICS, metric_rows
except ImportError:  # pragma: no cover - used when imported as src.enrich_advanced_metrics
    from src.advanced_metric_formulas import ADVANCED_METRICS, metric_rows


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"
METADATA_DIR = PROCESSED_DIR / "metadata"

PLAYER_MATCH_PATH = PROCESSED_DIR / "stats" / "player_match_stats_cleaned.csv"
MATCHES_PATH = PROCESSED_DIR / "core" / "matches_cleaned.csv"
GOALS_EVENTS_PATH = PROCESSED_DIR / "events" / "goals_events_cleaned.csv"
SOURCES_PATH = METADATA_DIR / "advanced_metric_sources.csv"
COVERAGE_PATH = METADATA_DIR / "advanced_metric_coverage.csv"
REPORT_PATH = LOGS_DIR / "advanced_metric_enrichment_report.json"


NULL_STRINGS = {"", "NULL", "NAN", "NA", "NONE"}


def read_processed(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, keep_default_na=False)


def is_nullish(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().isin(NULL_STRINGS)


def num(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="Float64")
    cleaned = df[column].mask(is_nullish(df[column]), pd.NA)
    return pd.to_numeric(cleaned, errors="coerce")


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.mask(denominator == 0)
    return numerator / denominator


def write_metric(df: pd.DataFrame, metric: str, values: pd.Series) -> dict:
    df[metric] = values.round(6).astype("object")
    df.loc[values.isna(), metric] = "NULL"
    return coverage_row(metric, df[metric])


def coverage_row(metric: str, series: pd.Series) -> dict:
    missing = int(is_nullish(series).sum())
    return {
        "metric": metric,
        "target_file": ADVANCED_METRICS[metric]["target_file"],
        "rows": int(len(series)),
        "filled": int(len(series) - missing),
        "missing": missing,
        "missing_pct": round(missing / len(series), 6) if len(series) else 0,
        "formula": ADVANCED_METRICS[metric]["formula"],
        "source": ADVANCED_METRICS[metric]["source"],
        "url": ADVANCED_METRICS[metric]["url"],
    }


def add_match_context(player_match: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    context = matches[
        ["match_id", "home_team_id", "away_team_id", "possession_home", "possession_away"]
    ].copy()
    return player_match.merge(context, on="match_id", how="left")


def team_possession_rate(df: pd.DataFrame) -> pd.Series:
    home_possession = num(df, "possession_home")
    away_possession = num(df, "possession_away")
    is_home = df["team_id"].astype(str) == df["home_team_id"].astype(str)
    is_away = df["team_id"].astype(str) == df["away_team_id"].astype(str)
    return home_possession.where(is_home, away_possession.where(is_away, pd.NA))


def opponent_possession_rate(df: pd.DataFrame) -> pd.Series:
    home_possession = num(df, "possession_home")
    away_possession = num(df, "possession_away")
    is_home = df["team_id"].astype(str) == df["home_team_id"].astype(str)
    is_away = df["team_id"].astype(str) == df["away_team_id"].astype(str)
    return away_possession.where(is_home, home_possession.where(is_away, pd.NA))


def enrich_player_match_stats() -> list[dict]:
    if not PLAYER_MATCH_PATH.exists():
        return []

    df = read_processed(PLAYER_MATCH_PATH)
    matches = read_processed(MATCHES_PATH) if MATCHES_PATH.exists() else pd.DataFrame()
    work = add_match_context(df, matches) if not matches.empty else df.copy()
    coverage = []

    passes_completed = num(work, "passes_completed")
    passes_attempted = num(work, "passes_attempted")
    pass_accuracy = num(work, "pass_accuracy")
    pass_accuracy = pass_accuracy.fillna(safe_div(passes_completed, passes_attempted))
    dribbles = num(work, "dribbles")
    tackles = num(work, "tackles")
    tackles_won = num(work, "tackles_won")
    interceptions = num(work, "interceptions")
    clearances = num(work, "clearances")
    fouls_committed = num(work, "fouls_committed")
    yellow_cards = num(work, "yellow_cards")
    red_cards = num(work, "red_cards")
    distance_covered = num(work, "distance_covered")
    top_speed = num(work, "top_speed")
    minutes_played = num(work, "minutes_played")

    coverage.append(write_metric(df, "expected_threat_total", (passes_completed * 0.05) + (dribbles * 0.07)))
    coverage.append(
        write_metric(
            df,
            "vaep_rating",
            (passes_completed + dribbles + tackles_won + interceptions) - fouls_committed,
        )
    )
    coverage.append(write_metric(df, "expected_assists", passes_attempted * pass_accuracy * 0.1))
    coverage.append(
        write_metric(df, "progressive_pass_dist", distance_covered * safe_div(passes_completed, passes_attempted))
    )
    coverage.append(
        write_metric(
            df,
            "defensive_action_height",
            safe_div(tackles + interceptions + clearances, distance_covered),
        )
    )
    defensive_actions = tackles + interceptions + fouls_committed
    coverage.append(write_metric(df, "pressing_intensity", safe_div(opponent_possession_rate(work), defensive_actions)))

    season_distance = distance_covered.groupby(work["player_id"]).transform("mean")
    coverage.append(write_metric(df, "acwr_index", safe_div(distance_covered, season_distance)))
    coverage.append(
        write_metric(
            df,
            "metabolic_power",
            (15.48 * top_speed) * safe_div(distance_covered, minutes_played * 60),
        )
    )
    coverage.append(
        write_metric(
            df,
            "foul_severity_index",
            (fouls_committed * 1) + (yellow_cards * 3) + (red_cards * 6),
        )
    )
    coverage.append(
        write_metric(
            df,
            "possession_involvement",
            safe_div(passes_attempted + dribbles, team_possession_rate(work) * minutes_played),
        )
    )

    df.to_csv(PLAYER_MATCH_PATH, index=False, encoding="utf-8")
    return coverage


def enrich_goal_events() -> list[dict]:
    if not GOALS_EVENTS_PATH.exists():
        return []
    df = read_processed(GOALS_EVENTS_PATH)
    coverage = []

    for metric in ["xg_probability", "shot_quality_index"]:
        if metric not in df.columns:
            df[metric] = "NULL"
        coverage.append(coverage_row(metric, df[metric]))

    df.to_csv(GOALS_EVENTS_PATH, index=False, encoding="utf-8")
    return coverage


def write_metadata(coverage: list[dict]) -> None:
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)
    pd.DataFrame(metric_rows()).to_csv(SOURCES_PATH, index=False, encoding="utf-8")
    pd.DataFrame(coverage).to_csv(COVERAGE_PATH, index=False, encoding="utf-8")
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": "Only documented formulas are used. Missing required inputs produce NULL, not inferred constants.",
        "outputs": {
            "player_match_stats": str(PLAYER_MATCH_PATH),
            "goals_events": str(GOALS_EVENTS_PATH),
            "sources": str(SOURCES_PATH),
            "coverage": str(COVERAGE_PATH),
        },
        "coverage": coverage,
    }
    with REPORT_PATH.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)


def main() -> None:
    coverage = []
    coverage.extend(enrich_player_match_stats())
    coverage.extend(enrich_goal_events())
    write_metadata(coverage)
    print(json.dumps({"report": str(REPORT_PATH), "coverage": str(COVERAGE_PATH)}, indent=2))


if __name__ == "__main__":
    main()
