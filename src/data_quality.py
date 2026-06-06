from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


MISSING_MARKERS = {
    "",
    "NULL",
    "NAN",
    "NONE",
    "NA",
    "NOT_AVAILABLE_IN_SOURCE",
    "NO_ASSIST_OR_NOT_RECORDED",
}
DEFAULT_NULL_THRESHOLD = 0.0025
DEFAULT_MIN_RECORDS = 1_500_000
DEFAULT_MIN_UNIQUE_PLAYERS = 85_000
DEFAULT_MIN_TEAMS = 3_000
MIN_RECORDS_BY_DATASET = {
    "matches": DEFAULT_MIN_RECORDS,
    "goals": DEFAULT_MIN_RECORDS,
}
COUNT_FIELDS = {
    "minutes_played",
    "goals",
    "assists",
    "shots",
    "shots_on_target",
    "shots_off_target",
    "shots_blocked",
    "passes_completed",
    "passes_attempted",
    "crosses_completed",
    "crosses_attempted",
    "dribbles",
    "offsides",
    "tackles",
    "tackles_won",
    "tackles_lost",
    "interceptions",
    "clearances",
    "fouls_committed",
    "fouls_suffered",
    "yellow_cards",
    "red_cards",
    "touches",
    "matches_played",
    "saves",
    "goals_conceded",
    "clean_sheets",
    "penalty_saves",
    "punches",
}
KEY_FIELDS_BY_DATASET = {
    "matches": ["match_id"],
    "players": ["player_id"],
    "teams": ["team_id"],
    "goals": ["goal_id"],
    "gk": ["player_id", "season"],
    "match_stats": ["player_id", "match_id"],
    "season_stats": ["player_id", "season"],
}


def is_missing(value: Any) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip().upper() in MISSING_MARKERS


def missing_mask(series: pd.Series) -> pd.Series:
    return series.map(is_missing)


def completeness_profile(df: pd.DataFrame, null_threshold: float = DEFAULT_NULL_THRESHOLD) -> dict[str, Any]:
    rows = int(len(df))
    columns: dict[str, Any] = {}
    for column in df.columns:
        missing_count = int(missing_mask(df[column]).sum())
        missing_ratio = (missing_count / rows) if rows else 0.0
        columns[column] = {
            "missing_count": missing_count,
            "missing_ratio": round(missing_ratio, 6),
            "passes_threshold": missing_ratio <= null_threshold,
        }
    failing = [column for column, profile in columns.items() if not profile["passes_threshold"]]
    return {
        "rows": rows,
        "columns": int(len(df.columns)),
        "null_threshold": null_threshold,
        "passes_null_threshold": not failing,
        "failing_columns": failing,
        "column_profiles": columns,
    }


def detect_numeric_anomalies(df: pd.DataFrame, dataset_name: str) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []

    def numeric(column: str) -> pd.Series:
        if column not in df.columns:
            return pd.Series([pd.NA] * len(df), index=df.index)
        return pd.to_numeric(df[column].map(lambda value: pd.NA if is_missing(value) else value), errors="coerce")

    def add_masked(mask: pd.Series, anomaly_type: str, message: str) -> None:
        for idx in df.index[mask.fillna(False)]:
            anomalies.append({"row": int(idx), "type": anomaly_type, "message": message})

    for column in COUNT_FIELDS.intersection(df.columns):
        values = numeric(column)
        add_masked(values.notna() & (values < 0), f"{column}_negative", f"{column} cannot be negative.")
        fractional = values.notna() & ((values.round() - values).abs() > 1e-9)
        add_masked(fractional, f"{column}_fractional", f"{column} is a count field and must be an integer.")

    if dataset_name == "matches":
        home_possession = numeric("possession_home")
        away_possession = numeric("possession_away")
        both = home_possession.notna() & away_possession.notna()
        bad_possession = both & ((home_possession + away_possession - 1.0).abs() > 0.02)
        add_masked(bad_possession, "possession_sum", "Home and away possession do not sum to approximately 1.0.")

    if dataset_name == "match_stats":
        attempted = numeric("passes_attempted")
        completed = numeric("passes_completed")
        bad_passes = attempted.notna() & completed.notna() & (completed > attempted)
        add_masked(bad_passes, "passes_completed_gt_attempted", "passes_completed exceeds passes_attempted.")

        shots = numeric("shots")
        shots_on_target = numeric("shots_on_target")
        bad_shots = shots.notna() & shots_on_target.notna() & (shots_on_target > shots)
        add_masked(bad_shots, "shots_on_target_gt_shots", "shots_on_target exceeds shots.")
        if {"shots_blocked", "shots_off_target"}.issubset(df.columns):
            shots_blocked = numeric("shots_blocked").fillna(0)
            shots_off_target = numeric("shots_off_target").fillna(0)
            shot_parts = shots_on_target.fillna(0) + shots_blocked + shots_off_target
            bad_shot_parts = shots.notna() & (shot_parts > shots)
            add_masked(bad_shot_parts, "shot_parts_gt_shots", "shot detail fields exceed total shots.")

        pass_accuracy = numeric("pass_accuracy")
        bad_accuracy = pass_accuracy.notna() & ((pass_accuracy < 0) | (pass_accuracy > 1))
        add_masked(bad_accuracy, "pass_accuracy_out_of_range", "pass_accuracy must be between 0 and 1.")

    if dataset_name == "season_stats":
        attempted = numeric("passes_attempted")
        completed = numeric("passes_completed")
        add_masked(
            attempted.notna() & completed.notna() & (completed > attempted),
            "passes_completed_gt_attempted",
            "passes_completed exceeds passes_attempted.",
        )
        shots = numeric("shots")
        shots_on_target = numeric("shots_on_target")
        add_masked(
            shots.notna() & shots_on_target.notna() & (shots_on_target > shots),
            "shots_on_target_gt_shots",
            "shots_on_target exceeds shots.",
        )

    return anomalies


def duplicate_profile(df: pd.DataFrame, dataset_name: str) -> dict[str, Any]:
    key_fields = KEY_FIELDS_BY_DATASET.get(dataset_name, [])
    if not key_fields or not set(key_fields).issubset(df.columns):
        return {"key_fields": key_fields, "duplicate_rows": 0, "duplicate_examples": []}
    duplicated = df[df.duplicated(subset=key_fields, keep=False)]
    examples = duplicated[key_fields].drop_duplicates().head(20).to_dict("records")
    return {
        "key_fields": key_fields,
        "duplicate_rows": int(df.duplicated(subset=key_fields).sum()),
        "duplicate_examples": examples,
    }


def adjacent_repetition_profile(df: pd.DataFrame, dataset_name: str) -> dict[str, Any]:
    """Find suspicious consecutive repeats without silently deleting anything."""
    if df.empty:
        return {"exact_adjacent_repeats": 0, "similar_adjacent_repeats": 0, "examples": []}

    work = df.astype("string").fillna("<NA>")
    exact = work.eq(work.shift()).all(axis=1)
    if not exact.empty:
        exact.iloc[0] = False
    key_fields = set(KEY_FIELDS_BY_DATASET.get(dataset_name, []))
    comparable_columns = [column for column in work.columns if column not in key_fields]
    similar = (
        work[comparable_columns].eq(work[comparable_columns].shift()).all(axis=1)
        if comparable_columns
        else pd.Series(False, index=work.index)
    )
    if not similar.empty:
        similar.iloc[0] = False
    examples = []
    for idx in work.index[similar.fillna(False)].tolist()[:20]:
        examples.append({"row": int(idx), "previous_row": int(idx) - 1})
    return {
        "exact_adjacent_repeats": int(exact.sum()),
        "similar_adjacent_repeats": int(similar.sum()),
        "examples": examples,
    }


def aggregate_target_profile(
    outputs: dict[str, pd.DataFrame],
    min_unique_players: int = DEFAULT_MIN_UNIQUE_PLAYERS,
    min_teams: int = DEFAULT_MIN_TEAMS,
) -> dict[str, Any]:
    players = outputs.get("players", pd.DataFrame())
    teams = outputs.get("teams", pd.DataFrame())
    unique_players = int(players["player_id"].nunique()) if "player_id" in players.columns else 0
    unique_teams = int(teams["team_id"].nunique()) if "team_id" in teams.columns else 0
    return {
        "minimum_unique_players_required": min_unique_players,
        "unique_players": unique_players,
        "passes_unique_players_requirement": unique_players >= min_unique_players,
        "minimum_teams_required": min_teams,
        "teams": unique_teams,
        "passes_teams_requirement": unique_teams >= min_teams,
    }


def write_quality_report(
    outputs: dict[str, pd.DataFrame],
    report_path: str | Path,
    *,
    null_threshold: float = DEFAULT_NULL_THRESHOLD,
    min_records: int = DEFAULT_MIN_RECORDS,
    min_records_by_dataset: dict[str, int] | None = None,
    min_unique_players: int = DEFAULT_MIN_UNIQUE_PLAYERS,
    min_teams: int = DEFAULT_MIN_TEAMS,
) -> dict[str, Any]:
    datasets = {}
    total_records = 0
    record_targets = min_records_by_dataset or MIN_RECORDS_BY_DATASET
    for name, df in outputs.items():
        profile = completeness_profile(df, null_threshold)
        anomalies = detect_numeric_anomalies(df, name)
        duplicates = duplicate_profile(df, name)
        adjacent_repeats = adjacent_repetition_profile(df, name)
        fallback_required_records = min_records if min_records != DEFAULT_MIN_RECORDS else 0
        required_records = int(record_targets.get(name, fallback_required_records))
        profile["anomalies_count"] = len(anomalies)
        profile["anomalies"] = anomalies[:100]
        profile["duplicate_profile"] = duplicates
        profile["adjacent_repetition_profile"] = adjacent_repeats
        profile["minimum_records_required"] = required_records
        profile["passes_record_requirement"] = int(len(df)) >= required_records
        profile["passes_quality_gate"] = (
            profile["passes_null_threshold"]
            and profile["passes_record_requirement"]
            and duplicates["duplicate_rows"] == 0
            and adjacent_repeats["exact_adjacent_repeats"] == 0
            and not anomalies
        )
        datasets[name] = profile
        total_records += int(len(df))

    aggregate_targets = aggregate_target_profile(outputs, min_unique_players, min_teams)
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": {
            "no_fabrication": True,
            "null_threshold_per_column": null_threshold,
            "minimum_records_required_default": min_records,
            "minimum_records_by_dataset": record_targets,
            "minimum_unique_players_required": min_unique_players,
            "minimum_teams_required": min_teams,
            "missing_values_above_threshold": "Flagged for additional source cross-reference, not automatically imputed.",
        },
        "total_records": total_records,
        "passes_minimum_record_requirement": all(
            dataset["passes_record_requirement"] for dataset in datasets.values()
        ),
        "aggregate_targets": aggregate_targets,
        "passes_aggregate_targets": (
            aggregate_targets["passes_unique_players_requirement"]
            and aggregate_targets["passes_teams_requirement"]
        ),
        "passes_all_quality_gates": (
            all(dataset["passes_quality_gate"] for dataset in datasets.values())
            and aggregate_targets["passes_unique_players_requirement"]
            and aggregate_targets["passes_teams_requirement"]
        ),
        "datasets": datasets,
    }

    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return report
