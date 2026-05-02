from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


MISSING_MARKERS = {"", "NULL", "NAN", "NONE", "NA"}
DEFAULT_NULL_THRESHOLD = 0.0025
DEFAULT_MIN_RECORDS = 1_500_000


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
        return pd.to_numeric(df[column].replace("NULL", pd.NA), errors="coerce")

    if dataset_name == "matches":
        home_possession = numeric("possession_home")
        away_possession = numeric("possession_away")
        both = home_possession.notna() & away_possession.notna()
        bad_possession = both & ((home_possession + away_possession - 1.0).abs() > 0.02)
        for idx in df.index[bad_possession]:
            anomalies.append(
                {
                    "row": int(idx),
                    "type": "possession_sum",
                    "message": "Home and away possession do not sum to approximately 1.0.",
                }
            )

    if dataset_name == "match_stats":
        attempted = numeric("passes_attempted")
        completed = numeric("passes_completed")
        bad_passes = attempted.notna() & completed.notna() & (completed > attempted)
        for idx in df.index[bad_passes]:
            anomalies.append(
                {
                    "row": int(idx),
                    "type": "passes_completed_gt_attempted",
                    "message": "passes_completed exceeds passes_attempted.",
                }
            )

        shots = numeric("shots")
        shots_on_target = numeric("shots_on_target")
        bad_shots = shots.notna() & shots_on_target.notna() & (shots_on_target > shots)
        for idx in df.index[bad_shots]:
            anomalies.append(
                {
                    "row": int(idx),
                    "type": "shots_on_target_gt_shots",
                    "message": "shots_on_target exceeds shots.",
                }
            )

        pass_accuracy = numeric("pass_accuracy")
        bad_accuracy = pass_accuracy.notna() & ((pass_accuracy < 0) | (pass_accuracy > 1))
        for idx in df.index[bad_accuracy]:
            anomalies.append(
                {
                    "row": int(idx),
                    "type": "pass_accuracy_out_of_range",
                    "message": "pass_accuracy must be between 0 and 1.",
                }
            )

    return anomalies


def write_quality_report(
    outputs: dict[str, pd.DataFrame],
    report_path: str | Path,
    *,
    null_threshold: float = DEFAULT_NULL_THRESHOLD,
    min_records: int = DEFAULT_MIN_RECORDS,
) -> dict[str, Any]:
    datasets = {}
    total_records = 0
    for name, df in outputs.items():
        profile = completeness_profile(df, null_threshold)
        anomalies = detect_numeric_anomalies(df, name)
        profile["anomalies_count"] = len(anomalies)
        profile["anomalies"] = anomalies[:100]
        profile["passes_quality_gate"] = profile["passes_null_threshold"] and not anomalies
        datasets[name] = profile
        total_records += int(len(df))

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": {
            "no_fabrication": True,
            "null_threshold_per_column": null_threshold,
            "minimum_records_required": min_records,
            "missing_values_above_threshold": "Flagged for additional source cross-reference, not automatically imputed.",
        },
        "total_records": total_records,
        "passes_minimum_record_requirement": total_records >= min_records,
        "passes_all_quality_gates": total_records >= min_records
        and all(dataset["passes_quality_gate"] for dataset in datasets.values()),
        "datasets": datasets,
    }

    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return report
