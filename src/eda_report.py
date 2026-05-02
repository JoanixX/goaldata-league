from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.data_quality import completeness_profile


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "reports"

DATASETS = {
    "matches": PROCESSED_DIR / "core" / "matches_cleaned.parquet",
    "players": PROCESSED_DIR / "core" / "players_cleaned.parquet",
    "teams": PROCESSED_DIR / "core" / "teams_cleaned.parquet",
    "goals": PROCESSED_DIR / "events" / "goals_events_cleaned.parquet",
    "gk": PROCESSED_DIR / "stats" / "goalkeeper_stats_cleaned.parquet",
    "match_stats": PROCESSED_DIR / "stats" / "player_match_stats_cleaned.parquet",
    "season_stats": PROCESSED_DIR / "stats" / "player_season_stats_cleaned.parquet",
}


def numeric_summary(df: pd.DataFrame) -> dict:
    summary = {}
    for column in df.columns:
        values = pd.to_numeric(df[column], errors="coerce")
        if values.notna().sum() == 0:
            continue
        summary[column] = {
            "count": int(values.notna().sum()),
            "mean": round(float(values.mean()), 4),
            "median": round(float(values.median()), 4),
            "min": round(float(values.min()), 4),
            "max": round(float(values.max()), 4),
        }
    return summary


def build_eda_report() -> dict:
    datasets = {}
    for name, path in DATASETS.items():
        if not path.exists():
            datasets[name] = {"missing_file": str(path)}
            continue
        df = pd.read_parquet(path)
        profile = completeness_profile(df)
        datasets[name] = {
            "path": str(path),
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "failing_columns": profile["failing_columns"],
            "numeric_summary": numeric_summary(df),
        }
    return {"generated_at": datetime.now().isoformat(timespec="seconds"), "datasets": datasets}


def write_markdown(report: dict, path: Path) -> None:
    lines = ["# EDA Summary", "", f"Generated at: `{report['generated_at']}`", ""]
    for name, dataset in report["datasets"].items():
        lines.append(f"## {name}")
        if "missing_file" in dataset:
            lines.append(f"- Missing file: `{dataset['missing_file']}`")
            lines.append("")
            continue
        lines.append(f"- Rows: {dataset['rows']}")
        lines.append(f"- Columns: {dataset['columns']}")
        lines.append(f"- Failing quality columns: {', '.join(dataset['failing_columns']) if dataset['failing_columns'] else 'none'}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    REPORTS_DIR.mkdir(exist_ok=True)
    report = build_eda_report()
    json_path = REPORTS_DIR / "eda_summary.json"
    md_path = REPORTS_DIR / "eda_summary.md"
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    write_markdown(report, md_path)
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, indent=2))


if __name__ == "__main__":
    main()
