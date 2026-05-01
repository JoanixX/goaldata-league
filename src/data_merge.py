import json
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


MISSING_MARKERS = {"", "NULL", "NAN", "NONE", "NA"}


def is_missing(value) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip().upper() in MISSING_MARKERS


def match_key(row: dict) -> tuple[str, str, str, str]:
    return (
        str(row.get("season", "")).strip(),
        str(row.get("fecha", row.get("date", ""))).strip(),
        str(row.get("local", row.get("home_team", ""))).strip().casefold(),
        str(row.get("visitante", row.get("away_team", ""))).strip().casefold(),
    )


def merge_results_into_completed(
    completed_csv: str | Path,
    scraper_json: str | Path,
    output_csv: str | Path | None = None,
    conflicts_json: str | Path | None = None,
) -> dict:
    """Merge scraper JSON rows into cl_2010_2025_completed.csv.

    Only empty target cells are filled. Existing non-empty cells are never
    overwritten; different incoming values are written to a JSON conflict log.
    """
    completed_csv = Path(completed_csv)
    scraper_json = Path(scraper_json)
    output_csv = Path(output_csv) if output_csv else completed_csv
    conflicts_json = Path(conflicts_json) if conflicts_json else completed_csv.parent / "cl_2010_2025_completed_conflicts.json"

    df = pd.read_csv(completed_csv, keep_default_na=False)
    with scraper_json.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload.get("matches", payload if isinstance(payload, list) else [])
    index = {match_key(row): idx for idx, row in df.iterrows()}
    updates = []
    conflicts = []
    not_found = []

    for row in rows:
        key = match_key(row)
        if key not in index:
            not_found.append({"key": key, "row": row})
            continue
        idx = index[key]
        for column, new_value in row.items():
            if column not in df.columns or column in {"season", "fecha", "local", "visitante", "date", "home_team", "away_team"}:
                continue
            current = df.at[idx, column]
            if is_missing(current):
                if not is_missing(new_value):
                    df.at[idx, column] = new_value
                    updates.append({"row": int(idx), "column": column, "value": new_value})
            elif not is_missing(new_value) and str(current).strip() != str(new_value).strip():
                conflicts.append(
                    {
                        "row": int(idx),
                        "key": key,
                        "column": column,
                        "current_value": current,
                        "incoming_value": new_value,
                    }
                )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8")

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_json": str(scraper_json),
        "target_csv": str(output_csv),
        "updates_count": len(updates),
        "conflicts_count": len(conflicts),
        "not_found_count": len(not_found),
        "updates": updates,
        "conflicts": conflicts,
        "not_found": not_found,
    }
    conflicts_json.parent.mkdir(parents=True, exist_ok=True)
    with conflicts_json.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return report


def main():
    parser = argparse.ArgumentParser(description="Merge scraper JSON results into cl_2010_2025_completed.csv without overwriting existing values.")
    parser.add_argument("scraper_json", help="JSON file produced by a scraper or diagnostic run.")
    parser.add_argument("--completed-csv", default="data/raw/cl_2010_2025_completed.csv")
    parser.add_argument("--output-csv", default="data/raw/cl_2010_2025_completed.csv")
    parser.add_argument("--conflicts-json", default="logs/cl_2010_2025_completed_conflicts.json")
    args = parser.parse_args()

    report = merge_results_into_completed(
        args.completed_csv,
        args.scraper_json,
        args.output_csv,
        args.conflicts_json,
    )
    print(json.dumps({k: report[k] for k in ["updates_count", "conflicts_count", "not_found_count"]}, indent=2))


if __name__ == "__main__":
    main()
