import json
from pathlib import Path

import pandas as pd

from src.data_merge import merge_results_into_completed


def test_merge_results_fills_only_empty_cells_and_logs_conflicts(tmp_path: Path):
    completed = tmp_path / "cl_2010_2025_completed.csv"
    output = tmp_path / "out.csv"
    conflicts = tmp_path / "conflicts.json"
    results = tmp_path / "scraper.json"

    pd.DataFrame(
        [
            {
                "season": "2010-2011",
                "fecha": "15-02-2011",
                "local": "Valencia",
                "visitante": "Schalke 04",
                "estadio": "Mestalla",
                "asistencias": "NULL",
            }
        ]
    ).to_csv(completed, index=False)

    results.write_text(
        json.dumps(
            {
                "matches": [
                    {
                        "season": "2010-2011",
                        "fecha": "15-02-2011",
                        "local": "Valencia",
                        "visitante": "Schalke 04",
                        "estadio": "Different Stadium",
                        "asistencias": "Player 10'",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    report = merge_results_into_completed(completed, results, output, conflicts)
    merged = pd.read_csv(output, keep_default_na=False)

    assert merged.loc[0, "asistencias"] == "Player 10'"
    assert merged.loc[0, "estadio"] == "Mestalla"
    assert report["updates_count"] == 1
    assert report["conflicts_count"] == 1


def test_merge_results_writes_not_found_to_json(tmp_path: Path):
    completed = tmp_path / "cl_2010_2025_completed.csv"
    results = tmp_path / "scraper.json"

    pd.DataFrame(
        [{"season": "2010-2011", "fecha": "15-02-2011", "local": "A", "visitante": "B", "estadio": "NULL"}]
    ).to_csv(completed, index=False)
    results.write_text(
        json.dumps({"matches": [{"season": "2011-2012", "fecha": "10-03-2012", "local": "C", "visitante": "D", "estadio": "X"}]}),
        encoding="utf-8",
    )

    report = merge_results_into_completed(completed, results)

    assert report["not_found_count"] == 1
