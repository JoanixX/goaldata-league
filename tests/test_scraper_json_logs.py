import json
from pathlib import Path


SCRAPER_RESULT_DIRS = [
    "espn",
    "fbref",
    "flashscore",
    "transfermarkt",
    "uefa",
    "worldfootball",
]


def test_each_scraper_has_json_result_logs():
    base = Path("tests/api_diagnostics/results")

    for scraper in SCRAPER_RESULT_DIRS:
        scraper_dir = base / scraper
        assert scraper_dir.exists(), f"Missing JSON log directory for {scraper}"
        json_files = sorted(scraper_dir.glob("*.json"))
        assert json_files, f"Missing JSON result logs for {scraper}"
        for path in json_files:
            with path.open("r", encoding="utf-8") as f:
                json.load(f)
