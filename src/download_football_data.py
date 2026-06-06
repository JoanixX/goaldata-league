from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import requests


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw" / "football-data"
LOGS_DIR = BASE_DIR / "logs"

LEAGUES = {
    "B1": {"competition": "Belgian Pro League", "country": "Belgium"},
    "E0": {"competition": "Premier League", "country": "England"},
    "SP1": {"competition": "La Liga", "country": "Spain"},
    "D1": {"competition": "Bundesliga", "country": "Germany"},
    "I1": {"competition": "Serie A", "country": "Italy"},
    "F1": {"competition": "Ligue 1", "country": "France"},
    "G1": {"competition": "Super League Greece", "country": "Greece"},
    "N1": {"competition": "Eredivisie", "country": "Netherlands"},
    "P1": {"competition": "Primeira Liga", "country": "Portugal"},
    "SC0": {"competition": "Scottish Premiership", "country": "Scotland"},
    "T1": {"competition": "Super Lig", "country": "Turkey"},
}
SEASONS = list(range(2004, 2026))
BASE_URL = "https://www.football-data.co.uk/mmz4281/{season_code}/{league_code}.csv"


def season_code(start_year: int) -> str:
    return f"{start_year % 100:02d}{(start_year + 1) % 100:02d}"


def download_all() -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)
    results = []
    session = requests.Session()
    session.headers.update({"User-Agent": "goaldata-league academic data pipeline"})
    for year in SEASONS:
        code = season_code(year)
        season_dir = RAW_DIR / code
        season_dir.mkdir(parents=True, exist_ok=True)
        for league_code, meta in LEAGUES.items():
            url = BASE_URL.format(season_code=code, league_code=league_code)
            out = season_dir / f"{league_code}.csv"
            item = {"season": f"{year}-{year + 1}", "league_code": league_code, "url": url, "path": str(out)}
            try:
                response = session.get(url, timeout=30)
                item["status_code"] = response.status_code
                content_type = response.headers.get("content-type", "")
                text = response.content.decode("utf-8-sig", errors="replace").lstrip("\r\n ")
                if response.status_code == 200 and "html" not in content_type.lower() and text.startswith("Div,"):
                    out.write_text(text, encoding="utf-8")
                    item["downloaded"] = True
                    item["bytes"] = len(response.content)
                else:
                    item["downloaded"] = False
                    item["reason"] = f"Unexpected response content_type={content_type!r}"
            except requests.RequestException as exc:
                item["downloaded"] = False
                item["reason"] = str(exc)
            results.append(item)
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "football-data.co.uk",
        "season_start": "2004-2005",
        "season_end": "2025-2026",
        "leagues": LEAGUES,
        "results": results,
        "attempted_count": len(results),
        "downloaded_count": sum(1 for item in results if item.get("downloaded")),
        "failed_count": sum(1 for item in results if not item.get("downloaded")),
    }
    with (LOGS_DIR / "football_data_download_report.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return report


def main():
    report = download_all()
    print(json.dumps({"downloaded_count": report["downloaded_count"], "log": str(LOGS_DIR / "football_data_download_report.json")}, indent=2))


if __name__ == "__main__":
    main()
