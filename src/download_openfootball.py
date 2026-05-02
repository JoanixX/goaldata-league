from __future__ import annotations

import json
import zipfile
from datetime import datetime
from pathlib import Path

import requests


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw" / "openfootball"
LOGS_DIR = BASE_DIR / "logs"

REPOSITORIES = {
    "champions-league": "https://github.com/openfootball/champions-league/archive/refs/heads/master.zip",
    "south-america": "https://github.com/openfootball/south-america/archive/refs/heads/master.zip",
    "worldcup": "https://github.com/openfootball/worldcup/archive/refs/heads/master.zip",
    "euro": "https://github.com/openfootball/euro/archive/refs/heads/master.zip",
    "copa-america": "https://github.com/openfootball/copa-america/archive/refs/heads/master.zip",
}


def download_all() -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": "goaldata-league academic openfootball ingester"})
    results = []
    for name, url in REPOSITORIES.items():
        zip_path = RAW_DIR / f"{name}.zip"
        extract_dir = RAW_DIR / name
        item = {"name": name, "url": url, "zip_path": str(zip_path), "extract_dir": str(extract_dir)}
        try:
            response = session.get(url, timeout=60)
            item["status_code"] = response.status_code
            response.raise_for_status()
            zip_path.write_bytes(response.content)
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path) as archive:
                archive.extractall(extract_dir)
            item["downloaded"] = True
            item["bytes"] = len(response.content)
            item["txt_files"] = len(list(extract_dir.rglob("*.txt")))
        except (requests.RequestException, zipfile.BadZipFile, OSError) as exc:
            item["downloaded"] = False
            item["reason"] = str(exc)
        results.append(item)
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "openfootball GitHub public-domain repositories",
        "repositories": REPOSITORIES,
        "results": results,
        "attempted_count": len(results),
        "downloaded_count": sum(1 for item in results if item.get("downloaded")),
        "failed_count": sum(1 for item in results if not item.get("downloaded")),
    }
    out = LOGS_DIR / "openfootball_download_report.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def main() -> None:
    report = download_all()
    print(json.dumps({"downloaded_count": report["downloaded_count"], "log": str(LOGS_DIR / "openfootball_download_report.json")}, indent=2))


if __name__ == "__main__":
    main()
