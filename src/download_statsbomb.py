from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import requests


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw" / "statsbomb"
LOGS_DIR = BASE_DIR / "logs"
RAW_BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
TARGET_COMPETITIONS = {
    "1. Bundesliga": "Bundesliga",
    "Champions League": "UEFA Champions League",
    "FIFA World Cup": "FIFA World Cup",
    "UEFA Euro": "UEFA Eurocup",
    "Copa America": "CONMEBOL Copa America",
    "La Liga": "La Liga",
    "Ligue 1": "Ligue 1",
    "Premier League": "Premier League",
    "Serie A": "Serie A",
}


def get_json(session: requests.Session, url: str):
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def season_start(season_name: str) -> int | None:
    for token in season_name.replace("-", "/").split("/"):
        if token.isdigit() and len(token) == 4:
            return int(token)
    return None


def download_all() -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": "goaldata-league selective statsbomb ingester"})
    results = []
    competitions = get_json(session, f"{RAW_BASE}/competitions.json")
    write_json(RAW_DIR / "competitions.json", competitions)

    selected = []
    for row in competitions:
        if row.get("competition_name") not in TARGET_COMPETITIONS:
            continue
        start = season_start(str(row.get("season_name", "")))
        if start is not None and (start < 2004 or start > 2025):
            continue
        selected.append(row)

    for comp in selected:
        cid = comp["competition_id"]
        sid = comp["season_id"]
        comp_dir = RAW_DIR / "data" / str(cid) / str(sid)
        item = {
            "competition_id": cid,
            "season_id": sid,
            "competition_name": comp.get("competition_name"),
            "season_name": comp.get("season_name"),
        }
        try:
            matches_path = comp_dir / "matches.json"
            if matches_path.exists() and matches_path.stat().st_size > 0:
                matches = json.loads(matches_path.read_text(encoding="utf-8"))
            else:
                matches = get_json(session, f"{RAW_BASE}/matches/{cid}/{sid}.json")
                write_json(matches_path, matches)
            event_count = 0
            skipped_events = 0
            for match in matches:
                mid = match["match_id"]
                event_path = comp_dir / "events" / f"{mid}.json"
                if event_path.exists() and event_path.stat().st_size > 0:
                    events = json.loads(event_path.read_text(encoding="utf-8"))
                    skipped_events += 1
                else:
                    events = get_json(session, f"{RAW_BASE}/events/{mid}.json")
                    write_json(event_path, events)
                event_count += len(events)
            item["downloaded"] = True
            item["matches"] = len(matches)
            item["events"] = event_count
            item["already_present_events"] = skipped_events
        except requests.RequestException as exc:
            item["downloaded"] = False
            item["reason"] = str(exc)
        results.append(item)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "StatsBomb Open Data selective raw GitHub download",
        "selected_competitions": selected,
        "results": results,
        "downloaded_count": sum(1 for item in results if item.get("downloaded")),
        "match_count": sum(item.get("matches", 0) for item in results),
        "event_count": sum(item.get("events", 0) for item in results),
    }
    out = LOGS_DIR / "statsbomb_download_report.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def main() -> None:
    report = download_all()
    print(json.dumps({"downloaded_count": report["downloaded_count"], "matches": report["match_count"], "events": report["event_count"], "log": str(LOGS_DIR / "statsbomb_download_report.json")}, indent=2))


if __name__ == "__main__":
    main()
