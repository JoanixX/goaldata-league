"""
Understat ingestion — REAL per-match player statistics (Big-5, 2014+)
=====================================================================

Understat (https://understat.com) publishes real, free, per-match player data
(goals, assists, shots, xG, xA, key passes, minutes) for the Big-5 European
leagues from season 2014-2015 onward. This is the real-source backbone that
replaces modelled per-match splits wherever it has coverage:

1. For each (league, season): POST `main/getPlayersStats/` -> the real player
   catalog of that league-season (season totals incl. xG).
2. For each unique player id: POST `main/getPlayerMatches/{id}` -> the player's
   full REAL match log across all his Understat seasons.

Outputs (data/raw/):
  understat_player_seasons.csv   one row per (player, league, season) — real totals
  understat_player_matches.csv   one row per (player, match) — real per-match stats
  understat_progress.json        resumable progress (player ids already fetched)

Polite crawl: 0.35 s delay, browser UA, gzip. Fully incremental / resumable.

Run:  python -m src.ingest_understat
"""
from __future__ import annotations

import gzip
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
SEASONS_OUT = RAW / "understat_player_seasons.csv"
MATCHES_OUT = RAW / "understat_player_matches.csv"
PROGRESS = RAW / "understat_progress.json"

LEAGUES = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"]
SEASONS = [str(y) for y in range(2014, 2026)]  # 2014-15 .. 2025-26
DELAY = 0.35

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
           "X-Requested-With": "XMLHttpRequest"}


def _read(resp) -> str:
    raw = resp.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="replace")


def post(url: str, form: dict | None = None, referer: str = "https://understat.com/",
         retries: int = 3) -> dict:
    data = urllib.parse.urlencode(form).encode() if form else b""
    req = urllib.request.Request(url, data=data, headers={
        **HEADERS, "Referer": referer,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
    for attempt in range(retries):
        try:
            return json.loads(_read(urllib.request.urlopen(req, timeout=60)))
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2.0 * (attempt + 1))  # backoff on throttling/transient errors


def fetch_league_seasons() -> pd.DataFrame:
    frames = []
    for league in LEAGUES:
        for season in SEASONS:
            try:
                data = post("https://understat.com/main/getPlayersStats/",
                            {"league": league, "season": season},
                            referer=f"https://understat.com/league/{league}/{season}")
                players = data.get("players") or data.get("response", {}).get("players", [])
                df = pd.DataFrame(players)
                df["league"], df["season_start"] = league, season
                frames.append(df)
                print(f"  {league} {season}: {len(df)} players", flush=True)
            except Exception as exc:
                print(f"  {league} {season}: FAILED ({type(exc).__name__})", flush=True)
            time.sleep(DELAY)
    return pd.concat(frames, ignore_index=True)


def fetch_player_matches(player_ids: list[str], names: dict[str, str]) -> None:
    done: set[str] = set()
    if PROGRESS.exists():
        done = set(json.loads(PROGRESS.read_text())["done"])
    rows_buffer: list[dict] = []
    n_flushed = 0

    def flush():
        nonlocal rows_buffer, n_flushed
        if not rows_buffer:
            return
        df = pd.DataFrame(rows_buffer)
        header = not MATCHES_OUT.exists()
        df.to_csv(MATCHES_OUT, mode="a", header=header, index=False, encoding="utf-8")
        n_flushed += len(rows_buffer)
        rows_buffer = []
        PROGRESS.write_text(json.dumps({"done": sorted(done)}))

    todo = [p for p in player_ids if p not in done]
    print(f"Fetching match logs for {len(todo):,} players ({len(done):,} already done)...", flush=True)

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from threading import Lock
    lock = Lock()

    def fetch_one(pid: str):
        time.sleep(DELAY)  # per-request politeness inside each worker
        data = post(f"https://understat.com/main/getPlayerMatches/{pid}",
                    referer=f"https://understat.com/player/{pid}")
        matches = data.get("response", {}).get("matches", [])
        for m in matches:
            m["understat_player_id"] = pid
            m["player_name"] = names.get(pid, "")
        return pid, matches

    i = 0
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fetch_one, pid): pid for pid in todo}
        for fut in as_completed(futures):
            i += 1
            try:
                pid, matches = fut.result()
                with lock:
                    rows_buffer.extend(matches)
                    done.add(pid)
            except Exception as exc:
                print(f"  player {futures[fut]}: FAILED ({type(exc).__name__})", flush=True)
            if i % 300 == 0:
                with lock:
                    flush()
                print(f"  {i:,}/{len(todo):,} players | {n_flushed:,} match rows written", flush=True)
    flush()
    print(f"Done: {n_flushed:,} new match rows -> {MATCHES_OUT}", flush=True)


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    if SEASONS_OUT.exists():
        seasons = pd.read_csv(SEASONS_OUT)
        print(f"Using cached league seasons ({len(seasons):,} rows)", flush=True)
    else:
        seasons = fetch_league_seasons()
        seasons.to_csv(SEASONS_OUT, index=False, encoding="utf-8")
        print(f"Saved {len(seasons):,} player-league-season rows -> {SEASONS_OUT}", flush=True)

    ids = seasons["id"].astype(str)
    names = dict(zip(ids, seasons["player_name"].astype(str)))
    fetch_player_matches(sorted(set(ids)), names)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_understat", main)
