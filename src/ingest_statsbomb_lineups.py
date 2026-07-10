"""
Real StatsBomb lineups -> complete real participations (who played, which match)
================================================================================

Events only capture players who touched the ball; the real **lineup** sheet
(``lineups/{match_id}.json``) lists EVERY player who was named, with real
positions, real minutes (from the position from/to timestamps) and cards. This
builds the authoritative "who played in which match" participation table — 100%
real, nothing simulated. Hang-proof HTTP (timeout + retries + budget + resume).

Output: data/raw/statsbomb_lineups_real.csv  (player_id, player_name, match_id,
         competition, season, team, position_group, minutes, started, yellow_cards,
         red_cards, jersey_number)

Run:  python -m src.ingest_statsbomb_lineups --budget-min 15
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request
from pathlib import Path

import pandas as pd

BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "raw" / "statsbomb_lineups_real.csv"
PROGRESS = ROOT / "data" / "raw" / "statsbomb_lineups_progress.json"


def get(url: str, timeout: int = 20, retries: int = 2):
    last = None
    for a in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as exc:
            last = exc
            time.sleep(1.5 * (a + 1))
    raise last


def sb_pos_group(name: str) -> str:
    n = (name or "").lower()
    if "goalkeeper" in n:
        return "GK"
    if "back" in n:
        return "DEF"
    if "midfield" in n:
        return "MID"
    if "wing" in n or "forward" in n or "striker" in n:
        return "FW"
    return "MID"


def _mm(t: str | None) -> float | None:
    if not t:
        return None
    try:
        m, s = t.split(":")
        return int(m) + int(s) / 60.0
    except Exception:
        return None


def list_matches() -> list[dict]:
    comps = get(f"{BASE}/competitions.json")
    out = []
    for c in comps:
        try:
            ms = get(f"{BASE}/matches/{c['competition_id']}/{c['season_id']}.json")
        except Exception:
            continue
        for m in ms:
            out.append({"match_id": m["match_id"], "competition": c["competition_name"],
                        "season": str(c["season_name"]).replace("/", "-")})
    return out


def parse_lineup(lu: list[dict], meta: dict) -> list[dict]:
    rows = []
    for team in lu:
        tname = team.get("team_name")
        for p in team.get("lineup", []):
            positions = p.get("positions", []) or []
            # real minutes = last 'to' (or 95 if played to end) - first 'from'
            froms = [_mm(pos.get("from")) for pos in positions if _mm(pos.get("from")) is not None]
            tos = [(_mm(pos.get("to")) if pos.get("to") else 95.0) for pos in positions]
            minutes = (max(tos) - min(froms)) if froms and tos else 0.0
            started = bool(positions) and any(pos.get("start_reason") == "Starting XI" for pos in positions)
            cards = p.get("cards", []) or []
            pos_name = positions[0]["position"] if positions else ""
            rows.append({
                "player_id": "sb_" + str(p.get("player_id")),
                "player_name": p.get("player_nickname") or p.get("player_name"),
                "match_id": meta["match_id"], "competition": meta["competition"],
                "season": meta["season"], "team": tname,
                "position_group": sb_pos_group(pos_name),
                "minutes": round(max(0.0, minutes), 1),
                "started": int(started),
                "yellow_cards": sum(1 for c in cards if "Yellow" in c.get("card_type", "")),
                "red_cards": sum(1 for c in cards if "Red" in c.get("card_type", "")),
                "jersey_number": p.get("jersey_number"),
            })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget-min", type=float, default=15.0)
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    done = set(json.loads(PROGRESS.read_text())) if PROGRESS.exists() else set()
    rows = pd.read_csv(OUT).to_dict("records") if OUT.exists() else []
    matches = [m for m in list_matches() if m["match_id"] not in done]
    print(f"{len(matches)} matches with real lineups to fetch (resume={len(rows):,} rows).", flush=True)
    start, n = time.time(), 0
    for m in matches:
        if (time.time() - start) / 60 > args.budget_min:
            print(f"Budget reached at {len(rows):,} participations.", flush=True)
            break
        try:
            lu = get(f"{BASE}/lineups/{m['match_id']}.json", timeout=args.timeout)
        except Exception as exc:
            print(f"  skip {m['match_id']} ({type(exc).__name__})", flush=True)
            done.add(m["match_id"]); continue
        rows.extend(parse_lineup(lu, m))
        done.add(m["match_id"]); n += 1
        if n % 200 == 0:
            pd.DataFrame(rows).to_csv(OUT, index=False, encoding="utf-8")
            PROGRESS.write_text(json.dumps(sorted(done)))
            print(f"  {n} matches; {len(rows):,} real participations; {int(time.time()-start)}s", flush=True)
    df = pd.DataFrame(rows).drop_duplicates(["player_id", "match_id"]).reset_index(drop=True)
    df.to_csv(OUT, index=False, encoding="utf-8")
    PROGRESS.write_text(json.dumps(sorted(done)))
    print(f"Saved {len(df):,} REAL participations (lineups) from {len(done)} matches "
          f"({df['player_id'].nunique():,} players, {df['competition'].nunique()} competitions).", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_statsbomb_lineups", main)
