"""
Real StatsBomb events WITH pitch location (for zone/direction style features)
=============================================================================

Re-ingests the real event stream keeping the **pitch coordinates** and pass/shot
detail that the plain event table dropped. These enable rich, 100%-real style
features (pitch zones, pass direction/length, progressive actions, shot location)
that make players far more distinctive -> higher recommender and position metrics.

Still hang-proof (HTTP + timeout + retries + budget + resumable checkpoints) and
capped at <= 3M events. Writes the >=1.5M real event dataset (richer schema).

Output: data/processed/events/statsbomb_events_real.parquet
Run:    python -m src.ingest_statsbomb_events_rich --target-events 1750000 --budget-min 35
"""
from __future__ import annotations

import argparse
import json
import math
import time
import urllib.request
from pathlib import Path

import pandas as pd

BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "processed" / "events" / "statsbomb_events_real.parquet"
PM_OUT = ROOT / "data" / "raw" / "statsbomb_player_match_real.csv"
PROGRESS = ROOT / "data" / "raw" / "statsbomb_rich_progress.json"


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


def parse(ev: list[dict], meta: dict):
    events, parts = [], {}
    for e in ev:
        pl = e.get("player")
        if not pl:
            continue
        pid = pl["id"]
        t = e["type"]["name"]
        loc = e.get("location") or [None, None]
        pas = e.get("pass") or {}
        sh = e.get("shot") or {}
        carry = e.get("carry") or {}
        end = pas.get("end_location") or sh.get("end_location") or carry.get("end_location") or [None, None]
        events.append({
            "match_id": meta["match_id"], "competition": meta["competition"], "season": meta["season"],
            "team": e.get("team", {}).get("name"), "player_id": pid, "player_name": pl.get("name"),
            "position": e.get("position", {}).get("name"), "minute": e.get("minute"),
            "event_type": t,
            "x": loc[0] if loc else None, "y": loc[1] if loc else None,
            "end_x": end[0] if end else None, "end_y": end[1] if end else None,
            "pass_length": pas.get("length"), "pass_angle": pas.get("angle"),
            "xg": sh.get("statsbomb_xg") if t == "Shot" else None,
        })
        a = parts.setdefault(pid, {"player_name": pl.get("name"), "team": e.get("team", {}).get("name"),
                                   "goals": 0, "assists": 0, "shots": 0, "shots_on_target": 0, "xg": 0.0,
                                   "passes_completed": 0, "passes_attempted": 0, "tackles": 0,
                                   "interceptions": 0, "fouls_committed": 0, "yellow_cards": 0,
                                   "red_cards": 0, "position_group": "", "_pos": {}})
        pos = e.get("position", {}).get("name")
        if pos:
            a["_pos"][pos] = a["_pos"].get(pos, 0) + 1
        if t == "Pass":
            a["passes_attempted"] += 1
            if "outcome" not in pas:
                a["passes_completed"] += 1
            if pas.get("goal_assist"):
                a["assists"] += 1
        elif t == "Shot":
            a["shots"] += 1
            a["xg"] += sh.get("statsbomb_xg", 0.0) or 0.0
            oc = sh.get("outcome", {}).get("name")
            a["goals"] += 1 if oc == "Goal" else 0
            a["shots_on_target"] += 1 if oc in {"Goal", "Saved", "Saved to Post"} else 0
        elif t == "Interception":
            a["interceptions"] += 1
        elif t == "Foul Committed":
            a["fouls_committed"] += 1
            card = e.get("foul_committed", {}).get("card", {}).get("name", "")
            a["yellow_cards"] += 1 if "Yellow" in card else 0
            a["red_cards"] += 1 if "Red" in card else 0
        elif t == "Duel" and e.get("duel", {}).get("type", {}).get("name") == "Tackle":
            a["tackles"] += 1
    prows = []
    for pid, a in parts.items():
        pos = max(a["_pos"].items(), key=lambda kv: kv[1])[0] if a["_pos"] else ""
        a["position_group"] = sb_pos_group(pos)
        a.pop("_pos")
        prows.append({"match_id": meta["match_id"], "competition": meta["competition"],
                      "season": meta["season"], "player_id": pid, **a})
    return events, prows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-events", type=int, default=1_750_000)
    ap.add_argument("--budget-min", type=float, default=35.0)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--max-events", type=int, default=3_000_000)
    args = ap.parse_args()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    done = set(json.loads(PROGRESS.read_text())) if PROGRESS.exists() else set()
    ev_rows = pd.read_parquet(OUT).to_dict("records") if (OUT.exists() and "x" in pd.read_parquet(OUT, columns=None).columns) else []
    pm_rows = pd.read_csv(PM_OUT).to_dict("records") if (PM_OUT.exists() and done) else []
    print(f"Resume: {len(ev_rows):,} rich events, {len(pm_rows):,} participations, {len(done)} matches.", flush=True)
    matches = [m for m in list_matches() if m["match_id"] not in done]
    print(f"{len(matches)} matches to fetch (rich events).", flush=True)
    start, n = time.time(), 0
    for m in matches:
        if len(ev_rows) >= min(args.target_events, args.max_events):
            print(f"Target events reached: {len(ev_rows):,}.", flush=True)
            break
        if (time.time() - start) / 60 > args.budget_min:
            print(f"Budget reached at {len(ev_rows):,} events.", flush=True)
            break
        try:
            ev = get(f"{BASE}/events/{m['match_id']}.json", timeout=args.timeout)
        except Exception as exc:
            print(f"  skip {m['match_id']} ({type(exc).__name__})", flush=True)
            done.add(m["match_id"]); continue
        e_rows, p_rows = parse(ev, m)
        ev_rows.extend(e_rows); pm_rows.extend(p_rows)
        done.add(m["match_id"]); n += 1
        if n % 50 == 0:
            pd.DataFrame(ev_rows).to_parquet(OUT, index=False)
            pd.DataFrame(pm_rows).to_csv(PM_OUT, index=False, encoding="utf-8")
            PROGRESS.write_text(json.dumps(sorted(done)))
            print(f"  {n} matches; {len(ev_rows):,} rich events; {int(time.time()-start)}s", flush=True)
    pd.DataFrame(ev_rows).to_parquet(OUT, index=False)
    pd.DataFrame(pm_rows).to_csv(PM_OUT, index=False, encoding="utf-8")
    PROGRESS.write_text(json.dumps(sorted(done)))
    print(f"DONE: {len(ev_rows):,} rich REAL events, {len(pm_rows):,} participations, {len(done)} matches.", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_statsbomb_events_rich", main)
