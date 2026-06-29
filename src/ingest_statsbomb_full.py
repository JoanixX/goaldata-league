"""
Real StatsBomb layer: participations + event stream (multi-competition, hang-proof)
===================================================================================

Builds TWO real datasets from StatsBomb Open Data's 24 real competitions
(Champions League, La Liga, World Cup, Euro, Copa America, Europa League, Premier,
Serie A, Ligue 1, Bundesliga, Copa del Rey, ... — 2005 onward):

1. ``data/processed/events/statsbomb_events_real.parquet`` — one row per REAL
   event (pass/shot/tackle/...). Millions of rows -> this is the >=1.5M dataset,
   100% real (real player, real team, real action). Nothing simulated.
2. ``data/raw/statsbomb_player_match_real.csv`` — one row per REAL participation
   (player x match) with real goals, assists, shots, passes, tackles,
   interceptions, fouls, cards, minutes, position. Replaces the invented
   "{team} {season} Squad NN" rows.

Hang-proof: HTTPS JSON, hard per-request timeout + retries, match cap, wall-clock
budget, resumable checkpoints. Caps total events at <=3M.

Run:  python -m src.ingest_statsbomb_full --target-events 1700000 --budget-min 35
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

import pandas as pd

BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
ROOT = Path(__file__).resolve().parents[1]
EVENTS_OUT = ROOT / "data" / "processed" / "events" / "statsbomb_events_real.parquet"
PM_OUT = ROOT / "data" / "raw" / "statsbomb_player_match_real.csv"
PROGRESS = ROOT / "data" / "raw" / "statsbomb_full_progress.json"


def get(url: str, timeout: int = 20, retries: int = 2):
    last = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as exc:
            last = exc
            time.sleep(1.5 * (attempt + 1))
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


def parse_match(ev: list[dict], meta: dict):
    """Return (event_rows, participation_rows) — both REAL, no simulation."""
    events = []
    pm = defaultdict(lambda: defaultdict(float))
    info = {}
    pos_count = defaultdict(lambda: defaultdict(int))
    mn_lo, mn_hi = {}, {}
    for e in ev:
        pl = e.get("player")
        if not pl:
            continue
        pid = pl["id"]
        t = e["type"]["name"]
        team = e.get("team", {}).get("name")
        pos = e.get("position", {}).get("name")
        minute = e.get("minute", 0)
        xg = e.get("shot", {}).get("statsbomb_xg") if t == "Shot" else None
        events.append({"match_id": meta["match_id"], "competition": meta["competition"],
                       "season": meta["season"], "team": team, "player_id": pid,
                       "player_name": pl.get("name"), "position": pos, "minute": minute,
                       "event_type": t, "xg": xg})
        info.setdefault(pid, {"player_name": pl.get("name"), "team": team})
        if pos:
            pos_count[pid][pos] += 1
        mn_lo[pid] = min(mn_lo.get(pid, 999), minute)
        mn_hi[pid] = max(mn_hi.get(pid, -1), minute)
        a = pm[pid]
        if t == "Pass":
            a["passes_attempted"] += 1
            if "outcome" not in e.get("pass", {}):
                a["passes_completed"] += 1
            if e.get("pass", {}).get("goal_assist"):
                a["assists"] += 1
        elif t == "Shot":
            sh = e.get("shot", {})
            a["shots"] += 1
            a["xg"] += sh.get("statsbomb_xg", 0.0) or 0.0
            oc = sh.get("outcome", {}).get("name")
            if oc == "Goal":
                a["goals"] += 1
            if oc in {"Goal", "Saved", "Saved to Post"}:
                a["shots_on_target"] += 1
        elif t == "Interception":
            a["interceptions"] += 1
        elif t == "Foul Committed":
            a["fouls_committed"] += 1
            card = e.get("foul_committed", {}).get("card", {}).get("name", "")
            a["yellow_cards"] += 1 if "Yellow" in card else 0
            a["red_cards"] += 1 if "Red" in card else 0
        elif t == "Bad Behaviour":
            card = e.get("bad_behaviour", {}).get("card", {}).get("name", "")
            a["yellow_cards"] += 1 if "Yellow" in card else 0
            a["red_cards"] += 1 if "Red" in card else 0
        elif t == "Duel" and e.get("duel", {}).get("type", {}).get("name") == "Tackle":
            a["tackles"] += 1

    parts = []
    for pid, a in pm.items():
        rec = {"match_id": meta["match_id"], "competition": meta["competition"],
               "season": meta["season"], "player_id": pid, **info[pid], **a}
        rec["minutes"] = float(min(max(0.0, mn_hi.get(pid, 0) - mn_lo.get(pid, 0)) + 1, 95.0))
        rec["position_group"] = sb_pos_group(max(pos_count[pid].items(), key=lambda kv: kv[1])[0]
                                             if pos_count[pid] else "")
        parts.append(rec)
    return events, parts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-events", type=int, default=1_700_000)
    ap.add_argument("--budget-min", type=float, default=35.0)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--max-events", type=int, default=3_000_000)
    args = ap.parse_args()

    EVENTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    PM_OUT.parent.mkdir(parents=True, exist_ok=True)
    done = set(json.loads(PROGRESS.read_text())) if PROGRESS.exists() else set()
    ev_rows = pd.read_parquet(EVENTS_OUT).to_dict("records") if EVENTS_OUT.exists() else []
    pm_rows = pd.read_csv(PM_OUT).to_dict("records") if PM_OUT.exists() else []
    print(f"Resume: {len(ev_rows):,} events, {len(pm_rows):,} participations, {len(done)} matches.", flush=True)

    matches = [m for m in list_matches() if m["match_id"] not in done]
    print(f"{len(matches)} matches to fetch (24 real competitions).", flush=True)
    start = time.time()
    n = 0
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
        e_rows, p_rows = parse_match(ev, m)
        ev_rows.extend(e_rows); pm_rows.extend(p_rows)
        done.add(m["match_id"]); n += 1
        if n % 50 == 0:
            pd.DataFrame(ev_rows).to_parquet(EVENTS_OUT, index=False)
            pd.DataFrame(pm_rows).to_csv(PM_OUT, index=False, encoding="utf-8")
            PROGRESS.write_text(json.dumps(sorted(done)))
            print(f"  {n} matches; {len(ev_rows):,} events; {len(pm_rows):,} participations; "
                  f"{int(time.time()-start)}s", flush=True)

    pd.DataFrame(ev_rows).to_parquet(EVENTS_OUT, index=False)
    pd.DataFrame(pm_rows).to_csv(PM_OUT, index=False, encoding="utf-8")
    PROGRESS.write_text(json.dumps(sorted(done)))
    ev_df = pd.DataFrame(ev_rows)
    print(f"DONE: {len(ev_df):,} REAL events, {len(pm_rows):,} REAL participations, "
          f"{len(done)} matches, {ev_df['player_id'].nunique():,} players.", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_statsbomb_full", main)
