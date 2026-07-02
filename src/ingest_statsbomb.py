"""
Real player data from StatsBomb Open Data (HTTP, hang-proof)
============================================================

Builds REAL per-player season stats from StatsBomb Open Data events. Unlike the
FBref/Selenium path (which hung), this uses plain HTTPS JSON with a hard per-
request timeout, retries, a match cap, a wall-clock budget and checkpointing, so
it can be monitored and never hangs.

Real fields obtained per player-season (the ones that were imputed before):
  team, position, minutes, matches, goals, shots, shots_on_target, xg, assists,
  passes_completed, passes_attempted, tackles, interceptions, fouls_committed,
  yellow_cards, red_cards.

Output (checkpointed, resumable):
  data/raw/statsbomb_player_season.csv
  data/raw/statsbomb_progress.json   (processed match ids)

Run:  python -m src.ingest_statsbomb --max-matches 1500 --budget-min 35
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
RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
OUT = RAW / "statsbomb_player_season.csv"
PROGRESS = RAW / "statsbomb_progress.json"


def get(url: str, timeout: int = 20, retries: int = 2):
    last = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as exc:  # network / 404 / timeout
            last = exc
            time.sleep(1.5 * (attempt + 1))
    raise last


def sb_position_group(name: str) -> str:
    n = (name or "").lower()
    if "goalkeeper" in n:
        return "GK"
    if "back" in n:                 # Center/Left/Right Back, Wing Back -> defender
        return "DEF"
    if "midfield" in n:
        return "MID"
    if "wing" in n or "forward" in n or "striker" in n:
        return "FW"
    return "MID"


def norm_season(name: str) -> str:
    return str(name).replace("/", "-").strip()


def list_matches() -> list[dict]:
    comps = get(f"{BASE}/competitions.json")
    matches = []
    for c in comps:
        try:
            ms = get(f"{BASE}/matches/{c['competition_id']}/{c['season_id']}.json")
        except Exception:
            continue
        for m in ms:
            matches.append({
                "match_id": m["match_id"],
                "season": norm_season(c["season_name"]),
                "competition": c["competition_name"],
            })
    return matches


def aggregate_match(events: list[dict]) -> dict[int, dict]:
    acc: dict[int, dict] = defaultdict(lambda: defaultdict(float))
    pos_count: dict[int, dict] = defaultdict(lambda: defaultdict(int))
    meta: dict[int, dict] = {}
    minute_min: dict[int, float] = {}
    minute_max: dict[int, float] = {}
    for e in events:
        pl = e.get("player")
        if not pl:
            continue
        pid = pl["id"]
        meta.setdefault(pid, {"player_name": pl.get("name"),
                              "team": e.get("team", {}).get("name")})
        pos = e.get("position", {}).get("name")
        if pos:
            pos_count[pid][pos] += 1
        minute = e.get("minute", 0)
        minute_min[pid] = min(minute_min.get(pid, 999), minute)
        minute_max[pid] = max(minute_max.get(pid, -1), minute)
        t = e["type"]["name"]
        a = acc[pid]
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
            outcome = sh.get("outcome", {}).get("name")
            if outcome == "Goal":
                a["goals"] += 1
            if outcome in {"Goal", "Saved", "Saved to Post"}:
                a["shots_on_target"] += 1
        elif t == "Interception":
            a["interceptions"] += 1
        elif t == "Foul Committed":
            a["fouls_committed"] += 1
            card = e.get("foul_committed", {}).get("card", {}).get("name", "")
            if "Yellow" in card:
                a["yellow_cards"] += 1
            elif "Red" in card:
                a["red_cards"] += 1
        elif t == "Bad Behaviour":
            card = e.get("bad_behaviour", {}).get("card", {}).get("name", "")
            if "Yellow" in card:
                a["yellow_cards"] += 1
            elif "Red" in card:
                a["red_cards"] += 1
        elif t == "Duel" and e.get("duel", {}).get("type", {}).get("name") == "Tackle":
            a["tackles"] += 1

    out = {}
    for pid, a in acc.items():
        span = max(0.0, minute_max.get(pid, 0) - minute_min.get(pid, 0)) + 1
        rec = {**meta[pid], **a}
        rec["minutes"] = float(min(span, 95.0))
        rec["matches"] = 1
        best_pos = max(pos_count[pid].items(), key=lambda kv: kv[1])[0] if pos_count[pid] else ""
        rec["position_group"] = sb_position_group(best_pos)
        out[pid] = rec
    return out


_STYLE_COLS = ["shots", "shots_on_target", "passes_completed", "passes_attempted",
               "tackles", "interceptions", "fouls_committed"]


def load_statsbomb_style_rates(path: str | Path = OUT, min_minutes: float = 450.0) -> pd.DataFrame:
    """Per-90 STYLE rates per player from StatsBomb (career-aggregated).

    StatsBomb season coverage is partial, so totals are NOT full-season; but
    per-90 *style* rates are valid once a player has enough minutes. We aggregate
    a player across all StatsBomb matches (sum minutes + style counts), keep those
    with >= min_minutes, and return per-90 rates indexed by canonical name.
    """
    from src.position_resolution import _canonical_name

    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "player_name" not in df.columns or "minutes" not in df.columns:
        return pd.DataFrame()
    df["_cn"] = df["player_name"].map(_canonical_name)
    cols = [c for c in _STYLE_COLS if c in df.columns]
    agg = df.groupby("_cn").agg({**{c: "sum" for c in cols}, "minutes": "sum"})
    agg = agg[agg["minutes"] >= min_minutes]
    rates = pd.DataFrame(index=agg.index)
    for c in cols:
        rates[f"{c}_rate90"] = agg[c] / agg["minutes"] * 90.0
    return rates


def match_catalog_to_statsbomb(catalog_names, rates: pd.DataFrame, min_score: int = 90) -> dict:
    """Map catalog player_name -> StatsBomb canonical key (exact, else fuzzy by surname).

    Surname-blocked fuzzy keeps it cheap and avoids cross-surname false matches.
    """
    from rapidfuzz import fuzz
    from src.position_resolution import _canonical_name

    sb_keys = list(rates.index)
    by_surname: dict[str, list[str]] = {}
    for k in sb_keys:
        parts = k.split()
        if parts:
            by_surname.setdefault(parts[-1], []).append(k)
    sb_set = set(sb_keys)
    out = {}
    for name in catalog_names:
        cn = _canonical_name(name)
        if not cn:
            continue
        if cn in sb_set:
            out[name] = cn
            continue
        surname = cn.split()[-1]
        cands = by_surname.get(surname, [])
        best, best_s = None, 0
        for c in cands:
            s = fuzz.token_set_ratio(cn, c)
            if s > best_s:
                best, best_s = c, s
        if best is not None and best_s >= min_score:
            out[name] = best
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-matches", type=int, default=1500)
    ap.add_argument("--budget-min", type=float, default=35.0)
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)
    done = set(json.loads(PROGRESS.read_text())) if PROGRESS.exists() else set()
    rows = []
    if OUT.exists():
        rows = pd.read_csv(OUT).to_dict("records")

    print("Listing StatsBomb matches...", flush=True)
    matches = list_matches()
    todo = [m for m in matches if m["match_id"] not in done][:args.max_matches]
    print(f"{len(matches)} total matches; {len(todo)} to fetch this run "
          f"(cap {args.max_matches}, budget {args.budget_min} min)", flush=True)

    start = time.time()
    processed = 0
    for i, m in enumerate(todo, 1):
        if (time.time() - start) / 60.0 > args.budget_min:
            print(f"Budget reached; stopping at {processed} matches.", flush=True)
            break
        try:
            ev = get(f"{BASE}/events/{m['match_id']}.json", timeout=args.timeout)
        except Exception as exc:
            print(f"  skip {m['match_id']} ({type(exc).__name__})", flush=True)
            done.add(m["match_id"])
            continue
        for pid, rec in aggregate_match(ev).items():
            rec["season"] = m["season"]
            rec["competition"] = m["competition"]
            rows.append(rec)
        done.add(m["match_id"])
        processed += 1
        if i % 50 == 0:
            pd.DataFrame(rows).to_csv(OUT, index=False, encoding="utf-8")
            PROGRESS.write_text(json.dumps(sorted(done)))
            print(f"  [{i}/{len(todo)}] matches; rows so far={len(rows):,}; "
                  f"elapsed={int(time.time()-start)}s", flush=True)

    # final season aggregation (sum match-level player rows -> player-season)
    df = pd.DataFrame(rows)
    if not df.empty:
        count_cols = ["minutes", "matches", "goals", "assists", "shots", "shots_on_target",
                      "xg", "passes_completed", "passes_attempted", "tackles",
                      "interceptions", "fouls_committed", "yellow_cards", "red_cards"]
        for c in count_cols:
            if c not in df.columns:
                df[c] = 0.0
        agg = (df.groupby(["player_name", "season"], as_index=False)
                 .agg({**{c: "sum" for c in count_cols},
                       "position_group": lambda s: s.mode().iat[0] if not s.mode().empty else "MID",
                       "team": lambda s: s.mode().iat[0] if not s.mode().empty else ""}))
        agg.to_csv(OUT, index=False, encoding="utf-8")
        PROGRESS.write_text(json.dumps(sorted(done)))
        print(f"Saved {len(agg):,} real player-seasons ({agg['player_name'].nunique():,} players) -> {OUT}", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_statsbomb", main)
