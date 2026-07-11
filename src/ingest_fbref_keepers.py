"""
Real goalkeeper season statistics from FBref (Big-5, 2005-2025)
===============================================================

The previous `goalkeeper_stats_cleaned` derived saves/clean sheets from real
scorelines because the original FBref ingest only pulled outfield tables. FBref
publishes REAL goalkeeper tables (stat_type="keeper": saves, shots on target
against, goals against, clean sheets, save%) — this module ingests them for
every Big-5 season 2005-2006 .. 2024-2025 via `soccerdata`, and
`build_goalkeeper_stats` rebuilds `goalkeeper_stats_cleaned` from them:
100% observed values, no derivation.

Outputs:
  data/raw/fbref_big5_keepers.csv                      (raw real keeper rows)
  data/processed/stats/goalkeeper_stats_cleaned.*      (rebuilt, observed_fbref_keeper)

Run:  python -m src.ingest_fbref_keepers
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
RAW_OUT = BASE / "data" / "raw" / "fbref_big5_keepers.csv"
PROC = BASE / "data" / "processed"
GK_STEM = PROC / "stats" / "goalkeeper_stats_cleaned"

SEASONS = [f"{y}-{y + 1}" for y in range(2005, 2025)]

_KEEPER_MAP = {
    "player": "player_name", "team": "team", "nation": "nation", "age": "age",
    "Playing Time|Min": "minutes_played", "Playing Time|MP": "matches_played",
    "Performance|GA": "goals_against", "Performance|SoTA": "shots_on_target_against",
    "Performance|Saves": "saves", "Performance|Save%": "save_pct",
    "Performance|CS": "clean_sheets", "Performance|CS%": "clean_sheet_pct",
    "Performance|W": "wins", "Performance|D": "draws", "Performance|L": "losses",
    "Penalty Kicks|PKA": "penalties_faced", "Penalty Kicks|PKsv": "penalties_saved",
}


def _flat(columns) -> list[str]:
    out = []
    for c in columns:
        if isinstance(c, tuple):
            parts = [str(p) for p in c if str(p) and not str(p).startswith("Unnamed")]
            out.append("|".join(parts) if len(parts) > 1 else (parts[0] if parts else ""))
        else:
            out.append(str(c))
    return out


def fetch() -> pd.DataFrame:
    import soccerdata as sd

    frames = []
    have: set[str] = set()
    if RAW_OUT.exists():
        existing = pd.read_csv(RAW_OUT)
        have = set(existing["season"].astype(str).unique())
        frames.append(existing)
        print(f"  keeping {len(existing):,} rows already ingested", flush=True)
    missing = [s for s in SEASONS if s not in have]
    if missing:
        fb = sd.FBref(leagues="Big 5 European Leagues Combined", seasons=missing)
        df = fb.read_player_season_stats(stat_type="keeper").reset_index()
        df.columns = _flat(df.columns)
        cols = {k: v for k, v in _KEEPER_MAP.items() if k in df.columns}
        tidy = df[["season"] + list(cols)].rename(columns=cols)
        # soccerdata season ids like '0506' -> '2005-2006'
        def _label(s):
            s = str(s)
            if "-" in s:
                return s
            a = int(s[:2])
            year = 2000 + a if a < 90 else 1900 + a
            return f"{year}-{year + 1}"
        tidy["season"] = tidy["season"].map(_label)
        frames.append(tidy)
        print(f"  fetched {len(tidy):,} keeper-season rows for {len(missing)} seasons", flush=True)
    return pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["player_name", "team", "season"])


def build_goalkeeper_stats(raw: pd.DataFrame) -> pd.DataFrame:
    """Rebuild goalkeeper_stats_cleaned from REAL FBref keeper rows."""
    from src.position_resolution import _canonical_name
    from src.build_processed import stable_id

    players = pd.read_parquet(PROC / "core" / "players_cleaned.parquet",
                              columns=["player_id", "player_name"])
    by_canon = {}
    for pid, name in zip(players["player_id"], players["player_name"]):
        by_canon.setdefault(_canonical_name(name), pid)

    gk = raw.copy()
    for c in gk.columns:
        if c not in ("player_name", "team", "season", "nation"):
            gk[c] = pd.to_numeric(gk[c], errors="coerce")
    # aggregate multi-club seasons to one row per (player, season)
    num_cols = [c for c in ("minutes_played", "matches_played", "goals_against",
                            "shots_on_target_against", "saves", "clean_sheets",
                            "wins", "draws", "losses", "penalties_faced",
                            "penalties_saved") if c in gk.columns]
    agg = (gk.groupby([gk["player_name"].map(_canonical_name), "season"])
           .agg({**{c: "sum" for c in num_cols}, "player_name": "first", "team": "first"})
           .rename_axis(["_canon", "season"]).reset_index())

    agg["player_id"] = agg["_canon"].map(by_canon)
    agg["player_id"] = agg["player_id"].fillna(agg["_canon"].map(lambda c: stable_id("player", c)))
    mins = agg["minutes_played"].clip(lower=1.0)
    agg["saves_per90"] = (agg["saves"] / mins * 90.0).round(4)
    agg["goals_against_per90"] = (agg["goals_against"] / mins * 90.0).round(4)
    agg["clean_sheet_rate"] = (agg["clean_sheets"] / agg["matches_played"].clip(lower=1)).round(4)
    agg["save_pct"] = np.where(agg["shots_on_target_against"] > 0,
                               (agg["saves"] / agg["shots_on_target_against"]).round(4), 0.0)
    agg["position_group"] = "GK"
    agg["data_provenance"] = "observed_fbref_keeper"
    return agg.drop(columns=["_canon"])


def main() -> None:
    raw = fetch()
    RAW_OUT.parent.mkdir(parents=True, exist_ok=True)
    raw.to_csv(RAW_OUT, index=False, encoding="utf-8")
    print(f"Saved {len(raw):,} raw keeper rows -> {RAW_OUT}", flush=True)

    gk = build_goalkeeper_stats(raw)
    gk.to_parquet(GK_STEM.with_suffix(".parquet"), index=False)
    gk.to_csv(GK_STEM.with_suffix(".csv"), index=False, encoding="utf-8")
    print(f"Rebuilt goalkeeper_stats_cleaned: {len(gk):,} REAL keeper-seasons "
          f"({gk['player_id'].nunique():,} keepers) — provenance observed_fbref_keeper", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_fbref_keepers", main)
