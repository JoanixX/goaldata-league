"""
Real multi-season player ingestion from FBref (P1-2)
====================================================

Pulls real player-season **positions and season stats** for the Big-5 European
leagues across several seasons via ``soccerdata`` (FBref), and saves a tidy table
plus an authoritative ``canonical_name -> position_group`` map.

Why: the committed FBref file only covers 2021-2022 (~35% of our real players);
multi-season coverage fixes positions for players missing there (e.g. Modric) and
provides real multi-season rows that the recommender can use.

Output:
  data/raw/fbref_big5_multiseason.csv   (real, tidy: player, season, team, pos, stats)

Run:  python -m src.ingest_real_player_data
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_PATH = BASE_DIR / "data" / "raw" / "fbref_big5_multiseason.csv"

SEASONS = [f"{y}-{y + 1}" for y in range(2005, 2025)]  # 2005-2006 .. 2024-2025


def _flat(columns) -> list[str]:
    out = []
    for c in columns:
        if isinstance(c, tuple):
            parts = [str(p) for p in c if str(p) and not str(p).startswith("Unnamed")]
            out.append("|".join(parts) if len(parts) > 1 else (parts[0] if parts else ""))
        else:
            out.append(str(c))
    return out


# csv column -> our processed-schema column. Only columns present are overlaid.
_OVERLAY_MAP = {
    "goals": "goals", "assists": "assists", "minutes": "minutes_played",
    "matches": "matches_played", "yellow_cards": "yellow_cards", "red_cards": "red_cards",
    "shots": "shots", "shots_on_target": "shots_on_target",
    "fouls_committed": "fouls_committed", "tackles": "tackles", "interceptions": "interceptions",
}


def load_real_season_stats(path: str | Path) -> pd.DataFrame:
    """Real FBref season totals keyed by (canonical_name, season).

    Used to OVERLAY real, per-player values onto the real players' season stats
    (replacing position-median imputation that homogenised profiles). A player who
    changed club mid-season has several FBref rows; counts are summed to the full
    season total. Returns a DataFrame indexed by (canonical_name, season) whose
    columns use the processed-table schema names.
    """
    from src.position_resolution import _canonical_name

    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "player_name" not in df.columns or "season" not in df.columns:
        return pd.DataFrame()
    present = {src: dst for src, dst in _OVERLAY_MAP.items() if src in df.columns}
    work = df[["player_name", "season", *present]].copy()
    work["_cn"] = work["player_name"].map(_canonical_name)
    work["season"] = work["season"].astype(str)
    work = work.rename(columns=present)
    agg = work.groupby(["_cn", "season"])[list(present.values())].sum(numeric_only=True)
    return agg


# soccerdata Big-5 *combined* only exposes these stat types (no passing/defense),
# but they are reliable (no Selenium hang). We pull standard + shooting + misc and
# merge per player-team-season to get real full-season shots/SoT/fouls too.
_STD_MAP = {
    "player": "player_name", "team": "team", "pos": "pos", "nation": "nation", "age": "age",
    "Playing Time|Min": "minutes", "Playing Time|MP": "matches",
    "Performance|Gls": "goals", "Performance|Ast": "assists",
    "Performance|CrdY": "yellow_cards", "Performance|CrdR": "red_cards",
}
_SHOOT_MAP = {"player": "player_name", "team": "team",
              "Standard|Sh": "shots", "Standard|SoT": "shots_on_target"}
_MISC_MAP = {"player": "player_name", "team": "team", "Performance|Fls": "fouls_committed"}


def _read(fb, stat: str, colmap: dict) -> pd.DataFrame:
    df = fb.read_player_season_stats(stat_type=stat).reset_index()
    df.columns = _flat(df.columns)
    cols = {k: v for k, v in colmap.items() if k in df.columns}
    return df[list(cols)].rename(columns=cols)


def fetch() -> pd.DataFrame:
    """Incremental fetch: seasons already in the output CSV are kept, not re-scraped."""
    import soccerdata as sd

    frames = []
    have: set[str] = set()
    if OUT_PATH.exists():
        existing = pd.read_csv(OUT_PATH)
        if "season" in existing.columns and len(existing):
            have = set(existing["season"].astype(str).unique())
            frames.append(existing)
            print(f"  keeping {len(existing):,} rows already ingested ({sorted(have)})", flush=True)
    for season in SEASONS:
        if season in have:
            continue
        try:
            fb = sd.FBref(leagues="Big 5 European Leagues Combined", seasons=season)
            tidy = _read(fb, "standard", _STD_MAP)
            for stat, cmap in (("shooting", _SHOOT_MAP), ("misc", _MISC_MAP)):
                try:
                    extra = _read(fb, stat, cmap)
                    tidy = tidy.merge(extra, on=["player_name", "team"], how="left")
                except Exception as exc:  # one stat type failing must not lose the season
                    print(f"  {season}/{stat}: skip ({type(exc).__name__})", flush=True)
            tidy["season"] = season
            frames.append(tidy)
            print(f"  {season}: {len(tidy)} players (cols: {[c for c in tidy.columns if c not in ('player_name','team','season')]})", flush=True)
        except Exception as exc:  # keep going if one season fails (rate limit etc.)
            print(f"  {season}: FAILED ({type(exc).__name__}: {str(exc)[:80]})", flush=True)
    if not frames:
        raise RuntimeError("no seasons fetched")
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    print("Fetching FBref Big-5 multi-season player stats...", flush=True)
    df = fetch()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False, encoding="utf-8")
    n_players = df["player_name"].nunique() if "player_name" in df.columns else 0
    print(f"Saved {len(df):,} player-seasons ({n_players:,} unique players) -> {OUT_PATH}", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_real_player_data", main)
