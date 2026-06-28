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

SEASONS = ["2017-2018", "2018-2019", "2019-2020", "2020-2021",
           "2021-2022", "2022-2023", "2023-2024", "2024-2025"]


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


def fetch() -> pd.DataFrame:
    import soccerdata as sd

    frames = []
    for season in SEASONS:
        try:
            fb = sd.FBref(leagues="Big 5 European Leagues Combined", seasons=season)
            df = fb.read_player_season_stats(stat_type="standard").reset_index()
            df.columns = _flat(df.columns)
            keep = {
                "player": "player_name", "season": "season", "team": "team",
                "pos": "pos", "nation": "nation", "age": "age",
                "Playing Time|Min": "minutes", "Playing Time|MP": "matches",
                "Performance|Gls": "goals", "Performance|Ast": "assists",
                "Performance|CrdY": "yellow_cards", "Performance|CrdR": "red_cards",
            }
            cols = {k: v for k, v in keep.items() if k in df.columns}
            tidy = df[list(cols)].rename(columns=cols)
            tidy["season"] = season
            frames.append(tidy)
            print(f"  {season}: {len(tidy)} players", flush=True)
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
    main()
