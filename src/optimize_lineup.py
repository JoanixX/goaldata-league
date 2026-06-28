"""
Starting-XI optimisation via Integer Linear Programming (P2-3)
=============================================================

Turns the descriptive layers (real positions + performance indices) into a
*decision*: pick the starting XI that maximises total expected rating subject to
a valid formation. This is the client-facing "decision engine" the project
promises (line-ups / roles), and it directly consumes the P0-1 position fix and
the existing per-season indices.

Model (binary ILP, solved with PuLP / CBC):
    maximise  sum_i rating_i * x_i
    s.t.      sum_i x_i = 11
              sum_{i in GK}  x_i = 1
              sum_{i in DEF} x_i = formation.DEF      (e.g. 4)
              sum_{i in MID} x_i = formation.MID      (e.g. 3)
              sum_{i in FW}  x_i = formation.FW       (e.g. 3)
              x_i in {0,1}

Rating (z-scored within the candidate pool so positions are comparable):
    FW  : 0.60*scoring + 0.30*creation + 0.10*defence
    MID : 0.35*scoring + 0.40*creation + 0.25*defence
    DEF : 0.15*scoring + 0.25*creation + 0.60*defence
    GK  : 0.50*clean_sheet_rate + 0.50*saves_per90   (from goalkeeper_stats)

Run:  python -m src.optimize_lineup --season 2021-2022 --formation 4-3-3
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pulp

BASE_DIR = Path(__file__).resolve().parents[1]
SEASON_STATS = BASE_DIR / "data" / "processed" / "stats" / "player_season_stats_cleaned.parquet"
GK_STATS = BASE_DIR / "data" / "processed" / "stats" / "goalkeeper_stats_cleaned.parquet"
PLAYERS = BASE_DIR / "data" / "processed" / "core" / "players_cleaned.parquet"

FORMATIONS = {  # outfield only; GK is always 1
    "4-3-3": {"DEF": 4, "MID": 3, "FW": 3},
    "4-4-2": {"DEF": 4, "MID": 4, "FW": 2},
    "3-5-2": {"DEF": 3, "MID": 5, "FW": 2},
    "4-2-3-1": {"DEF": 4, "MID": 5, "FW": 1},
}

_OUTFIELD_WEIGHTS = {
    "FW": (0.60, 0.30, 0.10),
    "MID": (0.35, 0.40, 0.25),
    "DEF": (0.15, 0.25, 0.60),
}


def _z(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    std = s.std(ddof=0)
    return (s - s.mean()) / std if std and std > 0 else s * 0.0


def build_candidate_pool(season: str, min_minutes: int = 900) -> pd.DataFrame:
    ps = pd.read_parquet(SEASON_STATS)
    players = pd.read_parquet(PLAYERS, columns=["player_id", "player_name", "profile_data_source"])
    ps = ps.merge(players, on="player_id", how="left")
    # Real players only, enough minutes for a reliable rating, in the chosen season.
    ps = ps[(ps["profile_data_source"] != "imputed_team_season_roster")
            & (ps["season"].astype(str) == season)
            & (pd.to_numeric(ps["minutes_played"], errors="coerce") >= min_minutes)].copy()
    pos = ps["player_position_group"].astype(str).str.upper()
    ps["position_group"] = pos.where(pos.isin(["GK", "DEF", "MID", "FW"]), "MID")

    # Outfield rating from z-scored indices, weighted by role.
    sc, cr, de = _z(ps["scoring_index"]), _z(ps["creator_index"]), _z(ps["defensive_index"])
    rating = pd.Series(0.0, index=ps.index)
    for grp, (ws, wc, wd) in _OUTFIELD_WEIGHTS.items():
        m = ps["position_group"].eq(grp)
        rating[m] = ws * sc[m] + wc * cr[m] + wd * de[m]
    ps["rating"] = rating

    # GK rating from goalkeeper_stats (clean sheets + saves), z-scored among GKs.
    gk = pd.read_parquet(GK_STATS)
    gk = gk[gk["season"].astype(str) == season]
    if not gk.empty:
        gk_rating = (0.5 * _z(gk.get("clean_sheet_rate", 0)) + 0.5 * _z(gk.get("saves_per90", 0)))
        gk_map = dict(zip(gk["player_id"].astype(str), gk_rating))
        is_gk = ps["position_group"].eq("GK")
        ps.loc[is_gk, "rating"] = ps.loc[is_gk, "player_id"].astype(str).map(gk_map).fillna(0.0).to_numpy()
    return ps[["player_id", "player_name", "position_group", "minutes_played", "rating"]].reset_index(drop=True)


def optimise_xi(pool: pd.DataFrame, formation: dict[str, int]) -> pd.DataFrame:
    prob = pulp.LpProblem("starting_xi", pulp.LpMaximize)
    x = {i: pulp.LpVariable(f"x_{i}", cat="Binary") for i in pool.index}
    prob += pulp.lpSum(pool.loc[i, "rating"] * x[i] for i in pool.index)
    prob += pulp.lpSum(x.values()) == 11
    prob += pulp.lpSum(x[i] for i in pool.index if pool.loc[i, "position_group"] == "GK") == 1
    for grp, n in formation.items():
        prob += pulp.lpSum(x[i] for i in pool.index if pool.loc[i, "position_group"] == grp) == n
    status = prob.solve(pulp.PULP_CBC_CMD(msg=False))
    if pulp.LpStatus[status] != "Optimal":
        raise RuntimeError(f"ILP not optimal: {pulp.LpStatus[status]} (pool too small for formation?)")
    chosen = [i for i in pool.index if x[i].value() and x[i].value() > 0.5]
    return pool.loc[chosen].sort_values(["position_group", "rating"], ascending=[True, False])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2021-2022")
    parser.add_argument("--formation", default="4-3-3", choices=list(FORMATIONS))
    parser.add_argument("--min-minutes", type=int, default=900)
    args = parser.parse_args()

    pool = build_candidate_pool(args.season, args.min_minutes)
    counts = pool["position_group"].value_counts().to_dict()
    print(f"Candidate pool ({args.season}, >= {args.min_minutes} min): {len(pool)} real players {counts}")
    xi = optimise_xi(pool, FORMATIONS[args.formation])
    print(f"\nOptimal XI ({args.formation}), total rating = {xi['rating'].sum():.3f}:")
    print(xi[["player_name", "position_group", "minutes_played", "rating"]].to_string(index=False))
    out = BASE_DIR / "artifacts" / f"optimal_xi_{args.season}_{args.formation}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    xi[["player_name", "position_group", "minutes_played", "rating"]].to_csv(out, index=False, encoding="utf-8")
    print(f"saved -> {out}")


if __name__ == "__main__":
    main()
