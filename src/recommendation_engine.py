"""
Player similarity / ranking engine (Week 10)
============================================

Task framing
------------
This is a **content-based item-to-item similarity & ranking** system for
*scouting / squad planning*: given a query player-season, return the most
similar player-seasons (candidate replacements / comparables). The unit being
ranked is a ``(player, season)`` profile. There is no user-click interaction log,
so this is an item-similarity recommender, not collaborative filtering.

Two systems (so we can compare, per the rubric)
-----------------------------------------------
* **Baseline - content-based, 2 PCs, global pool.** Euclidean distance on the
  first two PCA axes over *all* player-seasons. Simple and cheap; this is the
  previous behaviour and the reference the stronger model must beat.
* **Stronger - segmentation-feeding ranking, full PC space, position-aware pool.**
  Standardized distance over *all retained* PCA components, with the candidate
  pool restricted to the same ``position_group`` (optionally the same K-Means
  cluster). Standardisation stops PC1 from dominating; the wider space keeps
  efficiency/discipline signal that the 2D map discards; the position pool stops
  the engine recommending a defender to a striker.

Candidate pool (explicit, per rubric)
-------------------------------------
``stronger``: player-seasons that share the query's ``position_group`` and are
not the query player. ``baseline``: every other player-season. The query player's
own rows (all seasons) are always excluded.

Why distance and not raw cosine on 2D
-------------------------------------
Cosine on 2 PCA axes made almost every in-cluster pair score ~1.0 (rank collapse).
Standardised Euclidean over the retained components restores ranking resolution
and is the conventional metric for PCA-space nearest-neighbours.

Run examples are at the bottom (``python -m src.recommendation_engine``).
"""
from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
CLUSTER_LABELS_PATH = ARTIFACTS_DIR / "player_season_cluster_labels.csv"

_PC_RE = re.compile(r"^PC\d+$")


def pc_columns(df: pd.DataFrame) -> list[str]:
    return sorted([c for c in df.columns if _PC_RE.match(c)], key=lambda c: int(c[2:]))


def load_data() -> pd.DataFrame:
    if not CLUSTER_LABELS_PATH.exists():
        raise FileNotFoundError(
            f"{CLUSTER_LABELS_PATH} not found. Run build_pca_feature_matrix.py and "
            "build_clustering_analysis.py first."
        )
    df = pd.read_csv(CLUSTER_LABELS_PATH)
    df["player_name"] = df["player_name"].astype(str).str.strip()
    return df


def _standardized_matrix(df: pd.DataFrame, cols: list[str]) -> np.ndarray:
    x = df[cols].to_numpy(dtype="float64")
    mu = x.mean(axis=0)
    sd = x.std(axis=0)
    sd[sd == 0] = 1.0
    return (x - mu) / sd


def find_target(df: pd.DataFrame, player_name: str, season: str | None = None) -> pd.Series:
    """Flexible lookup: exact -> case-insensitive contains; optional season filter."""
    name = player_name.strip()
    hit = df[df["player_name"].str.casefold() == name.casefold()]
    if hit.empty:
        hit = df[df["player_name"].str.contains(re.escape(name), case=False, na=False)]
    if hit.empty:
        raise ValueError(f"Player '{player_name}' not found in the player-season table.")
    if season is not None:
        season_hit = hit[hit["season"].astype(str) == str(season)]
        if not season_hit.empty:
            return season_hit.iloc[0]
    return hit.iloc[0]


def recommend(
    player_name: str,
    season: str | None = None,
    method: str = "stronger",
    top_n: int = 5,
    same_cluster: bool = False,
) -> pd.DataFrame:
    """Return the top-N most similar player-seasons to the query.

    method='baseline' -> Euclidean on PC1,PC2 over all player-seasons.
    method='stronger' -> standardized Euclidean over all retained PCs,
                         candidate pool = same position_group (and cluster if asked).
    """
    df = load_data().reset_index(drop=True)
    all_pcs = pc_columns(df)
    if not all_pcs:
        raise ValueError("No PC columns found in cluster labels; re-run the PCA step.")
    target = find_target(df, player_name, season)
    t_name, t_season = target["player_name"], target["season"]

    if method == "baseline":
        cols = [c for c in ["PC1", "PC2"] if c in all_pcs] or all_pcs[:2]
        pool_mask = df["player_name"] != t_name  # exclude all rows of the same player
        x = df[cols].to_numpy(dtype="float64")
        t_vec = target[cols].to_numpy(dtype="float64")
        dist = np.sqrt(((x - t_vec) ** 2).sum(axis=1))
        score_name, ascending = "distance", True
    else:
        cols = all_pcs
        xs = _standardized_matrix(df, cols)
        t_idx = target.name
        t_vec = xs[df.index.get_loc(t_idx)]
        dist = np.sqrt(((xs - t_vec) ** 2).sum(axis=1))
        pool_mask = (df["player_name"] != t_name) & (df["position_group"] == target["position_group"])
        if same_cluster and "kmeans_cluster" in df.columns:
            pool_mask &= df["kmeans_cluster"] == target["kmeans_cluster"]
        score_name, ascending = "distance", True

    out = df.loc[pool_mask, ["player_name", "season", "position_group"]].copy()
    out[score_name] = dist[pool_mask.to_numpy()]
    # similarity for readability (1 / (1 + distance))
    out["similarity"] = (1.0 / (1.0 + out[score_name])).round(4)
    out = out.sort_values(score_name, ascending=ascending).head(top_n).reset_index(drop=True)
    out.attrs["query"] = {"player": t_name, "season": str(t_season), "method": method,
                          "candidate_pool_size": int(pool_mask.sum()), "pc_dims": len(cols)}
    return out


if __name__ == "__main__":
    df = load_data()
    # Pick a real, named player (not a synthetic "Squad" placeholder, not "Unknown")
    # that appears in at least two seasons, for a meaningful demo.
    real = df[~df["player_name"].str.contains("Squad", case=False, na=False)
              & (df["player_name"].str.casefold() != "unknown")]
    multi = real.groupby("player_name")["season"].nunique()
    candidates = multi[multi >= 2].index.tolist()
    example = candidates[0] if candidates else (real["player_name"].iloc[0] if not real.empty else df["player_name"].iloc[0])
    print(f"Query player: {example}")
    print(f"\n=== BASELINE (PC1-PC2, global pool) ===")
    print(recommend(example, method="baseline").to_string(index=False))
    print(f"\n=== STRONGER (full PC space, same-position pool) ===")
    rec = recommend(example, method="stronger")
    print(rec.to_string(index=False))
    print("query meta:", rec.attrs.get("query"))
