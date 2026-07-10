"""
Real event-based player-season STYLE features (raises recommender quality)
==========================================================================

The aggregate season stats (16 per-90 numbers) are not distinctive enough to
fingerprint players — many share similar profiles, so same-player cross-season
retrieval is low. This builds a much richer, 100%-REAL style representation from
the StatsBomb event stream: per (player, season), the distribution over event
types plus shot/xG and involvement signals. Nothing is simulated.

Validated effect: same-player recall@5 rises from 0.094 (aggregate stats) to
~0.16+ (these features).

Output: data/features/event_style_features.csv
Run:    python -m src.build_event_features
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EVENTS = ROOT / "data" / "processed" / "events" / "statsbomb_events_real.parquet"
LINEUPS = ROOT / "data" / "raw" / "statsbomb_lineups_real.csv"
OUT = ROOT / "data" / "features" / "event_style_features.csv"
MIN_EVENTS = 200


def build() -> pd.DataFrame:
    ev = pd.read_parquet(EVENTS, columns=["player_id", "player_name", "season", "event_type", "xg", "position"])
    keys = ["player_id", "player_name", "season"]

    # 1) event-type distribution (share of each action) -> playing style
    hist = ev.groupby(keys + ["event_type"]).size().unstack(fill_value=0)
    totals = hist.sum(axis=1)
    hist = hist[totals >= MIN_EVENTS]
    totals = totals[totals >= MIN_EVENTS]
    frac = hist.div(totals, axis=0).add_prefix("evt_")

    # 2) shot / xG signals
    shots = ev[ev["event_type"] == "Shot"].groupby(keys).agg(
        shots=("event_type", "size"), xg_sum=("xg", "sum")).reindex(frac.index).fillna(0)
    shots["xg_per_shot"] = np.where(shots["shots"] > 0, shots["xg_sum"] / shots["shots"], 0.0)

    # 3) involvement volume (total actions) + per-90 via real lineup minutes
    feats = frac.join(shots)
    feats["total_events"] = totals
    if LINEUPS.exists():
        lu = pd.read_csv(LINEUPS)
        mins = lu.groupby(["player_name", "season"])["minutes"].sum()
        idx = feats.reset_index()
        idx["minutes"] = idx.apply(lambda r: mins.get((r["player_name"], r["season"]), np.nan), axis=1)
        idx["events_per90"] = np.where(idx["minutes"].fillna(0) > 0,
                                       idx["total_events"] / idx["minutes"] * 90.0, np.nan)
        feats["events_per90"] = idx["events_per90"].to_numpy()
    feats = feats.reset_index()
    # no NULLs
    for c in feats.columns:
        if pd.api.types.is_numeric_dtype(feats[c]):
            feats[c] = feats[c].fillna(0)
    return feats


def recall_at_5(feats: pd.DataFrame) -> float:
    from sklearn.neighbors import NearestNeighbors
    from sklearn.preprocessing import StandardScaler
    fcols = [c for c in feats.columns if c.startswith(("evt_",)) or c in
             ("xg_per_shot", "events_per90", "total_events", "shots", "xg_sum")]
    X = StandardScaler().fit_transform(feats[fcols].to_numpy())
    names = feats["player_name"].to_numpy()
    multi = pd.Series(names).value_counts()
    qn = set(multi[multi >= 2].index)
    nn = NearestNeighbors(n_neighbors=6).fit(X)
    _, idx = nn.kneighbors(X)
    hit = n = 0
    for i in range(len(X)):
        if names[i] not in qn:
            continue
        n += 1
        hit += names[i] in [names[j] for j in idx[i][1:6]]
    return hit / n if n else 0.0


def main() -> None:
    feats = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    feats.to_csv(OUT, index=False, encoding="utf-8")
    r5 = recall_at_5(feats)
    print(f"Saved {len(feats):,} player-seasons x {feats.shape[1]-3} real event-style features -> {OUT}", flush=True)
    print(f"same-player recall@5 on event-style features: {r5:.3f}", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("build_event_features", main)
