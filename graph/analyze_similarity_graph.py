"""analyze_similarity_graph.py  (P0-3 — consolidated graph analytics)

Single, reproducible graph-analytics step for the Week 12 deliverable. It loads
the k-NN player-similarity graph produced by ``graph/build_similarity_graph.py``
(real players only, since the upstream cluster-labels are already filtered to
real entities) and reports:

* connected components, degree / weighted degree,
* PageRank, betweenness, closeness, and **eigenvector** centrality,
* a ranking comparison of PageRank against an **external, independent** baseline
  (``goals_per90`` and ``minutes_played`` from the real season stats), aligned by
  ``player_id`` (not positionally).

Why this replaces the previous graph code (see ANALISIS_GOALDATA audit, H10):
  - one graph instead of two divergent ones,
  - exact centralities (the real catalog is small enough; no k=20 sampling),
  - eigenvector via ``eigenvector_centrality_numpy`` (the old run reported 0.0),
  - baseline is external, not ``position_suitability_score`` (which was a graph
    input -> circular), and is aligned by id.

Run:
  python -m graph.analyze_similarity_graph \
    --graph data/processed/similarity_graph.pkl \
    --season-stats data/processed/stats/player_season_stats_cleaned.parquet \
    --out-dir artifacts
"""
from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
from scipy.stats import kendalltau, spearmanr


def load_graph(path: str) -> nx.Graph:
    with open(path, "rb") as fh:
        return pickle.load(fh)


def external_baseline(season_stats_path: str) -> pd.DataFrame:
    """Independent popularity/quality baseline per player_id (real season data)."""
    df = pd.read_parquet(season_stats_path)
    cols = [c for c in ["goals_per90", "minutes_played", "goals"] if c in df.columns]
    agg = df.groupby("player_id")[cols].mean(numeric_only=True)
    return agg


def compute_centralities(g: nx.Graph) -> pd.DataFrame:
    deg = dict(g.degree())
    wdeg = dict(g.degree(weight="weight"))
    pagerank = nx.pagerank(g, weight="weight", max_iter=1000)
    # Exact betweenness/closeness — feasible on the small real-player graph.
    betweenness = nx.betweenness_centrality(g, weight=None, normalized=True)
    closeness = nx.closeness_centrality(g)
    try:
        eigenvector = nx.eigenvector_centrality_numpy(g, weight="weight")
    except Exception:
        # Fallback: power iteration with a high cap if numpy/ARPACK struggles.
        eigenvector = nx.eigenvector_centrality(g, weight="weight", max_iter=2000, tol=1e-06)

    rows = []
    for n in g.nodes():
        rows.append({
            "player_id": g.nodes[n].get("player_id", n),
            "player_name": g.nodes[n].get("player_name", str(n)),
            "position_group": g.nodes[n].get("position_group", "Unknown"),
            "degree": deg[n],
            "weighted_degree": float(wdeg[n]),
            "pagerank": float(pagerank[n]),
            "betweenness": float(betweenness[n]),
            "closeness": float(closeness[n]),
            "eigenvector": float(eigenvector[n]),
        })
    return pd.DataFrame(rows).sort_values("pagerank", ascending=False).reset_index(drop=True)


def compare_to_baseline(cent: pd.DataFrame, baseline: pd.DataFrame) -> dict:
    """Spearman/Kendall + top-k overlap of PageRank vs external baseline, by id."""
    merged = cent.merge(baseline, on="player_id", how="inner")
    out = {"n_compared": int(len(merged))}
    for base_col in [c for c in ["goals_per90", "minutes_played"] if c in merged.columns]:
        sub = merged[["pagerank", base_col]].dropna()
        if len(sub) < 3:
            continue
        sp = spearmanr(sub["pagerank"], sub[base_col])
        kd = kendalltau(sub["pagerank"], sub[base_col])
        entry = {
            "spearman_r": float(sp.correlation),
            "spearman_p": float(sp.pvalue),
            "kendall_tau": float(kd.correlation),
            "kendall_p": float(kd.pvalue),
        }
        for k in (10, 20, 50):
            top_g = set(merged.nlargest(k, "pagerank")["player_id"])
            top_b = set(merged.nlargest(k, base_col)["player_id"])
            entry[f"top{k}_overlap"] = len(top_g & top_b) / k
        out[f"pagerank_vs_{base_col}"] = entry
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--graph", default="data/processed/similarity_graph.pkl")
    parser.add_argument("--season-stats", default="data/processed/stats/player_season_stats_cleaned.parquet")
    parser.add_argument("--out-dir", default="artifacts")
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    g = load_graph(args.graph)
    components = list(nx.connected_components(g))
    cent = compute_centralities(g)
    comparison = compare_to_baseline(cent, external_baseline(args.season_stats))

    cent.to_csv(out / "graph_centralities.csv", index=False)
    stats = {
        "num_nodes": g.number_of_nodes(),
        "num_edges": g.number_of_edges(),
        "density": nx.density(g),
        "num_connected_components": len(components),
        "largest_component_size": max((len(c) for c in components), default=0),
        "degree": {"min": int(cent["degree"].min()), "max": int(cent["degree"].max()), "mean": float(cent["degree"].mean())},
        "eigenvector_nonzero": bool((cent["eigenvector"] > 0).any()),
        "top_10_by_pagerank": cent.head(10)[["player_name", "position_group", "pagerank"]].to_dict("records"),
        "ranking_comparison_vs_external_baseline": comparison,
    }
    (out / "graph_statistics.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({k: stats[k] for k in ["num_nodes", "num_edges", "num_connected_components", "eigenvector_nonzero", "ranking_comparison_vs_external_baseline"]}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))
    from src.logging_utils import run_logged
    run_logged("graph_analyze_similarity", main)
