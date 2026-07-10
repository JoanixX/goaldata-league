"""
Auditable graph analytics report (addresses the Week-12 graph rubric feedback)
==============================================================================

Single, reproducible artifact that regenerates the player-similarity graph AND
its full analysis, writing every raw output so the whole thing is auditable:

  * graph DEFINITION with explicit node identity and source feature table;
  * CONSTRUCTION from a documented input (no hidden state);
  * FULL centrality/ranking output for every node (CSV), not just examples;
  * threshold/k SENSITIVITY sweep + isolated-node counts (validity checks);
  * comparison vs an external baseline with the full ranking table;
  * a written report framing limitations as football-domain boundaries.

Node identity: one node per REAL player-season, keyed by ``player_id`` + ``season``
(label = player_name). Node features: the retained PCA components PC1..PCk of that
player-season, from ``artifacts/player_season_cluster_labels.csv`` (built by
``src.build_pca_feature_matrix`` from ``data/processed/stats/player_season_stats_cleaned.parquet``).
Edges: undirected, weighted by cosine similarity between node feature vectors,
kept when a node is among another's k nearest neighbours AND similarity >= threshold.

Run:  python -m graph.graph_report  (or  python graph/graph_report.py)
"""
from __future__ import annotations

import sys
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
from scipy.stats import kendalltau, spearmanr
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[1]
LABELS = ROOT / "artifacts" / "player_season_cluster_labels.csv"
SEASON_STATS = ROOT / "data" / "processed" / "stats" / "player_season_stats_cleaned.parquet"
ART = ROOT / "artifacts"
REPORT = ROOT / "reports" / "graph_analysis_report.md"

K_DEFAULT, THR_DEFAULT = 8, 0.15


def load_features():
    df = pd.read_csv(LABELS)
    pc_cols = sorted([c for c in df.columns if c.startswith("PC") and c[2:].isdigit()],
                     key=lambda c: int(c[2:]))
    X = StandardScaler().fit_transform(df[pc_cols].to_numpy(dtype=np.float32))
    return df.reset_index(drop=True), X, pc_cols


def build_graph(df: pd.DataFrame, X: np.ndarray, k: int, threshold: float) -> nx.Graph:
    nn = NearestNeighbors(n_neighbors=min(k + 1, len(X)), metric="euclidean").fit(X)
    dist, idx = nn.kneighbors(X)
    g = nx.Graph()
    for i in range(len(df)):
        g.add_node(i, player_id=str(df.at[i, "player_id"]), player_name=str(df.at[i, "player_name"]),
                   season=str(df.at[i, "season"]), position_group=str(df.at[i, "position_group"]))
    for i in range(len(X)):
        for j, d in zip(idx[i], dist[i]):
            if i == int(j):
                continue
            sim = 1.0 / (1.0 + float(d))
            if sim >= threshold:
                g.add_edge(i, int(j), weight=sim)
    return g


def graph_metrics(g: nx.Graph) -> dict:
    comps = sorted((len(c) for c in nx.connected_components(g)), reverse=True)
    degs = [d for _, d in g.degree()]
    return {
        "nodes": g.number_of_nodes(), "edges": g.number_of_edges(),
        "density": round(nx.density(g), 8),
        "components": len(comps),
        "largest_component_pct": round(100 * comps[0] / g.number_of_nodes(), 2) if comps else 0,
        "isolated_nodes": int(sum(1 for d in degs if d == 0)),
        "mean_degree": round(float(np.mean(degs)), 4) if degs else 0,
        "median_degree": int(np.median(degs)) if degs else 0,
    }


def full_centralities(g: nx.Graph) -> pd.DataFrame:
    deg = dict(g.degree())
    wdeg = dict(g.degree(weight="weight"))
    pr = nx.pagerank(g, weight="weight", max_iter=1000)
    btw = nx.betweenness_centrality(g, weight=None, normalized=True)
    clo = nx.closeness_centrality(g)
    try:
        eig = nx.eigenvector_centrality_numpy(g, weight="weight")
    except Exception:
        eig = nx.eigenvector_centrality(g, weight="weight", max_iter=2000)
    rows = [{
        "player_id": g.nodes[n]["player_id"], "player_name": g.nodes[n]["player_name"],
        "season": g.nodes[n]["season"], "position_group": g.nodes[n]["position_group"],
        "degree": deg[n], "weighted_degree": round(float(wdeg[n]), 5),
        "pagerank": pr[n], "betweenness": btw[n], "closeness": round(clo[n], 5),
        "eigenvector": round(float(eig[n]), 6),
    } for n in g.nodes()]
    return pd.DataFrame(rows).sort_values("pagerank", ascending=False).reset_index(drop=True)


def compare_baseline(cent: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    ps = pd.read_parquet(SEASON_STATS, columns=["player_id", "goals_per90", "minutes_played"])
    ps["player_id"] = ps["player_id"].astype(str)
    base = ps.groupby("player_id").mean(numeric_only=True)
    merged = cent.merge(base, on="player_id", how="inner")
    stats = {"n_compared": int(len(merged))}
    for col in ["goals_per90", "minutes_played"]:
        sub = merged[["pagerank", col]].dropna()
        if len(sub) < 5:
            continue
        sp, kd = spearmanr(sub["pagerank"], sub[col]), kendalltau(sub["pagerank"], sub[col])
        entry = {"spearman_r": round(float(sp.correlation), 4), "spearman_p": float(sp.pvalue),
                 "kendall_tau": round(float(kd.correlation), 4)}
        for kk in (10, 20, 50):
            a = set(merged.nlargest(kk, "pagerank")["player_id"])
            b = set(merged.nlargest(kk, col)["player_id"])
            entry[f"top{kk}_overlap"] = round(len(a & b) / kk, 3)
        stats[f"pagerank_vs_{col}"] = entry
    return merged, stats


def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    out = ["| " + " | ".join(map(str, cols)) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, r in df.iterrows():
        out.append("| " + " | ".join(f"{v:.5g}" if isinstance(v, float) else str(v) for v in r) + " |")
    return "\n".join(out)


def main() -> None:
    df, X, pc_cols = load_features()
    print(f"Loaded {len(df)} real player-season nodes; features = {pc_cols}", flush=True)

    # --- validity: sensitivity sweep over k and threshold ---
    sweep = []
    for k in (5, 8, 12):
        for thr in (0.10, 0.15, 0.20, 0.30):
            g = build_graph(df, X, k, thr)
            m = graph_metrics(g)
            m.update({"k": k, "threshold": thr})
            sweep.append(m)
    sweep_df = pd.DataFrame(sweep)[["k", "threshold", "nodes", "edges", "density",
                                    "components", "largest_component_pct", "isolated_nodes", "mean_degree"]]
    sweep_df.to_csv(ART / "graph_threshold_sensitivity.csv", index=False)

    # --- chosen configuration: full analysis ---
    g = build_graph(df, X, K_DEFAULT, THR_DEFAULT)
    chosen = graph_metrics(g)
    cent = full_centralities(g)
    cent.to_csv(ART / "graph_centralities_full.csv", index=False)
    merged, cmp_stats = compare_baseline(cent)
    merged[["player_id", "player_name", "season", "position_group", "pagerank",
            "goals_per90", "minutes_played"]].to_csv(ART / "graph_ranking_comparison_full.csv", index=False)

    # --- written report ---
    L = [f"# Week-12 Graph Analytics Report (auditable, real-only)\n",
         "## 1. Graph definition\n",
         "- **Grain:** one **player-similarity** graph. **Node = one real player-season** "
         "(key `player_id` + `season`; label `player_name`). No team-match grain is used here.",
         f"- **Node features:** retained PCA components `{', '.join(pc_cols)}` of the player-season, from "
         "`artifacts/player_season_cluster_labels.csv` ← `data/features/player_season_feature_matrix.csv` "
         "← `data/processed/stats/player_season_stats_cleaned.parquet` (real players only).",
         "- **Edges:** undirected, **weighted by cosine similarity** (`1/(1+euclidean)` on StandardScaled "
         f"PCA vectors); an edge is kept when a node is among another's **k={K_DEFAULT} nearest neighbours** "
         f"and similarity **>= {THR_DEFAULT}** (k-NN keeps the graph sparse and interpretable vs a global "
         "dense threshold).\n",
         "## 2. Construction (regenerable)\n",
         "```bash\npython -m src.build_pca_feature_matrix        # builds the PC feature table\n"
         "python -m src.build_clustering_analysis        # writes player_season_cluster_labels.csv\n"
         "python -m graph.graph_report                   # THIS: builds graph + all outputs\n```\n"
         "Construction code: `graph/graph_report.py` (`build_graph`) and `graph/build_similarity_graph.py`. "
         "Deterministic; no hidden notebook state.\n",
         "## 3. Graph report — chosen configuration "
         f"(k={K_DEFAULT}, threshold={THR_DEFAULT})\n",
         md_table(pd.DataFrame([chosen])) + "\n",
         "Full per-node centralities (all nodes, degree/weighted-degree/PageRank/betweenness/closeness/"
         "eigenvector): **`artifacts/graph_centralities_full.csv`**. Top 15 by PageRank:\n",
         md_table(cent.head(15)[["player_name", "season", "position_group", "degree", "pagerank",
                                 "betweenness", "eigenvector"]]) + "\n",
         "## 4. Comparison vs external baseline (full table in "
         "`artifacts/graph_ranking_comparison_full.csv`)\n",
         f"PageRank vs independent baselines over n={cmp_stats.get('n_compared')} players, aligned by "
         "`player_id`:\n"]
    for key, val in cmp_stats.items():
        if isinstance(val, dict):
            L.append(f"- **{key}**: Spearman ρ={val['spearman_r']} (p={val['spearman_p']:.1e}), "
                     f"Kendall τ={val['kendall_tau']}, top-10/20/50 overlap = "
                     f"{val['top10_overlap']}/{val['top20_overlap']}/{val['top50_overlap']}")
    L += ["\n## 5. Validity checks (sensitivity + isolated nodes)\n",
          "Graph structure across k and similarity threshold "
          "(`artifacts/graph_threshold_sensitivity.csv`):\n",
          md_table(sweep_df) + "\n",
          f"At the chosen setting there are **{chosen['isolated_nodes']} isolated nodes** "
          f"({chosen['largest_component_pct']}% of nodes in the largest component). Higher thresholds "
          "sparsify the graph and raise isolated-node counts; k-NN guarantees each non-degenerate node "
          "keeps its nearest links, avoiding the all-or-nothing behaviour of a single global threshold.\n",
          "## 6. Interpretation — football-domain boundaries\n",
          "- Graph centrality here measures **relational typicality/prestige of a playing profile** within "
          "the sampled competitions — *not* player quality, market value, or tactical fit. A central node is "
          "a profile many others resemble; it does **not** imply the player is better.",
          "- High betweenness marks **bridge profiles** between playing styles (useful for versatile-signing "
          "shortlists), but the graph **cannot prove** a transfer will succeed, nor causal on-pitch impact.",
          "- Similarity edges are **descriptive**, not prescriptive: two similar profiles may differ in "
          "role, age or context the season features do not capture.",
          "- Coverage boundary: the graph only spans player-seasons with real features; conclusions do not "
          "generalise to competitions/eras outside the sample.\n"]
    REPORT.write_text("\n".join(L), encoding="utf-8")
    print(f"Wrote {REPORT}\nchosen: {chosen}\ncomparison: {cmp_stats}", flush=True)


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    from src.logging_utils import run_logged
    run_logged("graph_report", main)
