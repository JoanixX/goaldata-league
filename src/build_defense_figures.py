"""
Defense figures (P0-P2 visual evidence)
=======================================

Generates the key visuals into ``reports/figures/``:

  1. passing_network_ucl_final.png  - real passing networks (StatsBomb UCL final),
     node size = betweenness, edge width = completed passes (the tactical graph).
  2. pagerank_vs_goals.png          - similarity-graph PageRank vs an external
     baseline (goals/90), with Spearman rho for the ranking comparison.
  3. remediation_before_after.png   - before/after of the headline fixes.

Run:  python -m src.build_defense_figures
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from scipy.stats import spearmanr

from src.build_passing_network import _get, pick_match, build_team_networks, centralities

BASE = Path(__file__).resolve().parents[1]
FIG = BASE / "reports" / "figures"


def _surname(name: str) -> str:
    parts = str(name).split()
    return parts[-1] if parts else str(name)


def fig_passing_networks() -> None:
    match_id = pick_match(16)
    events = _get(f"https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/{match_id}.json")
    graphs = build_team_networks(events)
    teams = list(graphs.items())[:2]
    fig, axes = plt.subplots(1, len(teams), figsize=(15, 7))
    if len(teams) == 1:
        axes = [axes]
    for ax, (team, g) in zip(axes, teams):
        cent = centralities(g).set_index("player")
        pos = nx.spring_layout(g, seed=42, k=0.9)
        sizes = [300 + 6000 * cent.loc[n, "betweenness"] for n in g.nodes()]
        weights = [g[u][v]["weight"] for u, v in g.edges()]
        wmax = max(weights) if weights else 1
        nx.draw_networkx_edges(g, pos, ax=ax, alpha=0.25,
                               width=[0.3 + 3 * (w / wmax) for w in weights],
                               edge_color="#888", arrows=False)
        nx.draw_networkx_nodes(g, pos, ax=ax, node_size=sizes,
                               node_color=[cent.loc[n, "betweenness"] for n in g.nodes()],
                               cmap="viridis")
        nx.draw_networkx_labels(g, pos, ax=ax, font_size=8,
                                labels={n: _surname(n) for n in g.nodes()})
        ax.set_title(f"{team}\npassing network (node size = betweenness)", fontsize=11)
        ax.axis("off")
    fig.suptitle("Real passing networks - 2019 UEFA Champions League final (StatsBomb)", fontsize=13)
    fig.tight_layout()
    fig.savefig(FIG / "passing_network_ucl_final.png", dpi=130)
    plt.close(fig)
    print("  saved passing_network_ucl_final.png")


def fig_pagerank_vs_goals() -> None:
    cent = pd.read_csv(BASE / "artifacts" / "graph_centralities.csv")
    ps = pd.read_parquet(BASE / "data" / "processed" / "stats" / "player_season_stats_cleaned.parquet",
                         columns=["player_id", "goals_per90"])
    g90 = ps.groupby("player_id")["goals_per90"].mean()
    df = cent.assign(goals_per90=cent["player_id"].map(g90)).dropna(subset=["goals_per90"])
    rho, p = spearmanr(df["pagerank"], df["goals_per90"])
    colors = {"Goalkeeper": "#4c78a8", "Defender": "#f58518", "Midfielder": "#54a24b", "Forward": "#e45756"}
    fig, ax = plt.subplots(figsize=(8, 6))
    for grp, sub in df.groupby("position_group"):
        ax.scatter(sub["pagerank"], sub["goals_per90"], s=14, alpha=0.5,
                   label=grp, color=colors.get(grp, "#999"))
    ax.set_xlabel("similarity-graph PageRank")
    ax.set_ylabel("goals per 90 (external baseline)")
    ax.set_title(f"Graph ranking vs external baseline\nSpearman rho = {rho:.3f} (p = {p:.1e}, n = {len(df)})")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "pagerank_vs_goals.png", dpi=130)
    plt.close(fig)
    print("  saved pagerank_vs_goals.png")


def fig_before_after() -> None:
    panels = [
        ("PCA components for 90% var", 46, 13, ""),
        ("PCA PC1+PC2 variance", 0.2148, 0.3866, ""),
        ("Recsys MRR (stronger)", 0.0018, 0.0342, ""),
        ("Impossible GK seasons", 506, 0, ""),
    ]
    fig, axes = plt.subplots(1, 4, figsize=(16, 4))
    for ax, (title, before, after, _) in zip(axes, panels):
        bars = ax.bar(["before", "after"], [before, after], color=["#bbbbbb", "#2a9d8f"])
        ax.set_title(title, fontsize=10)
        for b, v in zip(bars, [before, after]):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height(),
                    f"{v:g}", ha="center", va="bottom", fontsize=9)
        ax.margins(y=0.2)
    fig.suptitle("Remediation impact (before -> after)", fontsize=13)
    fig.tight_layout()
    fig.savefig(FIG / "remediation_before_after.png", dpi=130)
    plt.close(fig)
    print("  saved remediation_before_after.png")


def fig_recsys_metrics() -> None:
    data = json.loads((BASE / "artifacts" / "recommendation_evaluation.json").read_text(encoding="utf-8"))
    res = {r["representation"].split(" (")[0]: r for r in data["results"]}
    # Two panels: retrieval metrics (tiny scale) and position purity (0-1 scale),
    # so the retrieval improvement is not crushed by pos_purity.
    groups = [
        ("Retrieval quality", ["MRR", "MAP", "recall@5", "recall@10", "hit@5", "ndcg@10"]),
        ("Position purity", ["pos_purity@5", "pos_purity@10"]),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), gridspec_kw={"width_ratios": [3, 1]})
    for ax, (title, metrics) in zip(axes, groups):
        x = range(len(metrics))
        ax.bar([i - 0.2 for i in x], [res["baseline"][m] for m in metrics], width=0.4,
               label="baseline (PC1-PC2)", color="#bbbbbb")
        ax.bar([i + 0.2 for i in x], [res["stronger"][m] for m in metrics], width=0.4,
               label="stronger (12 PCs, position-aware)", color="#2a9d8f")
        ax.set_xticks(list(x))
        ax.set_xticklabels(metrics, rotation=20)
        ax.set_title(title)
        ax.set_ylabel("score")
    axes[0].legend()
    fig.suptitle(f"Recommendation offline evaluation (leave-one-out, n={data['results'][0]['n_queries']} queries)")
    fig.tight_layout()
    fig.savefig(FIG / "recsys_metrics.png", dpi=130)
    plt.close(fig)
    print("  saved recsys_metrics.png")


def fig_clustering_selection() -> None:
    sweep = pd.read_csv(BASE / "artifacts" / "clustering_kmeans_sweep.csv")
    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.plot(sweep["k"], sweep["silhouette"], "o-", color="#2a9d8f", label="silhouette")
    ax1.set_xlabel("k (number of clusters)")
    ax1.set_ylabel("silhouette", color="#2a9d8f")
    best = sweep.loc[sweep["silhouette"].idxmax(), "k"]
    ax1.axvline(best, ls="--", color="#e76f51", alpha=0.7, label=f"selected k={int(best)}")
    ax2 = ax1.twinx()
    ax2.plot(sweep["k"], sweep["inertia"], "s--", color="#999", alpha=0.6, label="inertia")
    ax2.set_ylabel("inertia", color="#999")
    ax1.set_title("K-Means model selection (silhouette + inertia sweep)")
    ax1.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(FIG / "clustering_selection.png", dpi=130)
    plt.close(fig)
    print("  saved clustering_selection.png")


def fig_provenance() -> None:
    """Honest data composition: real-anchored vs simulated share per table.

    Replaces the misleading 100%-completeness charts (imputation fills every cell,
    so completeness is trivially 100% and hides what is observed vs modelled).
    """
    proc = BASE / "data" / "processed"
    players = pd.read_parquet(proc / "core" / "players_cleaned.parquet", columns=["player_id", "profile_data_source"])
    real_ids = set(players.loc[players["profile_data_source"] != "imputed_team_season_roster", "player_id"])

    def real_share(path: Path, col: str = "player_id") -> float:
        ids = pd.read_parquet(path, columns=[col])[col]
        return float(ids.isin(real_ids).mean())

    rows = [
        ("matches", 1.0, "observed"),
        ("teams", 1.0, "observed"),
        ("goals_events", 1.0, "derived from real scoreline"),
        ("players", len(real_ids) / len(players), "real profile"),
        ("player_season", real_share(proc / "stats" / "player_season_stats_cleaned.parquet"), "real player"),
        ("player_match", real_share(proc / "stats" / "player_match_stats_cleaned.parquet"), "real player"),
    ]
    labels = [r[0] for r in rows]
    real = [r[1] * 100 for r in rows]
    sim = [100 - v for v in real]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(labels, real, color="#2a9d8f", label="real / observed / derived-from-real")
    ax.barh(labels, sim, left=real, color="#bbbbbb", label="simulated from anchors")
    for i, (r, note) in enumerate(zip(real, [x[2] for x in rows])):
        ax.text(2, i, f"{r:.1f}% {note}", va="center", fontsize=8, color="white" if r > 15 else "black")
    ax.set_xlim(0, 100)
    ax.set_xlabel("% of rows")
    ax.set_title("Data composition by provenance (honest view, not raw completeness)")
    ax.legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "data_provenance_breakdown.png", dpi=130)
    plt.close(fig)
    print("  saved data_provenance_breakdown.png")


def main() -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    print("Generating defense figures...")
    fig_provenance()
    fig_before_after()
    fig_recsys_metrics()
    fig_clustering_selection()
    fig_pagerank_vs_goals()
    fig_passing_networks()
    print(f"Done -> {FIG}")


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("build_defense_figures", main)
