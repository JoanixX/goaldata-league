"""
Exact metrics report (defense reference)
========================================

Reads the real pipeline artifacts and dumps every exact number into one
Markdown file (``reports/METRICS_REPORT.md``) so each metric can be cited
verbatim during the defense. Nothing is hard-coded: values come from the JSON/CSV
the pipeline produced. Re-run after re-running the pipeline.

Run:  python -m src.build_metrics_report
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
ART = BASE / "artifacts"
OUT = BASE / "reports" / "METRICS_REPORT.md"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_(not available)_\n"
    cols = list(df.columns)
    out = ["| " + " | ".join(map(str, cols)) + " |",
           "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in df.iterrows():
        out.append("| " + " | ".join(f"{v:.4f}" if isinstance(v, float) else str(v) for v in row) + " |")
    return "\n".join(out) + "\n"


def main() -> None:
    lines: list[str] = ["# Exact Metrics Report (defense reference)\n"]
    lines.append("Auto-generated from pipeline artifacts by `src/build_metrics_report.py`. "
                 "All values are read verbatim from the real outputs.\n")

    # ---- Dataset provenance ----
    lines.append("## 1. Dataset & provenance (exact counts)\n")
    try:
        players = pd.read_parquet(BASE / "data/processed/core/players_cleaned.parquet", columns=["profile_data_source"])
        ps = pd.read_parquet(BASE / "data/processed/stats/player_season_stats_cleaned.parquet", columns=["player_id"])
        pm = pd.read_parquet(BASE / "data/processed/stats/player_match_stats_cleaned.parquet", columns=["player_id"])
        matches = pd.read_parquet(BASE / "data/processed/core/matches_cleaned.parquet", columns=["match_id"])
        teams = pd.read_parquet(BASE / "data/processed/core/teams_cleaned.parquet", columns=["team_id"])
        goals = pd.read_parquet(BASE / "data/processed/events/goals_events_cleaned.parquet", columns=["goal_id"])
        real_players = int((players["profile_data_source"] != "imputed_team_season_roster").sum())
        synth_players = int((players["profile_data_source"] == "imputed_team_season_roster").sum())
        lines.append(f"- matches: **{len(matches):,}** (all observed scorelines)")
        lines.append(f"- teams: **{len(teams):,}** (all observed)")
        lines.append(f"- goal events: **{len(goals):,}** (anchored to real scorelines)")
        lines.append(f"- players: **{len(players):,}** = {real_players:,} real + {synth_players:,} synthetic squad fillers")
        lines.append(f"- player-season rows: **{len(ps):,}** | player-match rows: **{len(pm):,}**\n")
    except Exception as exc:  # noqa
        lines.append(f"_(dataset read failed: {exc})_\n")

    # ---- PCA ----
    pca = _json(ART / "pca_feature_matrix_report.json")
    lines.append("## 2. Representation / PCA (Week 5)\n")
    if pca:
        lines.append(f"- rows (real player-seasons): **{pca.get('rows')}**")
        lines.append(f"- encoded feature count: **{pca.get('encoded_feature_count')}**")
        lines.append(f"- components for >=90% variance: **{pca.get('optimal_components_90')}** "
                     f"(cumulative variance = **{pca.get('cumulative_variance_at_optimal'):.4f}**)")
        lines.append(f"- PC1+PC2 explained variance: **{pca.get('first_two_components_variance'):.4f}**\n")
    var = _csv(ART / "pca_explained_variance.csv")
    if not var.empty:
        lines.append("First 13 components:\n")
        lines.append(_md_table(var.head(13)))

    # ---- Clustering ----
    clu = _json(ART / "clustering_validation_report.json")
    lines.append("## 3. Clustering (Week 7)\n")
    if clu:
        lines.append(f"- rows: **{clu.get('rows')}** | selected K-Means k: **{clu.get('selected_kmeans_k')}** "
                     f"| selected DBSCAN eps/min_samples: **{clu.get('selected_dbscan_eps')}/{clu.get('selected_dbscan_min_samples')}**\n")
    sweep = _csv(ART / "clustering_kmeans_sweep.csv")
    if not sweep.empty:
        lines.append("K-Means parameter sweep:\n")
        lines.append(_md_table(sweep))

    # ---- Recommendation ----
    rec = _json(ART / "recommendation_evaluation.json")
    lines.append("## 4. Recommendation / ranking (Week 10)\n")
    if rec:
        lines.append(f"- protocol: {rec.get('protocol')}")
        lines.append(f"- player-seasons: **{rec.get('player_seasons')}** | retained PCs: **{rec.get('retained_pcs')}**\n")
        rows = []
        for r in rec.get("results", []):
            rows.append({k: r[k] for k in ["representation", "n_queries", "MRR", "MAP", "recall@5",
                                            "recall@10", "hit@5", "hit@10", "ndcg@5", "ndcg@10",
                                            "pos_purity@5", "pos_purity@10"] if k in r})
        lines.append(_md_table(pd.DataFrame(rows)))

    # ---- Graph ----
    g = _json(ART / "graph_statistics.json")
    lines.append("## 5. Graph analytics (Week 12)\n")
    if g:
        lines.append(f"- nodes: **{g.get('num_nodes')}** | edges: **{g.get('num_edges')}** "
                     f"| connected components: **{g.get('num_connected_components')}** "
                     f"| eigenvector non-zero: **{g.get('eigenvector_nonzero')}**")
        cmp = g.get("ranking_comparison_vs_external_baseline", {})
        for key, val in cmp.items():
            if isinstance(val, dict):
                lines.append(f"- {key}: Spearman r=**{val.get('spearman_r'):.4f}** (p={val.get('spearman_p'):.1e}), "
                             f"Kendall tau=**{val.get('kendall_tau'):.4f}**, "
                             f"top50_overlap={val.get('top50_overlap')}")
        top = g.get("top_10_by_pagerank", [])
        if top:
            lines.append("\nTop 10 by PageRank:\n")
            lines.append(_md_table(pd.DataFrame(top)))

    # ---- Decision layer ----
    lines.append("## 6. Decision layer (P2)\n")
    xi_files = sorted(ART.glob("optimal_xi_*.csv"))
    if xi_files:
        xi = _csv(xi_files[-1])
        lines.append(f"Optimal XI (`{xi_files[-1].name}`), total rating = **{xi['rating'].sum():.3f}**:\n")
        lines.append(_md_table(xi))
    xg_files = sorted(ART.glob("player_xg_*.csv"))
    if xg_files:
        xg = _csv(xg_files[-1])
        lines.append(f"Real per-player xG (`{xg_files[-1].name}`), top 8:\n")
        lines.append(_md_table(xg.head(8)))

    # ---- Supervised evaluation ----
    sup = _json(ART / "supervised_evaluation.json")
    lines.append("## 7. Supervised evaluation — position classification\n")
    if sup:
        lines.append(f"Task: {sup.get('task')} | full catalog n={sup.get('n_full')} | "
                     f"real-feature subset n={sup.get('n_real_subset')}\n")
        rows = [{"model": k, **v} for k, v in sup.get("results", {}).items()]
        lines.append(_md_table(pd.DataFrame(rows)))
        rr = sup.get("results_real_subset")
        if rr:
            lines.append("\nReal-feature subset (StatsBomb-covered players) — where high scores are legitimate:\n")
            lines.append(_md_table(pd.DataFrame([{"model": k, **v} for k, v in rr.items()])))

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("build_metrics_report", main)
