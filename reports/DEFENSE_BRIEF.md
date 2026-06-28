# GoalData League — Defense Brief

**Project:** Domain Discovery, Recommendation & Graph Intelligence for football
**Pipeline:** ingestion → schema → feature engineering → PCA → clustering → recommendation → graph analytics → decision layer
**Branch:** `joaquincito_branch2` · **Date:** 2026-06-27

---

## 1. Problem statement
A scouting & decision system over **94,525 real matches** (10+ leagues, 2004–2025),
1,538 real teams and 254,596 real-scoreline-anchored goals. It answers: *which
players are similar, which are central, and which XI to field.*

## 2. What we found and fixed (audit → remediation)
A single root cause drove almost every bad metric: **96% of "player" entities were
fabricated `Squad NN` rows** drawn from the same templates, so they were
near-identical and saturated every similarity/ranking layer; positions were also
assigned at random (a goalkeeper "scored" 3.26 goals/90). We fixed the data **at
the source** and rebuilt the downstream layers.

| Metric | Before | After |
|---|---|---|
| Analytical catalog | 190,979 (~96% synthetic) | **7,086 real** |
| Impossible goalkeeper seasons | 506 (max 16.4 g/90) | **0** |
| Real position coverage | 0% (random) | **47.5% (FBref) + 100% keepers** |
| PCA components for 90% variance | 46 | **13** |
| PCA PC1+PC2 variance | 0.215 | **0.387** |
| Recommender MRR / recall@5 | 0.0018 / 0.0004 | **0.0419 / 0.0115** (×23 / ×29, real-stats overlay) |
| Recommender PosPurity@5 | 0.91 (fake labels) | **0.996 (real)** |
| Graph top centrality | `Squad NN` fillers | **real stars** (Müller, Modric, Ramos) |
| Graph eigenvector | 0.0 (broken) | **non-zero** |
| Graph ranking vs **external** baseline | Spearman ≈ 0 (p=0.87), circular | **0.26 (p≈1e-70)** |

> **Exact figures for every metric below: `reports/METRICS_REPORT.md`** (auto-generated from artifacts).

## 3. Layer-by-layer evidence (figures)
- **Representation (Wk5):** `artifacts/pca_cumulative_variance.png`, `pca_player_season_2d.png` — 13 PCs reach 90%; clear role separation.
- **Clustering (Wk7):** `reports/figures/clustering_selection.png` (silhouette+inertia sweep, k=3 at silhouette 0.503), `artifacts/clustering_kmeans_2d.png` — role-coherent clusters.
- **Recommendation (Wk10):** `reports/figures/recsys_metrics.png` + `reports/recommendation_evaluation_report.md` — leakage-safe leave-one-out; position-aware pool (a keeper now retrieves only keepers).
- **Graph (Wk12):** `reports/figures/pagerank_vs_goals.png` — PageRank vs an **independent** baseline (goals/90), id-aligned; one reproducible k-NN graph with exact centralities + non-zero eigenvector.
- **Decision layer:** `reports/figures/passing_network_ucl_final.png` (real StatsBomb passing networks — Alexander-Arnold & Kane as top playmakers), ILP starting-XI (`src/optimize_lineup.py`), real per-player xG (`src/build_passing_network.py`).
- **Impact summary:** `reports/figures/remediation_before_after.png`.

## 4. Data honesty (Ethics & Access)
Sources: real match scorelines, FBref (committed 2021-22 + 7 ingested Big-5 seasons
via `soccerdata`), Transfermarkt profiles, StatsBomb Open Data (free, citable).
Every table carries a `data_provenance` column (`observed`/`derived`/`simulated`,
per-table/column). Per-match counts with no real granular source are simulated
**from real anchors** with cited models (Maher 1982; Dixon-Coles 1997; Decroos
2019; Little-Rubin 2002; van Buuren 2018), never unconditioned random fill.

## 5. Honest limitations
- ~52% of real players (outside FBref Big-5 2018-25 / pre-2018) keep their prior
  position label — widen with StatsBomb / more FBref leagues.
- Cross-season same-player retrieval is still low in absolute terms (hard with
  static profiles); ×19 over baseline, would benefit from time-aware models.
- Passing networks / real xG demonstrated on one match (scales to more StatsBomb).
- ~15.6k season count fields stored as floats (round in a future pass).

## 6. Reproducibility
```bash
python -m src.ingest_real_player_data          # real FBref multi-season (soccerdata)
python -m src.rebuild_realistic_datasets       # real positions + scoreline-anchored goals
python -m src.build_pca_feature_matrix         # PCA (real-only catalog)
python -m src.build_clustering_analysis        # clustering
python -m src.recommendation_evaluation        # recsys metrics
python graph/build_similarity_graph.py --input-csv artifacts/player_season_cluster_labels.csv --out-graph data/processed/similarity_graph.pkl --k-neighbors 8 --threshold 0.15
python graph/analyze_similarity_graph.py       # exact centralities + external baseline
python -m src.optimize_lineup --season 2021-2022 --formation 4-3-3   # ILP XI
python -m src.build_passing_network            # passing network + real xG
python -m src.build_defense_figures            # figures
```

## 7. Likely defense questions
- *Which data is real vs modelled?* → §4; matches/teams/scorelines/positions(47.5%)/xG real; per-match counts simulated-from-anchors and tagged.
- *Is the complex model better than the baseline?* → Yes on every recsys metric (×19 MRR) and the graph beats a circular comparison with a real, id-aligned external baseline (Spearman 0.26, p≈1e-70).
- *What does graph centrality mean here?* → similarity graph = "most typical profile"; passing network = "who organises play" (betweenness/eigenvector), the tactically meaningful one.
- *Biggest weakness?* → position coverage and absolute cross-season recall (§5), both addressable with broader real ingestion.
