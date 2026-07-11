# GoalData League — Defense Brief

**Project:** Domain Discovery, Recommendation & Graph Intelligence for football
**Pipeline:** ingestion → schema → feature engineering → PCA → clustering → recommendation → graph analytics → decision layer
**Branch:** `main` · **Date:** 2026-07-11 (metrics synced to current artifacts)

---

## 1. Problem statement
A scouting & decision system over **94,525 real matches** (10+ leagues, 2004–2025),
1,338 real teams and 254,596 real goals at match level (real scorelines). It answers:
*which players are similar, which are central, and which XI to field.*

## 2. What we found and fixed (audit → remediation)
A single root cause drove almost every bad metric: **96% of "player" entities were
fabricated `Squad NN` rows** drawn from the same templates, so they were
near-identical and saturated every similarity/ranking layer; positions were also
assigned at random (a goalkeeper "scored" 3.26 goals/90). We fixed the data **at
the source** and rebuilt the downstream layers.

| Metric | Before | After |
|---|---|---|
| Analytical catalog | 190,979 (~96% synthetic) | **18,782 real** (FBref 2005-2025 + StatsBomb, deduped) |
| Impossible goalkeeper seasons | 506 (max 16.4 g/90) | **0** |
| Real position coverage | 0% (random) | **47.5% (FBref) + 100% keepers** |
| PCA components for 90% variance | 46 | **11** |
| PCA PC1+PC2 variance | 0.215 | **0.455** |
| Recommender MRR / recall@5 | 0.0018 / 0.0004 | **0.1788 / 0.0939** (×99 / ×235, real per-season FBref+StatsBomb) |
| Position classification macro-F1 | — | **0.929** with real event+pitch-location features (0.68 aggregate → 0.85 events → 0.929 rich) |
| Recommender same-position precision@5 (unfiltered, scouting) | — | **0.839** — higher-ceiling metric, not position-filtered (unlike PosPurity) |
| Recommender same-player recall@5 (low-ceiling proxy) | 0.0004 | **0.266** with real event features (×2.8 over aggregate) |
| Recommendation unit | any/old season | **current form** (latest season per player) |
| ≥1.5M dataset | 1.95M simulated player-rows | **1,751,751 REAL StatsBomb events + 1,935,463 real-roster participations** (0 invented, 0 nulls, season totals == real FBref) |
| Invented entities / duplicate identities | 199k synthetic players | **0 / 0** (real-only, identity-deduped) |
| Recommender PosPurity@5 | 0.91 (fake labels) | **0.998 (real)** |
| Graph top centrality | `Squad NN` fillers | **real stars** (Müller, Modric, Ramos) |
| Graph eigenvector | 0.0 (broken) | **non-zero** |
| Graph ranking vs **external** baseline | Spearman ≈ 0 (p=0.87), circular | **0.26 (p≈1e-70)** |

> **Exact figures for every metric below: `reports/METRICS_REPORT.md`** (auto-generated from artifacts).

## 3. Layer-by-layer evidence (figures)
- **Representation (Wk5):** `artifacts/pca_cumulative_variance.png`, `pca_player_season_2d.png` — 11 PCs reach 90.2%; clear role separation.
- **Clustering (Wk7):** `reports/figures/clustering_selection.png` (silhouette+inertia sweep, k=3 at silhouette 0.4536), `artifacts/clustering_kmeans_2d.png` — role-coherent clusters.
- **Recommendation (Wk10):** `reports/figures/recsys_metrics.png` + `reports/recommendation_evaluation_report.md` — leakage-safe leave-one-out; position-aware pool (a keeper now retrieves only keepers).
- **Graph (Wk12):** `reports/figures/pagerank_vs_goals.png` — PageRank vs an **independent** baseline (goals/90), id-aligned; one reproducible k-NN graph with exact centralities + non-zero eigenvector.
- **Decision layer:** `reports/figures/passing_network_ucl_final.png` (real StatsBomb passing networks — Alexander-Arnold & Kane as top playmakers), ILP starting-XI (`src/optimize_lineup.py`), real per-player xG (`src/build_passing_network.py`).
- **Impact summary:** `reports/figures/remediation_before_after.png`.

## 4. Data honesty (Ethics & Access)
**Commercial-grade real-only policy.** Player/team identities, participations,
goals and assists are **never simulated** — they are real. The >=1.5M dataset is a
**real StatsBomb event stream** (1,751,751 real actions across 24 real
competitions — Champions League, La Liga, World Cup, Euro, Copa América, Europa
League, Premier, Serie A, Ligue 1, Bundesliga, Copa del Rey, MLS… 2005+).
Invented `Squad NN` players and all simulated participations/goals were
**removed**; identities are de-duplicated (`Cristiano Ronaldo`/`CR7`/
`CristianoRonaldo` → one id). Verified: **0 invented, 0 duplicate identities, 0
nulls, ≥1.5M real**. Only allowed secondary metrics (touches, possession, cards,
fouls, offsides, shots where a match lacks a real source) may be modelled, via
cited formulas/imputation (Maher 1982; Dixon-Coles 1997; Decroos 2019;
Little-Rubin 2002; van Buuren 2018). Sources: StatsBomb Open Data, FBref
(`soccerdata`), real scorelines. **Limitation:** real player-level data exists for
StatsBomb-covered matches; the ~94k real-scoreline matches keep no invented
player layer.

## 5. Honest limitations
- Real-data coverage: real per-90 *style* (shots/passes/tackles/interceptions)
  is overlaid for **1,435 players** with enough StatsBomb minutes (all StatsBomb
  open-data matches ingested); the rest use real FBref season totals with a
  modelled per-match split (provenance-tagged).
- Retrieval is now strong for covered players (MRR 0.1788, recall@5 0.0939,
  recall@10 0.1435) and weaker for uncovered ones — the remaining gap is *data
  coverage* (players outside StatsBomb's competitions), not the model.
- StatsBomb season coverage is partial, so its *style rates* (per-90) are used,
  not its raw season totals (which would undercount).
- Passing networks / real xG demonstrated on representative matches (scales to
  the full StatsBomb set already ingested).

## 6. Reproducibility
```bash
python -m src.ingest_real_player_data          # real FBref multi-season (soccerdata)
python -m src.build_real_only_datasets         # real-only tables (after src.ingest_statsbomb_full)
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
- *Is the complex model better than the baseline?* → Yes on every recsys metric (×2.3 MRR, ×3.0 recall@5 vs the 2-D baseline) and the graph beats a circular comparison with a real, id-aligned external baseline (Spearman 0.26, p≈1e-70).
- *What does graph centrality mean here?* → similarity graph = "most typical profile"; passing network = "who organises play" (betweenness/eigenvector), the tactically meaningful one.
- *Biggest weakness?* → position coverage and absolute cross-season recall (§5), both addressable with broader real ingestion.
