# GoalData League — System Brief

**Scope:** domain discovery, retrieval and graph intelligence for football scouting
**Pipeline:** ingestion → schema → feature engineering → PCA → clustering → retrieval → graph analytics → decision layer

---

## 1. What the system does

A scouting and decision system over 94,525 matches (10+ leagues, 2004-2025), 1,338 teams
and 254,596 match-level goals. It answers three questions: which players are similar,
which are central, and which XI to field.

## 2. Layer-by-layer evidence

- **Representation:** `artifacts/pca_cumulative_variance.png`, `artifacts/pca_player_season_2d.png` — 11 principal components reach 90.2% of the variance, with clear role separation.
- **Clustering:** `reports/figures/clustering_selection.png` (silhouette and inertia sweep, k=3 at silhouette 0.4536), `artifacts/clustering_kmeans_2d.png` — role-coherent clusters.
- **Retrieval:** `reports/figures/recsys_metrics.png` and `reports/recommendation_evaluation_report.md` — leakage-safe leave-one-out evaluation over a position-aware candidate pool, so a keeper retrieves only keepers.
- **Graph:** `reports/figures/pagerank_vs_goals.png` — PageRank against an independent baseline (goals/90), id-aligned, on one reproducible k-NN graph with exact centralities.
- **Decision layer:** `reports/figures/passing_network_ucl_final.png` (StatsBomb passing networks, with Alexander-Arnold and Kane as top playmakers), ILP starting XI (`src/optimize_lineup.py`), per-player xG (`src/build_passing_network.py`).

Exact figures for every metric: `reports/METRICS_REPORT.md`, auto-generated from the artifacts.

## 3. Data and provenance

Sources are StatsBomb Open Data (1,751,751 event-stream actions across 24 competitions from
2005 onward, including the Champions League, La Liga, the World Cup, the Euro, Copa América,
the Europa League, the Premier League, Serie A, Ligue 1, the Bundesliga, the Copa del Rey and
MLS), FBref season totals and rosters via `soccerdata`, Understat per-match records, and
published scorelines. Identities are deduplicated to canonical ids.

Every row carries a `data_provenance` tag. Identities, participations, goals and assists are
taken from these sources; secondary metrics (touches, possession, cards, fouls, offsides, and
shots where a match has no event-level source) are modelled from cited formulas and imputation
(Maher 1982; Dixon & Coles 1997; Decroos 2019; Little & Rubin 2002; van Buuren 2018) and
tagged as such. Full detail: `reports/methodology_and_citations.md`.

## 4. Limitations

- Per-90 style features (shots, passes, tackles, interceptions) are observed for 1,435 players
  with enough StatsBomb minutes; the rest use FBref season totals with a modelled per-match
  split, tagged in `data_provenance`.
- Retrieval is strongest for event-covered players (MRR 0.1788, recall@5 0.0939, recall@10
  0.1435) and weaker for the rest. The remaining gap is data coverage — players outside
  StatsBomb's competitions — rather than the model.
- StatsBomb season coverage is partial, so its per-90 style rates are used rather than its raw
  season totals, which would undercount.
- Player-level detail exists only for event-covered matches; the wider scoreline-only match set
  carries no player layer.
- Passing networks and per-player xG are demonstrated on representative matches, and scale to
  the full ingested StatsBomb set.

## 5. Reproducibility

```bash
python -m src.ingest_real_player_data          # FBref multi-season rosters (soccerdata)
python -m src.build_real_only_datasets         # core tables (after src.ingest_statsbomb_full)
python -m src.build_pca_feature_matrix         # PCA
python -m src.build_clustering_analysis        # clustering
python -m src.recommendation_evaluation        # retrieval metrics
python graph/build_similarity_graph.py --input-csv artifacts/player_season_cluster_labels.csv --out-graph data/processed/similarity_graph.pkl --k-neighbors 8 --threshold 0.15
python graph/analyze_similarity_graph.py       # exact centralities + external baseline
python -m src.optimize_lineup --season 2021-2022 --formation 4-3-3   # ILP XI
python -m src.build_passing_network            # passing network + xG
python -m src.build_figures                    # figures
```

## 6. Design notes

- **Retrieval vs baseline.** The full standardized PC representation beats the 2-component
  baseline on every metric (MRR ×2.3, recall@5 ×3.0). See `reports/recommendation_evaluation_report.md`.
- **Graph vs external baseline.** Similarity-graph centrality is compared against an
  independent, id-aligned baseline (goals/90) rather than against itself: Spearman 0.26,
  p≈1e-70.
- **What centrality means here.** In the similarity graph it identifies the most typical
  profile; in the passing network, betweenness and eigenvector identify who organizes play,
  which is the tactically meaningful reading.
