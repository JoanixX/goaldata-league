# Week-12 Graph Analytics Report (auditable, real-only)

## 1. Graph definition

- **Grain:** one **player-similarity** graph. **Node = one real player-season** (key `player_id` + `season`; label `player_name`). No team-match grain is used here.
- **Node features:** retained PCA components `PC1, PC2, PC3, PC4, PC5, PC6, PC7, PC8, PC9, PC10, PC11` of the player-season, from `artifacts/player_season_cluster_labels.csv` ← `data/features/player_season_feature_matrix.csv` ← `data/processed/stats/player_season_stats_cleaned.parquet` (real players only).
- **Edges:** undirected, **weighted by cosine similarity** (`1/(1+euclidean)` on StandardScaled PCA vectors); an edge is kept when a node is among another's **k=8 nearest neighbours** and similarity **>= 0.15** (k-NN keeps the graph sparse and interpretable vs a global dense threshold).

## 2. Construction (regenerable)

```bash
python -m src.build_pca_feature_matrix        # builds the PC feature table
python -m src.build_clustering_analysis        # writes player_season_cluster_labels.csv
python -m graph.graph_report                   # THIS: builds graph + all outputs
```
Construction code: `graph/graph_report.py` (`build_graph`) and `graph/build_similarity_graph.py`. Deterministic; no hidden notebook state.

## 3. Graph report — chosen configuration (k=8, threshold=0.15)

| nodes | edges | density | components | largest_component_pct | isolated_nodes | mean_degree | median_degree |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3670 | 20620 | 0.0030627 | 1 | 100 | 0 | 11.237 | 11 |

Full per-node centralities (all nodes, degree/weighted-degree/PageRank/betweenness/closeness/eigenvector): **`artifacts/graph_centralities_full.csv`**. Top 15 by PageRank:

| player_name | season | position_group | degree | pagerank | betweenness | eigenvector |
| --- | --- | --- | --- | --- | --- | --- |
| Geoffrey Kondogbia | 2019-2020 | Midfielder | 25 | 0.0005639 | 0.0022917 | 0 |
| David Silva | 2021-2022 | Midfielder | 24 | 0.00056056 | 0.00046074 | 0 |
| Ivan Rakitić | 2018-2019 | Midfielder | 23 | 0.00053853 | 0.0011821 | 0 |
| Odsonne Édouard | 2021-2022 | Forward | 23 | 0.00052995 | 0.00095389 | 0 |
| Kevin Bonifazi | 2021-2022 | Defender | 22 | 0.00052944 | 0.0014523 | 0 |
| Martin Ødegaard | 2021-2022 | Midfielder | 23 | 0.0005188 | 0.0013323 | 0 |
| Ozan Kabak | 2021-2022 | Defender | 22 | 0.00051755 | 0.001558 | 1e-06 |
| Jean-Victor Makengo | 2021-2022 | Midfielder | 22 | 0.00051735 | 0.00059684 | 0 |
| Kike | 2021-2022 | Forward | 22 | 0.00051067 | 0.00066029 | 0 |
| Arnaud Souquet | 2021-2022 | Defender | 22 | 0.00051013 | 0.0024613 | 0 |
| Vinícius Júnior | 2020-2021 | Forward | 22 | 0.00050644 | 0.0037351 | 0 |
| Souleyman Doumbia | 2021-2022 | Defender | 22 | 0.00050504 | 0.0037068 | 0 |
| Junior Sambia | 2021-2022 | Midfielder | 21 | 0.00050445 | 0.00050579 | 0 |
| Leroy Sané | 2020-2021 | Midfielder | 21 | 0.00049484 | 0.0015801 | 0 |
| Franco Cervi | 2021-2022 | Midfielder | 22 | 0.00048983 | 0.0043651 | 0 |

## 4. Comparison vs external baseline (full table in `artifacts/graph_ranking_comparison_full.csv`)

PageRank vs independent baselines over n=3670 players, aligned by `player_id`:

- **pagerank_vs_goals_per90**: Spearman ρ=-0.0166 (p=3.1e-01), Kendall τ=-0.0115, top-10/20/50 overlap = 0.0/0.0/0.02
- **pagerank_vs_minutes_played**: Spearman ρ=0.0805 (p=1.1e-06), Kendall τ=0.0543, top-10/20/50 overlap = 0.0/0.0/0.0

## 5. Validity checks (sensitivity + isolated nodes)

Graph structure across k and similarity threshold (`artifacts/graph_threshold_sensitivity.csv`):

| k | threshold | nodes | edges | density | components | largest_component_pct | isolated_nodes | mean_degree |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 5 | 0.1 | 3670 | 12998 | 0.0019306 | 2 | 99.56 | 0 | 7.0834 |
| 5 | 0.15 | 3670 | 12998 | 0.0019306 | 2 | 99.56 | 0 | 7.0834 |
| 5 | 0.2 | 3670 | 12967 | 0.001926 | 2 | 99.56 | 0 | 7.0665 |
| 5 | 0.3 | 3670 | 12284 | 0.0018246 | 91 | 85.37 | 78 | 6.6943 |
| 8 | 0.1 | 3670 | 20620 | 0.0030627 | 1 | 100 | 0 | 11.237 |
| 8 | 0.15 | 3670 | 20620 | 0.0030627 | 1 | 100 | 0 | 11.237 |
| 8 | 0.2 | 3670 | 20535 | 0.0030501 | 1 | 100 | 0 | 11.191 |
| 8 | 0.3 | 3670 | 19093 | 0.0028359 | 91 | 85.37 | 78 | 10.405 |
| 12 | 0.1 | 3670 | 30751 | 0.0045675 | 1 | 100 | 0 | 16.758 |
| 12 | 0.15 | 3670 | 30751 | 0.0045675 | 1 | 100 | 0 | 16.758 |
| 12 | 0.2 | 3670 | 30560 | 0.0045391 | 1 | 100 | 0 | 16.654 |
| 12 | 0.3 | 3670 | 27722 | 0.0041176 | 90 | 85.37 | 78 | 15.107 |

At the chosen setting there are **0 isolated nodes** (100.0% of nodes in the largest component). Higher thresholds sparsify the graph and raise isolated-node counts; k-NN guarantees each non-degenerate node keeps its nearest links, avoiding the all-or-nothing behaviour of a single global threshold.

## 6. Interpretation — football-domain boundaries

- Graph centrality here measures **relational typicality/prestige of a playing profile** within the sampled competitions — *not* player quality, market value, or tactical fit. A central node is a profile many others resemble; it does **not** imply the player is better.
- High betweenness marks **bridge profiles** between playing styles (useful for versatile-signing shortlists), but the graph **cannot prove** a transfer will succeed, nor causal on-pitch impact.
- Similarity edges are **descriptive**, not prescriptive: two similar profiles may differ in role, age or context the season features do not capture.
- Coverage boundary: the graph only spans player-seasons with real features; conclusions do not generalise to competitions/eras outside the sample.
