# Exact Metrics Report

Auto-generated from pipeline artifacts by `src/build_metrics_report.py`. All values are read verbatim from the real outputs.

## 1. Dataset & provenance (exact counts)

- matches: **94,525** (all observed scorelines)
- teams: **1,338** (all observed)
- goal-event rows: **75,925** (player-attributed, scoreline-anchored)
- players: **18,782** (all real identities, deduped)
- player-season rows: **52,387** | player-match rows: **1,935,463**
- real StatsBomb event-stream rows: **1,751,751**

## 2. Representation / PCA

- rows (real player-seasons): **3670**
- encoded feature count: **33**
- components for >=90% variance: **11** (cumulative variance = **0.9017**)
- PC1+PC2 explained variance: **0.4551**

First 13 components:

| component | explained_variance_ratio | cumulative_explained_variance |
| --- | --- | --- |
| PC1 | 0.2654 | 0.2654 |
| PC2 | 0.1897 | 0.4551 |
| PC3 | 0.1216 | 0.5767 |
| PC4 | 0.0967 | 0.6734 |
| PC5 | 0.0517 | 0.7251 |
| PC6 | 0.0400 | 0.7650 |
| PC7 | 0.0350 | 0.8000 |
| PC8 | 0.0301 | 0.8300 |
| PC9 | 0.0261 | 0.8561 |
| PC10 | 0.0250 | 0.8812 |
| PC11 | 0.0205 | 0.9017 |
| PC12 | 0.0184 | 0.9201 |
| PC13 | 0.0169 | 0.9369 |

## 3. Clustering

- rows: **3670** | selected K-Means k: **3** | selected DBSCAN eps/min_samples: **0.25/10**

K-Means parameter sweep:

| algorithm | k | inertia | silhouette | calinski_harabasz | davies_bouldin | cluster_count | noise_ratio |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kmeans | 2 | 4455.5815 | 0.4146 | 2374.5830 | 1.0103 | 2 | 0.0000 |
| kmeans | 3 | 2438.5568 | 0.4536 | 3685.2960 | 0.7519 | 3 | 0.0000 |
| kmeans | 4 | 1921.5029 | 0.4013 | 3445.9657 | 0.8192 | 4 | 0.0000 |
| kmeans | 5 | 1499.9788 | 0.4067 | 3567.3299 | 0.8002 | 5 | 0.0000 |
| kmeans | 6 | 1180.6981 | 0.4202 | 3822.7971 | 0.7792 | 6 | 0.0000 |
| kmeans | 7 | 1030.5097 | 0.4030 | 3737.9251 | 0.7989 | 7 | 0.0000 |
| kmeans | 8 | 918.2850 | 0.3770 | 3658.4607 | 0.8445 | 8 | 0.0000 |
| kmeans | 9 | 822.6639 | 0.3515 | 3625.4217 | 0.8896 | 9 | 0.0000 |
| kmeans | 10 | 739.9827 | 0.3628 | 3627.2112 | 0.8603 | 10 | 0.0000 |

## 4. Retrieval / ranking

- protocol: leave-one-out same-player cross-season retrieval (leakage-safe)
- player-seasons: **3670** | retained PCs: **11**

| representation | n_queries | MRR | MAP | recall@5 | recall@10 | hit@5 | hit@10 | ndcg@5 | ndcg@10 | pos_purity@5 | pos_purity@10 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline (PC1-PC2, 2 dims) | 1662 | 0.0773 | 0.0384 | 0.0316 | 0.0614 | 0.1047 | 0.1751 | 0.0304 | 0.0430 | 0.5889 | 0.5925 |
| stronger (standardized 11 PCs) | 1662 | 0.1788 | 0.0927 | 0.0939 | 0.1435 | 0.2395 | 0.3183 | 0.0953 | 0.1146 | 0.9978 | 0.9960 |

## 5. Graph analytics

- nodes: **2531** | edges: **17777** | connected components: **1** | eigenvector non-zero: **True**
- pagerank_vs_goals_per90: Spearman r=**0.0523** (p=8.5e-03), Kendall tau=**0.0387**, top50_overlap=0.0
- pagerank_vs_minutes_played: Spearman r=**0.0783** (p=8.0e-05), Kendall tau=**0.0531**, top50_overlap=0.04

Top 10 by PageRank:

| player_name | position_group | pagerank |
| --- | --- | --- |
| Fernandinho | Midfielder | 0.0014 |
| Maximilian Arnold | Midfielder | 0.0014 |
| Kyle Walker | Defender | 0.0014 |
| Dayot Upamecano | Defender | 0.0014 |
| Ben Davies | Defender | 0.0013 |
| José María Giménez | Defender | 0.0013 |
| Eric Dier | Defender | 0.0013 |
| Amadou Haidara | Midfielder | 0.0012 |
| Marcus Thuram | Forward | 0.0012 |
| Giovani Lo Celso | Midfielder | 0.0012 |

## 6. Decision layer (P2)

Optimal XI (`optimal_xi_2021-2022_4-3-3.csv`), total rating = **27.445**:

| player_name | position_group | minutes_played | rating |
| --- | --- | --- | --- |
| Benjamin Henrichs | DEF | 1143 | 3.1366 |
| Ricardo Pereira | DEF | 991 | 2.8428 |
| Alex Ferrari | DEF | 1643 | 2.7888 |
| Vladimír Coufal | DEF | 2209 | 2.7161 |
| Robert Lewandowski | FW | 2946 | 3.3866 |
| Patrik Schick | FW | 2076 | 3.1695 |
| Erling Haaland | FW | 1911 | 2.9935 |
| Mark Flekken | GK | 2880 | 0.0000 |
| Exequiel Palacios | MID | 1097 | 2.1981 |
| Kevin De Bruyne | MID | 2201 | 2.1150 |
| Domenico Berardi | MID | 2819 | 2.0981 |

Real per-player xG (`player_xg_22912.csv`), top 8:

| team | player | xg | goals | shots |
| --- | --- | --- | --- | --- |
| Liverpool | Mohamed Salah | 0.9375 | 1 | 6 |
| Tottenham Hotspur | Bamidele Alli | 0.3385 | 0 | 3 |
| Tottenham Hotspur | Lucas Rodrigues Moura da Silva | 0.2036 | 0 | 2 |
| Tottenham Hotspur | Heung-Min Son | 0.1239 | 0 | 3 |
| Liverpool | Divock Okoth Origi | 0.0780 | 1 | 1 |
| Tottenham Hotspur | Christian Dannemann Eriksen | 0.0764 | 0 | 3 |
| Liverpool | Virgil van Dijk | 0.0620 | 0 | 1 |
| Tottenham Hotspur | Jan Vertonghen | 0.0571 | 0 | 1 |

## 7. Supervised evaluation — position classification

Task: player position-group classification (GK/DEF/MID/FW) | full catalog n=3670 | real-feature subset n=1227

| model | accuracy | macro_f1 | weighted_f1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4139 | 0.1464 | 0.2424 |
| RandomForest | 0.7440 | 0.7833 | 0.7434 |
| GradientBoosting | 0.7429 | 0.7792 | 0.7431 |
| HistGradientBoosting | 0.7571 | 0.7858 | 0.7566 |


Real-feature subset (StatsBomb-covered players) — where high scores are legitimate:

| model | accuracy | macro_f1 | weighted_f1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4104 | 0.1455 | 0.2389 |
| RandomForest | 0.8436 | 0.8618 | 0.8436 |
| GradientBoosting | 0.8697 | 0.8658 | 0.8685 |
| HistGradientBoosting | 0.8730 | 0.8842 | 0.8729 |

## 8. Event-based real representation (hybrid recommender)

Real StatsBomb event-style features (with pitch location): **47** columns, 1577 player-seasons.

| same_player_recall@5 (proxy) | same_position_precision@5 (unfiltered, higher-ceiling) | position_macro_f1 |
| --- | --- | --- |
| 0.2663 | 0.8386 | 0.9291 |
