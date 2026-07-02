# Exact Metrics Report (defense reference)

Auto-generated from pipeline artifacts by `src/build_metrics_report.py`. All values are read verbatim from the real outputs.

## 1. Dataset & provenance (exact counts)

_(dataset read failed: No match for FieldRef.Name(goal_id) in player_id: large_string
match_id: int64
season: large_string
competition: large_string
team: large_string
goals: int32
__fragment_index: int32
__batch_index: int32
__last_in_fragment: bool
__filename: string)_

## 2. Representation / PCA (Week 5)

- rows (real player-seasons): **7257**
- encoded feature count: **33**
- components for >=90% variance: **13** (cumulative variance = **0.9069**)
- PC1+PC2 explained variance: **0.4075**

First 13 components:

| component | explained_variance_ratio | cumulative_explained_variance |
| --- | --- | --- |
| PC1 | 0.2417 | 0.2417 |
| PC2 | 0.1658 | 0.4075 |
| PC3 | 0.1165 | 0.5240 |
| PC4 | 0.0815 | 0.6055 |
| PC5 | 0.0493 | 0.6548 |
| PC6 | 0.0432 | 0.6979 |
| PC7 | 0.0399 | 0.7379 |
| PC8 | 0.0352 | 0.7731 |
| PC9 | 0.0295 | 0.8026 |
| PC10 | 0.0290 | 0.8316 |
| PC11 | 0.0281 | 0.8597 |
| PC12 | 0.0255 | 0.8852 |
| PC13 | 0.0217 | 0.9069 |

## 3. Clustering (Week 7)

- rows: **7257** | selected K-Means k: **4** | selected DBSCAN eps/min_samples: **0.25/10**

K-Means parameter sweep:

| algorithm | k | inertia | silhouette | calinski_harabasz | davies_bouldin | cluster_count | noise_ratio |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kmeans | 2 | 8940.9752 | 0.3789 | 4522.1773 | 1.1269 | 2 | 0.0000 |
| kmeans | 3 | 5408.3080 | 0.4098 | 6106.6403 | 0.8166 | 3 | 0.0000 |
| kmeans | 4 | 3652.4432 | 0.4283 | 7189.7339 | 0.7614 | 4 | 0.0000 |
| kmeans | 5 | 3052.4471 | 0.3972 | 6807.7013 | 0.8180 | 5 | 0.0000 |
| kmeans | 6 | 2528.4911 | 0.3728 | 6874.4395 | 0.8588 | 6 | 0.0000 |
| kmeans | 7 | 2234.0614 | 0.3673 | 6641.9537 | 0.8688 | 7 | 0.0000 |
| kmeans | 8 | 1989.8530 | 0.3707 | 6517.9291 | 0.8464 | 8 | 0.0000 |
| kmeans | 9 | 1754.6959 | 0.3507 | 6588.1818 | 0.8950 | 9 | 0.0000 |
| kmeans | 10 | 1561.8566 | 0.3656 | 6677.5699 | 0.8412 | 10 | 0.0000 |

## 4. Recommendation / ranking (Week 10)

- protocol: leave-one-out same-player cross-season retrieval (leakage-safe)
- player-seasons: **7257** | retained PCs: **12**

| representation | n_queries | MRR | MAP | recall@5 | recall@10 | hit@5 | hit@10 | ndcg@5 | ndcg@10 | pos_purity@5 | pos_purity@10 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline (PC1-PC2, 2 dims) | 4438 | 0.0580 | 0.0200 | 0.0184 | 0.0290 | 0.0757 | 0.1106 | 0.0235 | 0.0259 | 0.4694 | 0.4641 |
| stronger (standardized 12 PCs) | 4438 | 0.1492 | 0.0567 | 0.0556 | 0.0762 | 0.1913 | 0.2359 | 0.0746 | 0.0754 | 0.9849 | 0.9781 |

## 5. Graph analytics (Week 12)

- nodes: **4065** | edges: **35365** | connected components: **1** | eigenvector non-zero: **True**
- pagerank_vs_goals_per90: Spearman r=**0.1764** (p=9.4e-30), Kendall tau=**0.1322**, top50_overlap=0.0
- pagerank_vs_minutes_played: Spearman r=**0.1877** (p=1.5e-33), Kendall tau=**0.1230**, top50_overlap=0.0

Top 10 by PageRank:

| player_name | position_group | pagerank |
| --- | --- | --- |
| Fernandinho | Midfielder | 0.0013 |
| Wojciech Szczesny | Goalkeeper | 0.0013 |
| Lucas Moura | Midfielder | 0.0012 |
| Raphaël Varane | Defender | 0.0011 |
| Zaid Romero | Midfielder | 0.0011 |
| Noah Jauny | Midfielder | 0.0011 |
| Younes Lachaab | Midfielder | 0.0011 |
| Luca Raimund | Midfielder | 0.0011 |
| Isaac Cossier | Midfielder | 0.0011 |
| Juan Córdoba | Midfielder | 0.0011 |

## 6. Decision layer (P2)

Optimal XI (`optimal_xi_2021-2022_4-3-3.csv`), total rating = **28.294**:

| player_name | position_group | minutes_played | rating |
| --- | --- | --- | --- |
| Dani Parejo | DEF | 1055 | 3.1984 |
| Benjamin Henrichs | DEF | 1143 | 2.9731 |
| Ricardo Pereira | DEF | 991 | 2.7384 |
| Alex Ferrari | DEF | 1643 | 2.6182 |
| Benzema | FW | 1106 | 3.5627 |
| Robert Lewandowski | FW | 2946 | 3.2293 |
| Patrik Schick | FW | 2076 | 3.0190 |
| Vlachodimos | GK | 900 | 0.4751 |
| Exequiel Palacios | MID | 1097 | 2.2360 |
| Marco Verratti | MID | 1937 | 2.1569 |
| Kevin De Bruyne | MID | 2201 | 2.0864 |

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

Task: player position-group classification (GK/DEF/MID/FW) | full catalog n=7257 | real-feature subset n=2327

| model | accuracy | macro_f1 | weighted_f1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4033 | 0.1437 | 0.2318 |
| RandomForest | 0.6474 | 0.6759 | 0.6485 |
| GradientBoosting | 0.6364 | 0.6688 | 0.6377 |


Real-feature subset (StatsBomb-covered players) — where high scores are legitimate:

| model | accuracy | macro_f1 | weighted_f1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.3952 | 0.1416 | 0.2239 |
| RandomForest | 0.7921 | 0.8154 | 0.7911 |
| GradientBoosting | 0.7887 | 0.8094 | 0.7879 |
