# Exact Metrics Report (defense reference)

Auto-generated from pipeline artifacts by `src/build_metrics_report.py`. All values are read verbatim from the real outputs.

## 1. Dataset & provenance (exact counts)

- matches: **94,525** (all observed scorelines)
- teams: **1,538** (all observed)
- goal events: **254,596** (anchored to real scorelines)
- players: **206,151** = 7,121 real + 199,030 synthetic squad fillers
- player-season rows: **191,150** | player-match rows: **1,950,578**

## 2. Representation / PCA (Week 5)

- rows (real player-seasons): **7257**
- encoded feature count: **33**
- components for >=90% variance: **14** (cumulative variance = **0.9052**)
- PC1+PC2 explained variance: **0.3989**

First 13 components:

| component | explained_variance_ratio | cumulative_explained_variance |
| --- | --- | --- |
| PC1 | 0.2259 | 0.2259 |
| PC2 | 0.1730 | 0.3989 |
| PC3 | 0.1067 | 0.5056 |
| PC4 | 0.0646 | 0.5701 |
| PC5 | 0.0495 | 0.6196 |
| PC6 | 0.0434 | 0.6630 |
| PC7 | 0.0417 | 0.7047 |
| PC8 | 0.0378 | 0.7426 |
| PC9 | 0.0342 | 0.7768 |
| PC10 | 0.0310 | 0.8078 |
| PC11 | 0.0289 | 0.8367 |
| PC12 | 0.0240 | 0.8607 |
| PC13 | 0.0223 | 0.8830 |

## 3. Clustering (Week 7)

- rows: **7257** | selected K-Means k: **3** | selected DBSCAN eps/min_samples: **0.25/20**

K-Means parameter sweep:

| algorithm | k | inertia | silhouette | calinski_harabasz | davies_bouldin | cluster_count | noise_ratio |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kmeans | 2 | 8715.2132 | 0.4110 | 4827.2335 | 1.0137 | 2 | 0.0000 |
| kmeans | 3 | 4940.0463 | 0.4475 | 7029.2621 | 0.7717 | 3 | 0.0000 |
| kmeans | 4 | 3842.1818 | 0.4190 | 6715.2920 | 0.7973 | 4 | 0.0000 |
| kmeans | 5 | 2845.0822 | 0.4061 | 7436.0673 | 0.8288 | 5 | 0.0000 |
| kmeans | 6 | 2351.1342 | 0.4021 | 7502.2340 | 0.7919 | 6 | 0.0000 |
| kmeans | 7 | 2008.6875 | 0.3795 | 7522.6421 | 0.8412 | 7 | 0.0000 |
| kmeans | 8 | 1791.1233 | 0.3692 | 7356.0445 | 0.8524 | 8 | 0.0000 |
| kmeans | 9 | 1627.2184 | 0.3665 | 7175.2197 | 0.8612 | 9 | 0.0000 |
| kmeans | 10 | 1481.7234 | 0.3734 | 7082.4491 | 0.8331 | 10 | 0.0000 |

## 4. Recommendation / ranking (Week 10)

- protocol: leave-one-out same-player cross-season retrieval (leakage-safe)
- player-seasons: **7257** | retained PCs: **12**

| representation | n_queries | MRR | MAP | recall@5 | recall@10 | hit@5 | hit@10 | ndcg@5 | ndcg@10 | pos_purity@5 | pos_purity@10 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline (PC1-PC2, 2 dims) | 4438 | 0.0143 | 0.0050 | 0.0033 | 0.0049 | 0.0137 | 0.0225 | 0.0038 | 0.0042 | 0.3909 | 0.3883 |
| stronger (standardized 12 PCs) | 4438 | 0.0422 | 0.0138 | 0.0110 | 0.0194 | 0.0554 | 0.0872 | 0.0146 | 0.0165 | 0.9886 | 0.9833 |

## 5. Graph analytics (Week 12)

- nodes: **4065** | edges: **37969** | connected components: **1** | eigenvector non-zero: **True**
- pagerank_vs_goals_per90: Spearman r=**0.2123** (p=1.2e-42), Kendall tau=**0.1591**, top50_overlap=0.0
- pagerank_vs_minutes_played: Spearman r=**0.2848** (p=9.8e-77), Kendall tau=**0.1847**, top50_overlap=0.0

Top 10 by PageRank:

| player_name | position_group | pagerank |
| --- | --- | --- |
| Mats Seiler | Midfielder | 0.0011 |
| Fran González | Midfielder | 0.0011 |
| Francisco Silva | Midfielder | 0.0011 |
| Kjell Wätjen | Midfielder | 0.0011 |
| Dennis Seimen | Midfielder | 0.0011 |
| Francis Onyeka | Midfielder | 0.0011 |
| Miguel Chaiwa | Midfielder | 0.0011 |
| Noah Jauny | Midfielder | 0.0011 |
| Gianluca Prestianni | Midfielder | 0.0011 |
| Nicolás Otamendi | Defender | 0.0011 |

## 6. Decision layer (P2)

Optimal XI (`optimal_xi_2021-2022_4-3-3.csv`), total rating = **42.816**:

| player_name | position_group | minutes_played | rating |
| --- | --- | --- | --- |
| Jan Vertonghen | DEF | 900 | 7.1712 |
| Carvajal | DEF | 959 | 7.1148 |
| Dani Parejo | DEF | 1055 | 6.9211 |
| Oleksandr Zinchenko | DEF | 1047 | 4.1995 |
| Wissam Ben Yedder | FW | 2529 | 2.9902 |
| Max Kruse | FW | 2398 | 1.9903 |
| Martin Terrier | FW | 2779 | 1.9239 |
| Vlachodimos | GK | 900 | 0.1389 |
| Capoue | MID | 1046 | 4.9058 |
| Thiago Alcantara | MID | 1534 | 2.7356 |
| Luka Modric | MID | 2032 | 2.7248 |

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
