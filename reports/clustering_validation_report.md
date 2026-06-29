# Week 7 Clustering and Validation Report

Generated at: `2026-06-29T17:50:04`

## Objective

This report satisfies the Week 7 requirement: segment the domain and validate
whether the segmentation is meaningful. The experiments use the existing
player-season PCA embedding from `artifacts/pca_player_season_2d.csv`.

## Data and Variables Used

Unit of analysis: one player in one season.

Variables used for clustering: `PC1`, `PC2`.

Metadata used only for interpretation: `player_id`, `player_name`, `season`,
and `position_group`.

Why these variables:

- `PC1` and `PC2` are numeric latent features produced by the PCA pipeline.
- PCA already combines the original football features: minutes, goals, assists,
  shots, passes, tackles, interceptions, fouls, cards, and engineered per-90
  rates.
- Clustering on PCA coordinates reduces redundancy among correlated football
  statistics and keeps the Week 7 work tied to the Week 5 representation layer.
- `player_id` and `player_name` are identifiers, so they are not valid
  clustering features.
- `season` and `position_group` are not used to force clusters; they are used
  after clustering to interpret what each cluster represents.

No football formula is inferred in this script. The methods are standard
scikit-learn algorithms and metrics.

## Method References

- `KMeans` minimizes within-cluster sum of squares, reported as inertia:
  https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
- `DBSCAN` forms density-based clusters using `eps` and `min_samples`:
  https://scikit-learn.org/stable/modules/generated/sklearn.cluster.DBSCAN.html
- `silhouette_score` compares intra-cluster distance with nearest-cluster
  distance:
  https://scikit-learn.org/stable/modules/generated/sklearn.metrics.silhouette_score.html

## Preprocessing

The PCA scores are standardized with `StandardScaler` before clustering because
K-Means and DBSCAN use Euclidean distances. Even after PCA, `PC1` and `PC2` can
have different variances; without scaling, the higher-variance component would
dominate distance calculations.

Rows with missing `PC1` or `PC2` are dropped. No values are invented.

## PCA Context

PC1 + PC2 explained variance: `0.4075`.

This means the 2D clustering is interpretable and visual, but it does not
preserve all information from the full feature matrix. That limitation is
discussed in the failure analysis.

## K-Means Parameter Sweep

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

Selected K-Means model:

- `k`: `4`
- silhouette: `0.4283`
- inertia: `3652.4432`
- Calinski-Harabasz: `7189.7339`
- Davies-Bouldin: `0.7614`

Selection rule: choose the `k` with the highest silhouette score. Inertia is
reported because it is the K-Means objective, but it always tends to decrease as
`k` increases, so it should not be used alone.

## DBSCAN Parameter Sweep

| algorithm | eps | min_samples | cluster_count | noise_count | noise_ratio | silhouette_non_noise | non_noise_count |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dbscan | 0.1500 | 5 | 3 | 55 | 0.0076 | 0.1426 | 7202 |
| dbscan | 0.1500 | 10 | 5 | 158 | 0.0218 | -0.1591 | 7099 |
| dbscan | 0.1500 | 20 | 3 | 545 | 0.0751 | -0.0289 | 6712 |
| dbscan | 0.2500 | 5 | 1 | 9 | 0.0012 | nan | 7248 |
| dbscan | 0.2500 | 10 | 2 | 21 | 0.0029 | 0.3430 | 7236 |
| dbscan | 0.2500 | 20 | 2 | 44 | 0.0061 | 0.3411 | 7213 |
| dbscan | 0.3500 | 5 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 0.3500 | 10 | 1 | 2 | 0.0003 | nan | 7255 |
| dbscan | 0.3500 | 20 | 1 | 8 | 0.0011 | nan | 7249 |
| dbscan | 0.5000 | 5 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 0.5000 | 10 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 0.5000 | 20 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 0.7500 | 5 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 0.7500 | 10 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 0.7500 | 20 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.0000 | 5 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.0000 | 10 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.0000 | 20 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.2500 | 5 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.2500 | 10 | 1 | 0 | 0.0000 | nan | 7257 |

Selected DBSCAN model:

- `eps`: `0.25`
- `min_samples`: `10`
- clusters excluding noise: `2`
- noise ratio: `0.0029`
- silhouette on non-noise points: `0.3430`

Selection rule: prefer a valid silhouette score, at least two non-noise
clusters, at least 20% of observations assigned to clusters, and noise ratio no
higher than 40%. This avoids selecting a model that looks good only because it
labels too many observations as noise.

## K-Means Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 1783 | 2.3390 | -2.4350 | 2.1712 | -2.4629 | Midfielder: 41.6%; Defender: 29.4%; Forward: 29.0% | 2021-2022: 64.6%; 2024-2025: 6.2%; 2023-2024: 4.5% | Rodrygo; Lucas Vázquez; Marcelo; Luis Díaz; Gareth Bale |
| 1 | 2489 | -1.3621 | 1.8621 | -1.3196 | 1.8533 | Midfielder: 43.1%; Defender: 41.7%; Forward: 12.1% | 2021-2022: 21.1%; 2025-2026: 19.2%; 2013-2014: 6.8% | Benjamin Mendy; Valverde; Camavinga; Asensio; Miguel Gutiérrez |
| 2 | 1665 | -2.9735 | -1.8230 | -2.9616 | -1.6257 | Goalkeeper: 32.4%; Midfielder: 25.9%; Defender: 21.6% | 2021-2022: 49.5%; 2025-2026: 21.1%; 2010-2011: 3.0% | Thibaut Courtois; Jović; E. Hazard; Ceballos; Vallejo |
| 3 | 1320 | 3.1595 | 2.0775 | 2.9850 | 2.3162 | Midfielder: 51.6%; Defender: 33.7%; Forward: 14.7% | 2021-2022: 35.5%; 2018-2019: 7.5%; 2020-2021: 7.5% | Vinícius Júnior; Benzema; Luka Modric; Éder Militão; David Alaba |

Interpretation: clusters with high positive PC1 generally represent
high-volume/high-involvement player-seasons because PC1 in the PCA report loads
strongly on minutes, matches, passing volume, defensive activity, and shot
volume. PC2 separates more attacking output because the PCA report shows strong
loadings for goals, shots on target, goals per 90, and shot efficiency.

## DBSCAN Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| -1 | 21 | -1.1255 | 0.7579 | -3.1630 | 2.2156 | Midfielder: 38.1%; Defender: 33.3%; Forward: 23.8% | 2021-2022: 61.9%; 2025-2026: 14.3%; 2011-2012: 4.8% | Pablo Sarabia; Edinson Cavani; Paco Alcácer; Mauro Icardi; Daniel Ginczek |
| 0 | 7190 | 0.0362 | 0.0286 | -0.1768 | 0.2005 | Midfielder: 40.3%; Defender: 32.7%; Forward: 18.4% | 2021-2022: 40.5%; 2025-2026: 12.1%; 2024-2025: 4.7% | Thibaut Courtois; Vinícius Júnior; Benzema; Luka Modric; Éder Militão |
| 1 | 46 | -5.1459 | -4.8228 | -5.2967 | -4.7749 | Midfielder: 43.5%; Forward: 34.8%; Defender: 13.0% | 2021-2022: 87.0%; 2025-2026: 10.9%; 2014-2015: 2.2% | Woltman; Bradley; Rúben Vinagre; Wesley; Lihadji |

DBSCAN should be interpreted as a density test rather than a replacement for
K-Means. It can identify dense regions and noise/outlier player-seasons, but it
is sensitive to the `eps` scale.

## Visual Outputs

- K-Means 2D plot: `artifacts/clustering_kmeans_2d.png`
- DBSCAN 2D plot: `artifacts/clustering_dbscan_2d.png`
- K-Means silhouette sweep: `artifacts/clustering_kmeans_silhouette.png`

## Failure Analysis and Limits

- The current clustering uses only `PC1` and `PC2`. This is useful for visual
  explanation, but PC1 + PC2 do not capture 100% of the original variance.
- K-Means assumes roughly compact, spherical clusters in Euclidean space. If
  player profiles form elongated tactical gradients, K-Means may split a
  continuum into artificial groups.
- DBSCAN is sensitive to `eps`. Small `eps` values can mark too many players as
  noise, while large `eps` values can collapse most players into one cluster.
- Position labels are partly incomplete or normalized into broad groups, so
  cluster interpretation should rely on statistical profiles and examples, not
  only position names.
- The clusters describe statistical similarity, not causal player quality.

## Work Left for Teammates

- Add football interpretation for each cluster using domain examples.
- Compare this 2D PCA clustering with clustering on the full PCA component set.
- Add slides with representative players and tactical labels.
- Expand the failure analysis with manual review of unusual/outlier players.
