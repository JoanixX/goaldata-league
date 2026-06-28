# Week 7 Clustering and Validation Report

Generated at: `2026-06-27T17:02:10`

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

PC1 + PC2 explained variance: `0.3989`.

This means the 2D clustering is interpretable and visual, but it does not
preserve all information from the full feature matrix. That limitation is
discussed in the failure analysis.

## K-Means Parameter Sweep

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

Selected K-Means model:

- `k`: `3`
- silhouette: `0.4475`
- inertia: `4940.0463`
- Calinski-Harabasz: `7029.2621`
- Davies-Bouldin: `0.7717`

Selection rule: choose the `k` with the highest silhouette score. Inertia is
reported because it is the K-Means objective, but it always tends to decrease as
`k` increases, so it should not be used alone.

## DBSCAN Parameter Sweep

| algorithm | eps | min_samples | cluster_count | noise_count | noise_ratio | silhouette_non_noise | non_noise_count |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dbscan | 0.1500 | 5 | 6 | 99 | 0.0136 | 0.0326 | 7158 |
| dbscan | 0.1500 | 10 | 3 | 234 | 0.0322 | 0.2453 | 7023 |
| dbscan | 0.1500 | 20 | 8 | 605 | 0.0834 | 0.0056 | 6652 |
| dbscan | 0.2500 | 5 | 1 | 20 | 0.0028 | nan | 7237 |
| dbscan | 0.2500 | 10 | 2 | 38 | 0.0052 | 0.2468 | 7219 |
| dbscan | 0.2500 | 20 | 2 | 106 | 0.0146 | 0.3322 | 7151 |
| dbscan | 0.3500 | 5 | 1 | 7 | 0.0010 | nan | 7250 |
| dbscan | 0.3500 | 10 | 1 | 12 | 0.0017 | nan | 7245 |
| dbscan | 0.3500 | 20 | 1 | 29 | 0.0040 | nan | 7228 |
| dbscan | 0.5000 | 5 | 1 | 3 | 0.0004 | nan | 7254 |
| dbscan | 0.5000 | 10 | 1 | 4 | 0.0006 | nan | 7253 |
| dbscan | 0.5000 | 20 | 1 | 6 | 0.0008 | nan | 7251 |
| dbscan | 0.7500 | 5 | 1 | 1 | 0.0001 | nan | 7256 |
| dbscan | 0.7500 | 10 | 1 | 1 | 0.0001 | nan | 7256 |
| dbscan | 0.7500 | 20 | 1 | 2 | 0.0003 | nan | 7255 |
| dbscan | 1.0000 | 5 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.0000 | 10 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.0000 | 20 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.2500 | 5 | 1 | 0 | 0.0000 | nan | 7257 |
| dbscan | 1.2500 | 10 | 1 | 0 | 0.0000 | nan | 7257 |

Selected DBSCAN model:

- `eps`: `0.25`
- `min_samples`: `20`
- clusters excluding noise: `2`
- noise ratio: `0.0146`
- silhouette on non-noise points: `0.3322`

Selection rule: prefer a valid silhouette score, at least two non-noise
clusters, at least 20% of observations assigned to clusters, and noise ratio no
higher than 40%. This avoids selecting a model that looks good only because it
labels too many observations as noise.

## K-Means Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 2514 | 3.0460 | 0.7010 | 2.9544 | 0.5314 | Midfielder: 43.8%; Defender: 33.5%; Forward: 19.5% | 2025-2026: 17.8%; 2021-2022: 17.0%; 2011-2012: 7.6% | Benzema; Luka Modric; Éder Militão; Carvajal; Casemiro |
| 1 | 2762 | -2.5251 | 1.4204 | -2.6885 | 1.1668 | Midfielder: 44.2%; Defender: 33.4%; Forward: 19.3% | 2021-2022: 57.4%; 2024-2025: 7.4%; 2018-2019: 6.2% | Thibaut Courtois; Vinícius Júnior; David Alaba; Nacho; Rodrygo |
| 2 | 1981 | -0.3449 | -2.8700 | -0.5218 | -2.5202 | Midfielder: 30.6%; Defender: 30.4%; Goalkeeper: 22.7% | 2021-2022: 48.2%; 2025-2026: 21.7%; 2010-2011: 3.4% | E. Hazard; Miguel Gutiérrez; Ceballos; Vallejo; Gareth Bale |

Interpretation: clusters with high positive PC1 generally represent
high-volume/high-involvement player-seasons because PC1 in the PCA report loads
strongly on minutes, matches, passing volume, defensive activity, and shot
volume. PC2 separates more attacking output because the PCA report shows strong
loadings for goals, shots on target, goals per 90, and shot efficiency.

## DBSCAN Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| -1 | 106 | 4.3331 | -0.4927 | 5.5734 | -0.8949 | Midfielder: 53.8%; Forward: 27.4%; Defender: 17.9% | 2021-2022: 48.1%; 2025-2026: 8.5%; 2015-2016: 6.6% | Vinícius Júnior; Toni Kroos; Benjamin Mendy; Blanco; Mané |
| 0 | 7107 | -0.0464 | 0.0431 | -0.4342 | 0.1161 | Midfielder: 40.2%; Defender: 32.9%; Forward: 18.3% | 2021-2022: 40.5%; 2025-2026: 12.1%; 2024-2025: 4.7% | Thibaut Courtois; Benzema; Luka Modric; Éder Militão; David Alaba |
| 1 | 44 | -2.9432 | -5.7680 | -2.9454 | -5.9507 | Midfielder: 40.9%; Forward: 34.1%; Defender: 15.9% | 2021-2022: 88.6%; 2025-2026: 11.4% | Woltman; Bradley; Wesley; Lihadji; Heaton |

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
