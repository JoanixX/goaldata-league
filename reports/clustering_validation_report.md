# Week 7 Clustering and Validation Report

Generated at: `2026-06-13T10:36:53`

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

PC1 + PC2 explained variance: `0.2148`.

This means the 2D clustering is interpretable and visual, but it does not
preserve all information from the full feature matrix. That limitation is
discussed in the failure analysis.

## K-Means Parameter Sweep

| algorithm | k | inertia | silhouette | calinski_harabasz | davies_bouldin | cluster_count | noise_ratio |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kmeans | 2 | 234502.4663 | 0.3977 | 120087.1105 | 1.1271 | 2 | 0.0000 |
| kmeans | 3 | 134239.4421 | 0.4351 | 176212.3710 | 0.7902 | 3 | 0.0000 |
| kmeans | 4 | 91757.6349 | 0.4463 | 201332.9577 | 0.7644 | 4 | 0.0000 |
| kmeans | 5 | 71093.6330 | 0.4412 | 208765.0495 | 0.7727 | 5 | 0.0000 |
| kmeans | 6 | 59451.8018 | 0.4265 | 207196.8905 | 0.8532 | 6 | 0.0000 |
| kmeans | 7 | 49487.5430 | 0.4455 | 213836.1878 | 0.7800 | 7 | 0.0000 |
| kmeans | 8 | 44264.4445 | 0.4346 | 208135.9705 | 0.7940 | 8 | 0.0000 |
| kmeans | 9 | 40159.9030 | 0.4223 | 203171.0680 | 0.8259 | 9 | 0.0000 |
| kmeans | 10 | 37145.3675 | 0.3998 | 196972.7307 | 0.8628 | 10 | 0.0000 |

Selected K-Means model:

- `k`: `4`
- silhouette: `0.4463`
- inertia: `91757.6349`
- Calinski-Harabasz: `201332.9577`
- Davies-Bouldin: `0.7644`

Selection rule: choose the `k` with the highest silhouette score. Inertia is
reported because it is the K-Means objective, but it always tends to decrease as
`k` increases, so it should not be used alone.

## DBSCAN Parameter Sweep

| algorithm | eps | min_samples | cluster_count | noise_count | noise_ratio | silhouette_non_noise | non_noise_count |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dbscan | 0.1500 | 5 | 4 | 53 | 0.0027 | 0.2548 | 19947 |
| dbscan | 0.1500 | 10 | 2 | 139 | 0.0069 | 0.4162 | 19861 |
| dbscan | 0.1500 | 20 | 1 | 433 | 0.0216 | nan | 19567 |
| dbscan | 0.2500 | 5 | 1 | 10 | 0.0005 | nan | 19990 |
| dbscan | 0.2500 | 10 | 1 | 24 | 0.0012 | nan | 19976 |
| dbscan | 0.2500 | 20 | 1 | 78 | 0.0039 | nan | 19922 |
| dbscan | 0.3500 | 5 | 1 | 1 | 0.0001 | nan | 19999 |
| dbscan | 0.3500 | 10 | 1 | 4 | 0.0002 | nan | 19996 |
| dbscan | 0.3500 | 20 | 1 | 12 | 0.0006 | nan | 19988 |
| dbscan | 0.5000 | 5 | 1 | 1 | 0.0001 | nan | 19999 |
| dbscan | 0.5000 | 10 | 1 | 1 | 0.0001 | nan | 19999 |
| dbscan | 0.5000 | 20 | 1 | 1 | 0.0001 | nan | 19999 |
| dbscan | 0.7500 | 5 | 1 | 0 | 0.0000 | nan | 20000 |
| dbscan | 0.7500 | 10 | 1 | 0 | 0.0000 | nan | 20000 |
| dbscan | 0.7500 | 20 | 1 | 0 | 0.0000 | nan | 20000 |
| dbscan | 1.0000 | 5 | 1 | 0 | 0.0000 | nan | 20000 |
| dbscan | 1.0000 | 10 | 1 | 0 | 0.0000 | nan | 20000 |
| dbscan | 1.0000 | 20 | 1 | 0 | 0.0000 | nan | 20000 |
| dbscan | 1.2500 | 5 | 1 | 0 | 0.0000 | nan | 20000 |
| dbscan | 1.2500 | 10 | 1 | 0 | 0.0000 | nan | 20000 |

Selected DBSCAN model:

- `eps`: `0.15`
- `min_samples`: `10`
- clusters excluding noise: `2`
- noise ratio: `0.0069`
- silhouette on non-noise points: `0.4162`

Selection rule: prefer a valid silhouette score, at least two non-noise
clusters, at least 20% of observations assigned to clusters, and noise ratio no
higher than 40%. This avoids selecting a model that looks good only because it
labels too many observations as noise.

## K-Means Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 29372 | -2.1839 | 3.1999 | -2.0364 | 2.9709 | Forward: 67.5%; Midfielder: 28.9%; Defender: 3.6% | 2025-2026: 8.4%; 2024-2025: 7.9%; 2021-2022: 4.4% | Benzema; Asensio; Jović; E. Hazard; Luis Díaz |
| 1 | 73694 | 2.4747 | 1.1516 | 2.5295 | 1.0878 | Midfielder: 52.5%; Forward: 46.9%; Defender: 0.6% | 2024-2025: 4.8%; 2015-2016: 4.8%; 2020-2021: 4.5% | Rodrygo; Ceballos; Mohamed Salah; Mané; Diogo Jota |
| 2 | 40524 | -4.5232 | -1.2179 | -4.3137 | -1.2438 | Defender: 39.5%; Goalkeeper: 36.9%; Midfielder: 17.1% | 2021-2022: 8.6%; 2025-2026: 7.7%; 2024-2025: 6.6% | Thibaut Courtois; Lucas Vázquez; Marcelo; Miguel Gutiérrez; Blanco |
| 3 | 47389 | 1.3732 | -2.7327 | 1.4924 | -2.8517 | Defender: 89.0%; Midfielder: 10.8%; Goalkeeper: 0.1% | 2021-2022: 4.9%; 2024-2025: 4.9%; 2015-2016: 4.6% | Vinícius Júnior; Luka Modric; Éder Militão; David Alaba; Carvajal |

Interpretation: clusters with high positive PC1 generally represent
high-volume/high-involvement player-seasons because PC1 in the PCA report loads
strongly on minutes, matches, passing volume, defensive activity, and shot
volume. PC2 separates more attacking output because the PCA report shows strong
loadings for goals, shots on target, goals per 90, and shot efficiency.

## DBSCAN Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| -1 | 171118 | -0.0021 | 0.0040 | 1.0125 | -0.0458 | Defender: 31.2%; Midfielder: 31.0%; Forward: 29.9% | 2024-2025: 5.7%; 2025-2026: 5.5%; 2021-2022: 5.4% | Vinícius Júnior; Benzema; Luka Modric; Éder Militão; David Alaba |
| 0 | 19848 | 0.0198 | -0.0392 | 1.0300 | -0.0646 | Defender: 31.1%; Midfielder: 31.0%; Forward: 29.7% | 2024-2025: 5.7%; 2025-2026: 5.5%; 2021-2022: 5.4% | Thibaut Courtois; Raúl Albiol; Grimaldo; Leroy Sané; Aymeric Laporte |
| 1 | 13 | -3.0241 | 7.2160 | -2.9917 | 7.1758 | Forward: 92.3%; Midfielder: 7.7% | 2021: 15.4%; 2023-2024: 7.7%; 2025: 7.7% | TSC Bačka Topola 2023-2024 Squad 21; Racing 2025 Squad 17; Universidad Católica 2017 Squad 19; UTC-3  Mexico 2014 Squad 22; Liga de Loja 2015 Squad 22 |

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
