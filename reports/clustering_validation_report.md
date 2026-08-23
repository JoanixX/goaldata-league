# Clustering and Validation Report

Generated at: `2026-07-09T02:02:16`

## Objective

This report segments the player-season domain and validates
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
  statistics and keeps the segmentation tied to the representation layer.
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

PC1 + PC2 explained variance: `0.4551`.

This means the 2D clustering is interpretable and visual, but it does not
preserve all information from the full feature matrix. That limitation is
discussed in the failure analysis.

## K-Means Parameter Sweep

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

Selected K-Means model:

- `k`: `3`
- silhouette: `0.4536`
- inertia: `2438.5568`
- Calinski-Harabasz: `3685.2960`
- Davies-Bouldin: `0.7519`

Selection rule: choose the `k` with the highest silhouette score. Inertia is
reported because it is the K-Means objective, but it always tends to decrease as
`k` increases, so it should not be used alone.

## DBSCAN Parameter Sweep

| algorithm | eps | min_samples | cluster_count | noise_count | noise_ratio | silhouette_non_noise | non_noise_count |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dbscan | 0.1500 | 5 | 7 | 78 | 0.0213 | -0.3316 | 3592 |
| dbscan | 0.1500 | 10 | 4 | 251 | 0.0684 | -0.2071 | 3419 |
| dbscan | 0.1500 | 20 | 8 | 970 | 0.2643 | 0.0884 | 2700 |
| dbscan | 0.2500 | 5 | 1 | 14 | 0.0038 | nan | 3656 |
| dbscan | 0.2500 | 10 | 2 | 31 | 0.0084 | 0.3488 | 3639 |
| dbscan | 0.2500 | 20 | 2 | 107 | 0.0292 | 0.3197 | 3563 |
| dbscan | 0.3500 | 5 | 1 | 10 | 0.0027 | nan | 3660 |
| dbscan | 0.3500 | 10 | 1 | 11 | 0.0030 | nan | 3659 |
| dbscan | 0.3500 | 20 | 1 | 30 | 0.0082 | nan | 3640 |
| dbscan | 0.5000 | 5 | 1 | 1 | 0.0003 | nan | 3669 |
| dbscan | 0.5000 | 10 | 1 | 6 | 0.0016 | nan | 3664 |
| dbscan | 0.5000 | 20 | 1 | 6 | 0.0016 | nan | 3664 |
| dbscan | 0.7500 | 5 | 1 | 0 | 0.0000 | nan | 3670 |
| dbscan | 0.7500 | 10 | 1 | 0 | 0.0000 | nan | 3670 |
| dbscan | 0.7500 | 20 | 1 | 0 | 0.0000 | nan | 3670 |
| dbscan | 1.0000 | 5 | 1 | 0 | 0.0000 | nan | 3670 |
| dbscan | 1.0000 | 10 | 1 | 0 | 0.0000 | nan | 3670 |
| dbscan | 1.0000 | 20 | 1 | 0 | 0.0000 | nan | 3670 |
| dbscan | 1.2500 | 5 | 1 | 0 | 0.0000 | nan | 3670 |
| dbscan | 1.2500 | 10 | 1 | 0 | 0.0000 | nan | 3670 |

Selected DBSCAN model:

- `eps`: `0.25`
- `min_samples`: `10`
- clusters excluding noise: `2`
- noise ratio: `0.0084`
- silhouette on non-noise points: `0.3488`

Selection rule: prefer a valid silhouette score, at least two non-noise
clusters, at least 20% of observations assigned to clusters, and noise ratio no
higher than 40%. This avoids selecting a model that looks good only because it
labels too many observations as noise.

## K-Means Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 1544 | 1.3025 | -2.0305 | 1.0215 | -1.9014 | Midfielder: 44.9%; Forward: 30.6%; Defender: 24.4% | 2021-2022: 65.3%; 2024-2025: 6.7%; 2023-2024: 5.0% | Vinícius Júnior; Rodrygo; Lucas Vázquez; Marcelo; Luis Díaz |
| 1 | 1089 | -3.6601 | 0.0226 | -3.5728 | -0.2836 | Defender: 33.7%; Midfielder: 28.2%; Goalkeeper: 26.0% | 2021-2022: 84.8%; 2024-2025: 3.5%; 2019-2020: 2.5% | Thibaut Courtois; Benjamin Mendy; Miguel Gutiérrez; Phil Jones; Jan Oblak |
| 2 | 1037 | 1.9042 | 2.9995 | 1.7816 | 3.2797 | Midfielder: 50.0%; Defender: 42.2%; Forward: 7.8% | 2021-2022: 43.7%; 2018-2019: 8.6%; 2020-2021: 8.6% | Luka Modric; Éder Militão; David Alaba; Casemiro; Toni Kroos |

Interpretation: clusters with high positive PC1 generally represent
high-volume/high-involvement player-seasons because PC1 in the PCA report loads
strongly on minutes, matches, passing volume, defensive activity, and shot
volume. PC2 separates more attacking output because the PCA report shows strong
loadings for goals, shots on target, goals per 90, and shot efficiency.

## DBSCAN Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| -1 | 31 | 0.2822 | -0.2642 | -1.3442 | -2.6234 | Forward: 48.4%; Midfielder: 35.5%; Defender: 16.1% | 2021-2022: 61.3%; 2017-2018: 9.7%; 2018-2019: 9.7% | Tyler Morton; Kylian Mbappé; Edinson Cavani; Mauro Icardi; Malik Tillman |
| 0 | 3626 | 0.0239 | 0.0132 | 0.3450 | -0.5158 | Midfielder: 41.5%; Defender: 32.4%; Forward: 18.3% | 2021-2022: 64.9%; 2024-2025: 6.2%; 2019-2020: 5.1% | Thibaut Courtois; Vinícius Júnior; Luka Modric; Éder Militão; David Alaba |
| 1 | 13 | -7.3437 | -3.0406 | -7.4049 | -3.0014 | Forward: 53.8%; Midfielder: 30.8%; Goalkeeper: 7.7% | 2021-2022: 100.0% | Nicola Bagnolini; Iker Bravo; Adrián Butzke; Daniel Chesters; Luis Hartwig |

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
