# Week 7 Clustering and Validation Report

Generated at: `2026-05-13T21:35:23`

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

PC1 + PC2 explained variance: `0.2807`.

This means the 2D clustering is interpretable and visual, but it does not
preserve all information from the full feature matrix. That limitation is
discussed in the failure analysis.

## K-Means Parameter Sweep

| algorithm | k | inertia | silhouette | calinski_harabasz | davies_bouldin | cluster_count | noise_ratio |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kmeans | 2 | 5252.6463 | 0.6104 | 2225.6781 | 0.7908 | 2 | 0.0000 |
| kmeans | 3 | 3158.5824 | 0.6136 | 3196.6747 | 0.5880 | 3 | 0.0000 |
| kmeans | 4 | 2377.6710 | 0.6307 | 3274.9472 | 0.5885 | 4 | 0.0000 |
| kmeans | 5 | 1782.3521 | 0.5736 | 3614.8429 | 0.5931 | 5 | 0.0000 |
| kmeans | 6 | 1336.6962 | 0.4680 | 4125.7209 | 0.6723 | 6 | 0.0000 |
| kmeans | 7 | 1037.2037 | 0.4430 | 4625.0599 | 0.6565 | 7 | 0.0000 |
| kmeans | 8 | 861.1837 | 0.4345 | 4892.0948 | 0.6515 | 8 | 0.0000 |
| kmeans | 9 | 753.8133 | 0.4229 | 4961.1013 | 0.6757 | 9 | 0.0000 |
| kmeans | 10 | 653.7307 | 0.3950 | 5152.8243 | 0.6853 | 10 | 0.0000 |

Selected K-Means model:

- `k`: `4`
- silhouette: `0.6307`
- inertia: `2377.6710`
- Calinski-Harabasz: `3274.9472`
- Davies-Bouldin: `0.5885`

Selection rule: choose the `k` with the highest silhouette score. Inertia is
reported because it is the K-Means objective, but it always tends to decrease as
`k` increases, so it should not be used alone.

## DBSCAN Parameter Sweep

| algorithm | eps | min_samples | cluster_count | noise_count | noise_ratio | silhouette_non_noise | non_noise_count |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dbscan | 0.1500 | 5 | 7 | 137 | 0.0337 | 0.1005 | 3928 |
| dbscan | 0.1500 | 10 | 4 | 240 | 0.0590 | 0.4098 | 3825 |
| dbscan | 0.1500 | 20 | 1 | 415 | 0.1021 | nan | 3650 |
| dbscan | 0.2500 | 5 | 2 | 80 | 0.0197 | 0.6631 | 3985 |
| dbscan | 0.2500 | 10 | 2 | 100 | 0.0246 | 0.7016 | 3965 |
| dbscan | 0.2500 | 20 | 1 | 188 | 0.0462 | nan | 3877 |
| dbscan | 0.3500 | 5 | 2 | 47 | 0.0116 | 0.7152 | 4018 |
| dbscan | 0.3500 | 10 | 1 | 77 | 0.0189 | nan | 3988 |
| dbscan | 0.3500 | 20 | 1 | 101 | 0.0248 | nan | 3964 |
| dbscan | 0.5000 | 5 | 1 | 30 | 0.0074 | nan | 4035 |
| dbscan | 0.5000 | 10 | 1 | 55 | 0.0135 | nan | 4010 |
| dbscan | 0.5000 | 20 | 1 | 71 | 0.0175 | nan | 3994 |
| dbscan | 0.7500 | 5 | 2 | 14 | 0.0034 | 0.8087 | 4051 |
| dbscan | 0.7500 | 10 | 1 | 26 | 0.0064 | nan | 4039 |
| dbscan | 0.7500 | 20 | 1 | 41 | 0.0101 | nan | 4024 |
| dbscan | 1.0000 | 5 | 1 | 12 | 0.0030 | nan | 4053 |
| dbscan | 1.0000 | 10 | 1 | 23 | 0.0057 | nan | 4042 |
| dbscan | 1.0000 | 20 | 1 | 25 | 0.0062 | nan | 4040 |
| dbscan | 1.2500 | 5 | 1 | 11 | 0.0027 | nan | 4054 |
| dbscan | 1.2500 | 10 | 1 | 11 | 0.0027 | nan | 4054 |

Selected DBSCAN model:

- `eps`: `0.75`
- `min_samples`: `5`
- clusters excluding noise: `2`
- noise ratio: `0.0034`
- silhouette on non-noise points: `0.8087`

Selection rule: prefer a valid silhouette score, at least two non-noise
clusters, at least 20% of observations assigned to clusters, and noise ratio no
higher than 40%. This avoids selecting a model that looks good only because it
labels too many observations as noise.

## K-Means Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 3195 | -1.0189 | -0.0258 | -1.2373 | -0.1129 | Other: 65.7%; Unknown: 24.6%; Midfielder: 3.0% | 2021-2022: 86.7%; 2025-2026: 13.3% | Jović; Marcelo; E. Hazard; Miguel Gutiérrez; Ceballos |
| 1 | 717 | 4.1044 | -1.3604 | 3.4511 | -1.1692 | Unknown: 39.3%; Midfielder: 22.6%; Defender: 21.1% | 2021-2022: 59.4%; 2025-2026: 40.6% | Courtois; Vinícius Júnior; Benzema; Modrić; Éder Militão |
| 2 | 10 | 10.1837 | 27.2505 | 11.2307 | 24.0549 | Other: 90.0%; Unknown: 10.0% | 2021-2022: 100.0% | Luca Pellegrini; Morgan Gibbs-White; Gori; Yanis Guermouche; Carlos Martín |
| 3 | 143 | 1.4739 | 5.4924 | 0.9411 | 4.4528 | Other: 86.0%; Unknown: 10.5%; Midfielder: 2.8% | 2021-2022: 92.3%; 2025-2026: 7.7% | Blanco; Knauff; Dentinho; Fabio Miretti; Zesiger |

Interpretation: clusters with high positive PC1 generally represent
high-volume/high-involvement player-seasons because PC1 in the PCA report loads
strongly on minutes, matches, passing volume, defensive activity, and shot
volume. PC2 separates more attacking output because the PCA report shows strong
loadings for goals, shots on target, goals per 90, and shot efficiency.

## DBSCAN Cluster Profiles

| cluster | size | pc1_mean | pc2_mean | pc1_median | pc2_median | top_positions | top_seasons | example_players |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| -1 | 14 | 10.3532 | 21.1070 | 10.4124 | 18.8407 | Other: 78.6%; Unknown: 14.3%; Forward: 7.1% | 2021-2022: 100.0% | Benzema; Luca Pellegrini; Morgan Gibbs-White; Kaide Gordon; Gori |
| 0 | 4042 | -0.0500 | -0.1019 | -0.9430 | -0.2352 | Other: 55.9%; Unknown: 26.8%; Midfielder: 6.4% | 2021-2022: 82.0%; 2025-2026: 18.0% | Courtois; Vinícius Júnior; Modrić; Éder Militão; Alaba |
| 1 | 9 | 6.3381 | 12.9123 | 6.4246 | 12.6413 | Other: 88.9%; Midfielder: 11.1% | 2021-2022: 100.0% | Blanco; Filip Benkovi?; Solomon Bonnah; Lohann Doucet; Estanis |

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
