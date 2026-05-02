# PCA Feature Matrix Report

Generated at: `2026-05-02T12:23:11`

## Unit of Analysis

The feature matrix uses one row per `player_id` and `season` from
`data/processed/stats/player_season_stats_cleaned.parquet`. This level was chosen
because player-season aggregates are denser and more stable than player-match
rows, which still contain many source-limited `NULL` values for tracking and
event-detail columns.

## Variable Selection

### Numeric Variables Used

Base numeric columns:

`matches_played, minutes_played, goals, assists, shots, shots_on_target, passes_completed, passes_attempted, tackles, interceptions, fouls_committed, yellow_cards, red_cards`

Engineered numeric columns:

`minutes_per_match, goals_per90, assists_per90, shots_per90, shots_on_target_per90, passes_completed_per90, passes_attempted_per90, tackles_per90, interceptions_per90, fouls_committed_per90, cards_per90, shot_accuracy, pass_accuracy, goal_conversion_rate, defensive_actions_per90, discipline_points_per90`

These variables describe attacking output, chance volume, passing involvement,
defensive activity, minutes/load, and discipline. They are relevant for PCA
because the objective is to discover latent player profiles rather than predict
a single target.

### Categorical Variables Used

`season` and `position_group` were encoded with One-Hot Encoding. PCA can only
operate on numbers, so categories must be converted into numeric dummy columns.
`position_group` adds tactical role context, while `season` controls for source
and competition-period differences.

### Variables Discarded

- Identifiers such as `player_id`, `team_id`, and names are excluded from PCA
  because they are keys, not behavioral features.
- `nationality` is excluded because it has high missingness and would add many
  sparse identity dummies that do not directly describe player performance.
- Raw player-match advanced metrics with 100% missingness are excluded from this
  PCA matrix because they would add no signal.

## Missing Values

Numeric missing values are imputed with the median. Median imputation is robust
to skewed football statistics, where a few elite players can have very high
values. Categorical missing values are imputed with the most frequent category
inside the preprocessing pipeline. No missing values are replaced with invented
football events or source data.

## Feature Engineering

Per-90 rates normalize production by playing time, making bench players and
starters more comparable. Accuracy and conversion rates summarize efficiency.
Discipline and defensive-action rates add behavioral context beyond goals and
assists.

## Scaling

`StandardScaler` is applied before PCA. PCA is variance-based: without scaling,
large-scale variables such as minutes or passes would dominate the principal
components simply because their units are larger. Scaling gives each feature
mean 0 and standard deviation 1, so PCA reflects correlation structure instead
of raw measurement scale.

## PCA Results

- Rows in feature matrix: `4065`
- Encoded feature count after One-Hot Encoding: `37`
- Components needed to reach at least 90% cumulative explained variance:
  `19`
- Cumulative explained variance at that point:
  `0.9113`
- Explained variance captured by PC1 + PC2:
  `0.2807`

The full cumulative explained variance table is saved to:
`artifacts\pca_explained_variance.csv`

## Component Interpretation

Top absolute loadings for PC1:

| feature | PC1 | abs_loading |
| --- | --- | --- |
| passes_attempted | 0.326330 | 0.326330 |
| passes_completed | 0.323548 | 0.323548 |
| interceptions | 0.314009 | 0.314009 |
| fouls_committed | 0.284171 | 0.284171 |
| shots | 0.255080 | 0.255080 |
| tackles | 0.248703 | 0.248703 |
| shots_on_target | 0.223353 | 0.223353 |
| red_cards | 0.221213 | 0.221213 |

Top absolute loadings for PC2:

| feature | PC2 | abs_loading |
| --- | --- | --- |
| discipline_points_per90 | 0.337489 | 0.337489 |
| fouls_committed_per90 | 0.325246 | 0.325246 |
| passes_attempted_per90 | 0.319768 | 0.319768 |
| passes_completed_per90 | 0.305064 | 0.305064 |
| cards_per90 | 0.301043 | 0.301043 |
| minutes_per_match | -0.254461 | 0.254461 |
| defensive_actions_per90 | 0.246558 | 0.246558 |
| tackles_per90 | 0.237442 | 0.237442 |

PC1 and PC2 are weighted combinations of the original scaled features. They do
not represent one original statistic; instead, they summarize dominant patterns
such as attacking volume, passing involvement, defensive activity, discipline,
or position/season structure depending on the loadings above.

## 2D Plot Interpretation

The 2D PCA plot is saved to `artifacts\pca_player_season_2d.png`. Points that
are close together represent player-seasons with similar statistical profiles.
Separation by `position_group` suggests that the feature matrix captures
tactical role differences. Overlap between groups is expected in football data,
especially for hybrid roles such as attacking midfielders or wing-backs.

## Why PCA Helps This Dataset

PCA reduces many correlated football statistics into fewer orthogonal
components. This helps downstream clustering, similarity search, ranking, and
recommendation by reducing redundancy and noise while preserving most of the
variance in player-season profiles.
