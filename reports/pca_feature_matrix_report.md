# PCA Feature Matrix Report

Generated at: `2026-05-02T10:36:51`

## Unit of Analysis

The feature matrix uses one row per `player_id` and `season` from
`data/processed/stats/player_season_stats_cleaned.csv`. This level was chosen
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

- Rows in feature matrix: `1458`
- Encoded feature count after One-Hot Encoding: `37`
- Components needed to reach at least 90% cumulative explained variance:
  `18`
- Cumulative explained variance at that point:
  `0.9036`
- Explained variance captured by PC1 + PC2:
  `0.3027`

The full cumulative explained variance table is saved to:
`artifacts\pca_explained_variance.csv`

## Component Interpretation

Top absolute loadings for PC1:

| feature | PC1 | abs_loading |
| --- | --- | --- |
| minutes_played | 0.340903 | 0.340903 |
| matches_played | 0.331047 | 0.331047 |
| passes_attempted | 0.322725 | 0.322725 |
| passes_completed | 0.312766 | 0.312766 |
| interceptions | 0.287002 | 0.287002 |
| fouls_committed | 0.267861 | 0.267861 |
| tackles | 0.230447 | 0.230447 |
| shots | 0.225534 | 0.225534 |

Top absolute loadings for PC2:

| feature | PC2 | abs_loading |
| --- | --- | --- |
| shots_on_target | 0.334575 | 0.334575 |
| goals | 0.332562 | 0.332562 |
| goals_per90 | 0.296185 | 0.296185 |
| shots_on_target_per90 | 0.284932 | 0.284932 |
| shots | 0.276666 | 0.276666 |
| goal_conversion_rate | 0.241285 | 0.241285 |
| shot_accuracy | 0.223446 | 0.223446 |
| interceptions | -0.203449 | 0.203449 |

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