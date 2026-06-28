# PCA Feature Matrix Report

Generated at: `2026-06-27T17:01:51`

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

## Data Treatment and Outliers (justified)

Before PCA the feature matrix receives a principled, documented treatment so the
components reflect real structure rather than a few extreme tails:

1. **Invalid-row removal.** Player-season rows with no `player_id`/`player_name`
   are aggregation artefacts (one "Unknown" bucket that collapses thousands of
   unmatched records into a single row with minutes in the millions). They are
   dropped because they are not real player-seasons and would otherwise dominate
   every scaled component.
2. **Winsorization at the 1st/99th percentile.** Per-90 rates explode for
   tiny-minute players (e.g. one goal in a few minutes -> 200+ goals/90), which
   is small-sample noise, not skill. Capping at robust quantiles keeps each
   feature's range even. PCA is variance-based and very sensitive to such tails.
3. **log1p transform** (inside the pipeline). Count and per-90 stats are heavily
   right-skewed; the log compresses high-volume tails so variance reflects
   structure, not a handful of high-usage players. All features are non-negative,
   so `log1p` is well defined.

## Missing Values

Numeric missing values are imputed with the median (robust to skew). Categorical
missing values use the most frequent category. No missing values are replaced
with invented football events or source data.

## Feature Engineering

Per-90 rates normalize production by playing time, making bench players and
starters more comparable. Accuracy and conversion rates summarize efficiency.
Discipline and defensive-action rates add behavioral context beyond goals and
assists.

## Scaling

`RobustScaler` (median centring, IQR scaling) is applied instead of
`StandardScaler` because, even after winsorization and `log1p`, football features
contain outliers; the IQR scale is not distorted by them the way mean/standard
deviation would be. This is scikit-learn's recommended scaler for data with
outliers. Scaling is necessary because PCA is variance-based: without it,
large-scale variables such as minutes or passes would dominate the components
simply because their units are larger.

## PCA Results

- Rows in feature matrix: `7257`
- Encoded feature count after One-Hot Encoding: `33`
- Components needed to reach at least 90% cumulative explained variance:
  `14`
- Cumulative explained variance at that point:
  `0.9052`
- Explained variance captured by PC1 + PC2:
  `0.3989`

The full cumulative explained variance table is saved to:
`artifacts\pca_explained_variance.csv`

## Component Interpretation

Top absolute loadings for PC1:

| feature | PC1 | abs_loading |
| --- | --- | --- |
| passes_completed_per90 | 0.318574 | 0.318574 |
| passes_attempted_per90 | 0.316249 | 0.316249 |
| defensive_actions_per90 | 0.310084 | 0.310084 |
| fouls_committed_per90 | 0.277182 | 0.277182 |
| tackles_per90 | 0.271693 | 0.271693 |
| interceptions_per90 | 0.256763 | 0.256763 |
| matches_played | -0.254000 | 0.254000 |
| shots_per90 | 0.229344 | 0.229344 |

Top absolute loadings for PC2:

| feature | PC2 | abs_loading |
| --- | --- | --- |
| minutes_played | 0.296155 | 0.296155 |
| passes_attempted | 0.280671 | 0.280671 |
| passes_completed | 0.280262 | 0.280262 |
| goals | 0.279716 | 0.279716 |
| shots | 0.271575 | 0.271575 |
| shots_on_target | 0.271379 | 0.271379 |
| fouls_committed | 0.264191 | 0.264191 |
| assists | 0.260577 | 0.260577 |

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
