# Missing Data Policy

The project uses missing-value imputation only as a last step after source
cross-reference. The goal is not to make the tables look complete; the goal is
to make them true enough for analysis.

## References Used

- Little and Rubin, *Statistical Analysis with Missing Data*.
- van Buuren, *Flexible Imputation of Missing Data*.
- Khan and Hoque, "SICE: an improved missing data imputation technique",
  *Journal of Big Data*, 2020.
- Brini and van den Heuvel, "Missing Data Imputation with High-Dimensional
  Data", *The American Statistician*, 2024.
- Kontos and Karlis, "Football analytics based on player tracking data using
  interpolation techniques for the prediction of missing coordinates", 2023.
- Butera et al., "Hot Deck Multiple Imputation for Handling Missing
  Accelerometer Data", 2019.
- Decroos et al., "Actions Speak Louder than Goals", 2019, for football
  action normalization and per-90 reasoning.

## Rules

1. Never overwrite observed non-empty values.
2. Never create extra rows to satisfy the per-dataset 1.5M-record gate.
3. Treat `NULL`, empty strings, `NA`, `NAN`, `NONE`, and invalid failed parses
   as missing.
4. Reclassify impossible football values as missing, for example both teams
   having `0.0` possession in a completed match.
5. Use formulas only when all formula inputs are observed or already justified.
6. Prefer medians over means for skewed counting stats.
7. Group imputations by football context where possible: player, season, team,
   position, competition phase, and minutes played.
8. Record the method, affected rows, columns, and justification in logs.
9. Flag exact adjacent repeated rows in the quality report. Similar adjacent
   rows are reported for analysis, but exact repeated rows fail the quality
   gate because they are a common symptom of accidental data inflation.

## Field-Level Policy

- `assist_player_id`: fill from observed event sources when available. If a
  source records the goal but not the assist, keep `NULL`; source-unavailable
  markers are counted as missing and are not accepted as completed data.
- `goal_type`: derive only from source goal descriptors such as penalty or own
  goal markers; otherwise use `regular` for goal rows with no special marker.
- Match/player counting fields such as goals, assists, shots, cards, passes,
  tackles, interceptions, clearances, fouls, offsides, and dribbles: prefer
  exact player-season or player-match source rows, then per-90 rates scaled by
  minutes within player/season or position groups.
- Formula fields: `pass_accuracy`, `shots_off_target`, `tackles_lost`, and
  `touches` are calculated only when their component fields are present or have
  already been justified.
- Physical/profile fields such as distance covered, top speed, nationality,
  age, height, weight, position, country, and logo: fill only from exact player
  or team source rows. Do not use median or demographic imputation for personal
  profile fields. If unavailable in integrated tabular sources, keep `NULL`.
- Team logos are not inferred from club names because logo URLs are mutable
  branded assets and require direct licensed source evidence.
- `NOT_AVAILABLE_IN_SOURCE` and `NO_ASSIST_OR_NOT_RECORDED` are treated as
  missing values by the validation layer.
- Count fields must be non-negative integers. Per-90 or rate-derived
  estimates are rounded back to record-level counts before storage; fractional
  count outputs are validation failures.
- All-zero action columns in `player_match_stats` are treated as suspicious
  source gaps for fields such as crosses, dribbles, offsides, clearances,
  tackles won/lost, shots blocked, and fouls suffered. If no positive reliable
  donor rate exists, the column remains `NULL` instead of storing fake zeros.
- Player names are normalized accent-insensitively for matching. Obvious
  short/full duplicates are merged only when there is exactly one same-team
  candidate, for example `Neuer` and `Manuel Neuer`.

## Formula Checks

- `pass_accuracy = passes_completed / passes_attempted`
- `shots_off_target = shots - shots_on_target - shots_blocked`
- `tackles_lost = tackles - tackles_won`
- `possession_home + possession_away ~= 1.0`
- completed passes cannot exceed attempted passes;
- shots on target cannot exceed total shots;
- red/yellow cards, goals, assists, fouls, and offsides cannot be negative.

## Large-Gap Rule

If a column has more than 0.25% missing values, the pipeline must first try new
source coverage. Statistical inference can be used only after the report makes
the limitation visible and cites the method used.

Latest correction on `2026-05-02` intentionally reclassified unavailable
markers as missing. This means several columns now fail the `0.25%` missing
threshold, which is preferable to silently accepting fake completeness. The
pipeline must fill those gaps only through exact source integration, such as a
successful Transfermarkt profile scrape/merge or another licensed player data
feed.

Latest executed quality report after the approved Football-Data refresh:

- `matches`: `169,964` rows; fails the `1,500,000` target and missingness for
  stadium, city, country, referee, and possession.
- `goals`: `3,506` rows; fails the `1,500,000` target and missingness for
  assists/player names.
- `players`: `6,109` unique players; fails the `85,000` target and profile
  missingness for nationality, age, height, weight, and position.
- `teams`: `831` teams; fails the `3,000` target and missingness for country
  and logo.
- Formula anomalies after normalization: `0`.
- Exact adjacent repeated rows after validation: `0` for `matches`.

These failures are blockers for the requested final dataset size. They should
be solved by adding licensed/open observed sources, not by cloning rows or
turning missing source fields into constants.

## Required Coverage Sources

- Football-Data CSV/XLSX/ZIP files are used for observed domestic match rows
  across the wider Football-Data league set from `2004-2005` through available
  `2025-2026` files.
- FBref-style top-league player-season records are used as priors for
  position-aware imputation where exact Champions League records are absent.
- UEFA, ESPN, Transfermarkt, local UEFA files, and 2025 Champions source files
  remain preferred for Champions League match, event, player, and profile
  fields.
- OpenFootball and FootyStats are registered as additional sources for Europa
  League, Copa Libertadores, and Copa Sudamericana. FootyStats player files/API
  can be used only when access and licensing permit.
- OpenFootball South America and national-team datasets are registered for
  Brasileirao, Argentina, Colombia, Paraguay, Ecuador, World Cup, UEFA Euro,
  and Copa America fixture coverage. Scores can support match-level auxiliary
  rows, but they cannot create player-level goal/assist records unless the
  source includes named scorers/assisters.
- StatsBomb Open Data is registered for event-level JSON where its selective
  competition coverage overlaps the project scope. Its attribution terms must
  be respected in any public output.

References:
- https://www.football-data.co.uk/downloadm.php
- https://www.football-data.co.uk/data.php
- https://github.com/footballcsv/cache.footballdata
- https://github.com/openfootball/leagues
- https://github.com/openfootball/south-america
- https://github.com/statsbomb/open-data
