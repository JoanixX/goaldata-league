# Data Pipeline Flow

This is the executable flow used for the current build. The dataset layer is
the primary artifact; model training is intentionally downstream of these
quality gates.

1. Download required domestic European leagues from Football-Data:
   `python -m src.download_football_data`
2. Build Champions League plus domestic league processed tables without
   changing existing schemas:
   `python -m src.build_processed`
3. Fill documented missing cells, preserving observed non-empty values, and
   regenerate Parquet quality outputs:
   `python src/impute_missing_stats.py`
4. Generate EDA summaries:
   `python -m src.eda_report`
5. Run tests:
   `python -m pytest tests -q`
6. Build PCA feature matrix and tactical clusters:
   `python src/build_pca_feature_matrix.py`

The legacy master orchestrator `python src/main.py` was also executed on
`2026-05-02`. Its pytest quality gate passed, and it now completes without
crashing, but the scraper subprocesses still returned errors in this local
environment: UEFA season stats exit code `1`, Transfermarkt exit code `1`, and
`data/raw/core/matches.csv` is absent for the older match-enrichment phase.
Those errors are logged in `logs/pipeline_20260502_092735.json` instead of
terminating the run.

## Integrated Real Sources

- UEFA/ESPN/local Champions League sources already present in `data/raw`.
- Football-Data CSVs for the required six leagues from `2004-2005` through
  available `2025-2026` files: Premier League, La Liga, Bundesliga, Serie A,
  Ligue 1, and Primeira Liga.
- FBref-style top-five-league player-season priors from
  `data/raw/2021-2022 Football Player Stats.csv`.
- 2025 Champions League player physical/performance priors from
  `data/raw/2025 Champions/DAY_4`.

## Step-by-Step Flow

1. Data ingestion: `src.download_football_data` fetches Football-Data CSV
   files for the configured European league set from `2004-2005` through
   `2025-2026`. In the sandboxed Codex environment, this step requires
   explicit network permission; the latest approved refresh downloaded `483`
   CSV files. `src.source_ingestion` remains the generic reader for CSV, TSV,
   JSON, JSONL, HTML, XLS/XLSX, Parquet, text, directories, and ZIP archives
   while excluding audio/video.
2. Data merging and unification: `src.build_processed` normalizes match,
   team, player, goal, player-match, player-season, and goalkeeper rows into
   the original processed schemas.
3. Data cleaning: invalid parsed values are converted back to `NULL`; formula
   contradictions such as completed passes greater than attempted passes are
   reclassified as missing before output.
4. Missing data handling: `src.impute_missing_stats` cross-references exact
   source rows first, then applies documented medians, per-90 rates, and
   formula fills only for remaining missing cells.
5. Data validation: `src.data_quality` checks per-column missing ratios,
   football formula anomalies, duplicate keys, exact adjacent repeated rows,
   `1.5M` rows for `matches` and `goals`, at least `85k` unique players, and
   at least `3k` teams.
6. Storage: every cleaned core table is written as CSV and Parquet under the
   existing `data/processed` folders; no new processed subdirectories are
   created by this flow.
7. EDA: `src.eda_report` reads Parquet outputs and writes
   `reports/eda_summary.json` plus `reports/eda_summary.md`.

## Current Status

Latest executed build on `2026-05-02` after approved Football-Data refresh:

- Total analytical processed records after wider Football-Data expansion:
  `195,294`.
- `matches`: `169,964` rows.
- `players`: `6,109` rows.
- `teams`: `831` rows.
- `goals`: `3,506` rows.
- `gk`: `960` rows.
- `match_stats`: `9,678` rows.
- `season_stats`: `4,246` rows.

The builder can be run either as `python -m src.build_processed` or
`python src/build_processed.py`. It now uses the top-five-league rows in
`2021-2022 Football Player Stats.csv`, not just Champions League rows, so
players and player-season stats are no longer unchanged while matches expand.
The Football-Data expansion now includes `483` downloaded CSVs across major
and secondary European divisions. Player identity resolution rewrites obvious
short-name aliases such as `Neuer` to a single canonical player id when there
is exactly one same-team full-name candidate.
The validation layer treats unavailable markers as missing. This causes several
columns to fail the `0.25%` missing-value threshold, but it prevents false data
quality caused by placeholders. Formula anomalies remain at `0` after hard
normalization. The strict record gates still fail: `matches` has `169,964`
real rows versus the `1,500,000` target, `goals` has `3,506` rows versus the
`1,500,000` target, `players` has `6,109` unique players versus `85,000`, and
`teams` has `831` teams versus `3,000`. These gaps are reported instead of
being filled with repeated or fabricated records.

## Expansion Candidates Registered

- OpenFootball Europe/Champions League for Champions League, Europa League,
  and UEFA Conference League fixture validation.
- OpenFootball South America for Brazil, Argentina, Colombia, Paraguay,
  Ecuador, Copa Libertadores, and Copa Sudamericana observed fixtures/scores.
- OpenFootball national-team data for World Cup, UEFA Euro, and Copa America
  fixture/scores.
- StatsBomb Open Data for selective event-level JSON with lineups and actions.
- FootyStats, API-Football, Statorium, football-data.org, and Data Sports
  Group remain credentialed/licensed options; they must not be scraped around
  licensing barriers.

## Source Notes

- Football-Data states that its all-country download page is updated regularly
  and exposes season CSV links back to `1993-1994`, including `2025-2026`.
- Football-Data lists Portugal Liga I as a main league. The footballcsv cache
  documents the source code for Portugal as `P1`.
- Football-Data also notes that match statistics are available for major
  leagues since `2000-2001` and for all 22 divisions since `2017-2018`; fields
  unavailable in a specific file are retained as source-unavailable markers
  rather than guessed.
- OpenFootball lists public-domain competition datasets for `cl`, `el`,
  `copa.l`, and `copa.s`. These are registered as future auxiliary competition
  ingestion sources for Europa League, Copa Libertadores, and Copa Sudamericana.

References:
- https://www.football-data.co.uk/downloadm.php
- https://www.football-data.co.uk/data.php
- https://github.com/footballcsv/cache.footballdata
