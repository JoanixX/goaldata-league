# Data Pipeline Flow

This project prioritizes observed data over model-ready convenience. The ML
stage must not run until the processed datasets pass the quality gates in
`logs/data_quality_report.json`.

## 1. Source Discovery

1. Review `src/source_registry.json`.
2. Prefer official or well-documented providers in this order:
   UEFA, ESPN, BALLDONTLIE UCL, football-data.org, openfootball,
   API-Football, Statorium, Data Sports Group.
3. Check licensing, authentication, rate limits, and historical coverage.
4. Store downloaded source files under `data/raw/` without changing their
   original meaning.

## 2. Source Ingestion

Use `src/source_ingestion.py` for local files, folders, and archives. It can
read CSV, TSV, Excel, JSON, JSONL, HTML tables, Parquet, ZIP archives, and
plain text. Audio and video files are skipped.

Every ingested table receives source trace columns:

- `__source_path`
- `__source_type`
- `__source_table`

## 3. Unification Into Processed Tables

Run:

```bash
python -m src.build_processed
```

This builds:

- `data/processed/core/*.csv` and `*.parquet`
- `data/processed/events/*.csv` and `*.parquet`
- `data/processed/stats/*.csv` and `*.parquet`
- `logs/build_processed_report.json`
- `logs/data_quality_report.json`

CSV remains for compatibility. Parquet is the performance format.

## 4. Quality Gate

The gate checks:

- each column must have at most 0.25% missing values;
- total records must be at least 1,500,000;
- formulas must be internally consistent;
- invalid failed parses, such as `0.0 + 0.0` possession, are reclassified as
  missing instead of treated as observed data.

If a column fails, do not fabricate values. Cross-reference additional sources
first.

## 5. Missing-Value Handling

Allowed order:

1. exact value from a higher-priority source;
2. exact value from a lower-priority source with conflict logging;
3. deterministic formula from observed fields;
4. grouped robust statistics such as position/season/team medians;
5. documented model-based imputation only when missingness is small enough or
   the report explicitly states why it is acceptable.

All inferred values must preserve observed values and appear in an imputation
report.

## 6. EDA

Run EDA only after reviewing the quality report. EDA should document:

- row counts by dataset and season;
- null ratios by column;
- source coverage by dataset;
- outliers and formula anomalies;
- distributions for goals, cards, shots, passes, minutes, and possession.

## 7. ML Readiness

Supervised and unsupervised models are allowed only when:

- `passes_all_quality_gates` is true, or
- the experiment explicitly marks itself as exploratory and excludes failing
  columns from production training.
