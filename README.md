# UEFA Champions League Data Pipeline (2011-2025)

This repository contains a data engineering + analytics pipeline for football
(Champions League and domestic leagues). It merges real match records (final
scores, player profiles, FBref season totals) into a relational schema of ~94.5k
matches, ~1.95M player-match rows, and ~254.6k real-sized goal events, then builds
a representation (PCA), segmentation (clustering) and a player-similarity
recommender on top.

> **Data honesty note.** Real granular event data is not available for the full
> historical scope, so part of the per-match/event data is **simulated from real
> anchors** (real scorelines, real position rates) using documented statistical
> models, not arbitrary fabrication. Every cell is tagged in a `data_provenance`
> column. See `reports/methodology_and_citations.md` and `data/dictionary.txt`.

## Project Architecture
The project is organized into modular components:
- **`src/`**: Core ingestion and enrichment logic.
- **`tests/`**: Integration tests and API diagnostic suite.
- **`notebooks/`**: Exploratory data analysis and prototyping.
- **`data/`**: Storage for raw and processed datasets.

## Key Features
- **Incremental Enrichment**: The pipeline only processes rows with missing data, saving bandwidth and time.
- **Multi-Source Validation**: Merges UEFA (official lineups/officials) with ESPN (match events/stats).
- **Extensible Source Ingestion**: Reads structured files, directories, and ZIP archives through `src/source_ingestion.py` for CSV, TSV, Excel, JSON, JSONL, HTML, Parquet, and text inputs while skipping audio/video.
- **Fuzzy Matching**: Resolves team name inconsistencies across different data providers.
- **Diagnostic Reporting**: Automated field coverage reports to ensure data integrity.
- **Quality Gates + Parquet**: Cleaned datasets are written as CSV and Parquet formats. Parquet is utilized for high-performance analytical queries and dimensionality reduction (PCA), preserving native data types. A generated `logs/data_quality_report.json` flags null ratios, formula anomalies, and the 1.5M-record requirement before ML use.
- **Dimensionality Reduction (PCA)**: Automated pipeline to transform multi-dimensional player statistics (4,000+ player-season profiles) into latent tactical embeddings for style-of-play clustering.

## Quick Start
1. Install dependencies: `pip install -r requirements.txt` (includes `pyarrow` for Parquet and `rapidfuzz` for entity resolution).
2. Build cleaned relational datasets: `python script.py`
3. Impute missing statistical fields without overwriting observed values: `python src/impute_missing_stats.py`
4. Expand top-division rosters, fill processed tables, and add 50+ ML features: `python src/enrich_processed_features.py`
5. **Realistic, paper-grounded synthesis + entity resolution**: `python -m src.rebuild_realistic_datasets`
   - Re-allocates per-match goals to the **real scoreline** (sum of player goals == real score, verified at 100%), grounds rates in real FBref priors, rebuilds `goals_events_cleaned` at its real size (~254k goals), collapses duplicate player identities (`CristianoRonaldo`/`cristiano_ronaldo`/`CR7` -> one id), and tags every table with `data_provenance`. See `reports/methodology_and_citations.md`.
6. Add documented advanced metrics to processed datasets: `python src/enrich_advanced_metrics.py`
7. Generate PCA feature matrix (retains the top components, not only 2): `python src/build_pca_feature_matrix.py`
8. Run Week 7 clustering validation: `python src/build_clustering_analysis.py`
9. **Week 10 recommendation + offline evaluation**: `python -m src.recommendation_evaluation` (and `python -m src.recommendation_engine` for example queries).
10. Run the enrichment pipeline when scraper access is needed: `python src/main.py`
11. Merge a scraper JSON into `cl_2010_2025_completed.csv` safely: `python src/data_merge.py path/to/scraper_results.json`
12. Run diagnostics: `python tests/api_diagnostics/run_all_tests.py`

Advanced metric enrichment writes only to `data/processed`. It does not modify
`data/raw`. Metrics whose cited methods require missing event locations, shot
populations, tracking data, or fitted model coefficients remain `NULL` and are
explained in `data/processed/metadata/advanced_metric_coverage.csv`.

`src/enrich_processed_features.py` preserves observed non-empty values, uses
available top-division source priors for team-season rosters, fills remaining
statistical gaps with deterministic probability rules, and writes both CSV and
Parquet outputs plus `logs/processed_feature_enrichment_report.json`.

`src/build_clustering_analysis.py` satisfies the Week 7 clustering milestone by
running K-Means and DBSCAN parameter sweeps on the PCA player-season embedding.
It writes validation tables, cluster labels, 2D cluster plots, and
`reports/clustering_validation_report.md`.

For detailed information about each component, refer to the README files in the respective subdirectories.

## Data Quality Policy
Observed rows and non-empty cells are preserved. Source-derived and simulated
records are generated from documented, literature-anchored models (never
unconditioned random constants), marked through the `data_provenance` column, and
validated with `logs/data_quality_report.json` and `logs/realistic_rebuild_report.json`.
See `reports/methodology_and_citations.md`, `reports/data_pipeline_flow.md`, and
`reports/missing_data_policies.md`.

### About the 1.5M-row target
`player_match_stats` (~1.95M) naturally exceeds 1.5M (≈20 players × 94.5k matches).
`goals_events_cleaned` is deliberately at its **real size (~254.6k goals, ≈2.69
per match)** and is *not* padded to 1.5M, because the number of goals is a physical
quantity — the same logic that keeps the players table at its natural size. Forcing
1.5M "goals" was the previous bug (a raw event-stream mislabelled as goals).

## Week 10: Recommendation / Ranking
The system is a **content-based item-item similarity & ranking** engine for
scouting (`src/recommendation_engine.py`): given a player-season it ranks the most
similar player-seasons. A **baseline** (Euclidean on PC1-PC2, global pool) is
compared against a **stronger** model (standardized distance over all retained PCs,
same-position candidate pool). `src/recommendation_evaluation.py` runs a
leakage-safe leave-one-out evaluation (same player across seasons should retrieve
itself) reporting Recall@k, MRR, MAP, NDCG vs the baseline. Reports:
`reports/recommendation_evaluation_report.md` and
`reports/recommendation_error_analysis.md`.
