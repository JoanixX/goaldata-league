# UEFA Champions League Data Pipeline (2011-2025)

This repository contains a robust data engineering pipeline designed to build a 100% complete historical dataset of the UEFA Champions League. By merging raw match records with official UEFA and ESPN APIs, the project provides granular tactical and statistical data for over 440 matches.

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
1. Install dependencies: `pip install -r requirements.txt` (includes `pyarrow` for Parquet support).
2. Build cleaned relational datasets: `python script.py`
3. Impute missing statistical fields without overwriting observed values: `python src/impute_missing_stats.py`
4. Add documented advanced metrics to processed datasets: `python src/enrich_advanced_metrics.py`
5. Generate PCA feature matrix and 2D tactical maps: `python src/build_pca_feature_matrix.py`
6. Run the enrichment pipeline when scraper access is needed: `python src/main.py`
7. Merge a scraper JSON into `cl_2010_2025_completed.csv` safely: `python src/data_merge.py path/to/scraper_results.json`
8. Run diagnostics: `python tests/api_diagnostics/run_all_tests.py`

Advanced metric enrichment writes only to `data/processed`. It does not modify
`data/raw`. Metrics whose cited methods require missing event locations, shot
populations, tracking data, or fitted model coefficients remain `NULL` and are
explained in `data/processed/metadata/advanced_metric_coverage.csv`.

For detailed information about each component, refer to the README files in the respective subdirectories.

## Data Quality Policy
Rows must come from observed sources. The 1.5M-record requirement is enforced per processed dataset and must not be met by duplicating or fabricating records. See `reports/data_pipeline_flow.md` and `reports/missing_data_policies.md` for the source discovery, cleaning, imputation, EDA, and ML-readiness flow.
