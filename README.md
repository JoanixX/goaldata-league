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
- **Fuzzy Matching**: Resolves team name inconsistencies across different data providers.
- **Diagnostic Reporting**: Automated field coverage reports to ensure data integrity.

## Quick Start
1. Install dependencies: `pip install -r requirements.txt`
2. Build cleaned relational datasets: `python script.py`
3. Impute missing statistical fields without overwriting observed values: `python src/impute_missing_stats.py`
4. Run the enrichment pipeline when scraper access is needed: `python src/main.py`
5. Merge a scraper JSON into `cl_2010_2025_completed.csv` safely: `python src/data_merge.py path/to/scraper_results.json`
6. Run diagnostics: `python tests/api_diagnostics/run_all_tests.py`

For detailed information about each component, refer to the README files in the respective subdirectories.
