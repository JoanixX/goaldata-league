# UEFA Match Data Diagnostic & Testing Suite

This directory contains the testing and diagnostic infrastructure for the UEFA Champions League data enrichment pipeline.

## Directory Structure

The testing suite is organized by data source, following a modular architecture:

```text
tests/api_diagnostics/
├── [source_name]/              # Subfolder for each scraper (uefa, espn, fbref, etc.)
│   └── test_[source].py        # Diagnostic test script for the source
└── results/
    └── [source_name]/          # Output directory for diagnostic reports
        ├── match_index.json    # IDs/Names of all available matches (2010-2025)
        ├── sample_data.json    # Sample JSON response from the platform
        ├── not_found_matches.json   # Matches in core/matches.csv not found on this platform
        └── missing_data_matches.json # Matches found but with missing fields
```

## Data Sources (Scrapers)

The pipeline utilizes five distinct scrapers to ensure 100% data density:

1.  **UEFA API**: Primary source for match start times, referees, and official lineups.
2.  **ESPN API**: Main source for match statistics (possession, referee) and event timelines.
3.  **Flashscore**: Fallback for granular statistics and real-time event validation.
4.  **Worldfootball**: Fallback for historical lineups and stadium metadata.
5.  **FBref**: High-granularity performance data (assists, tackles, interceptions).

## Diagnostic Reports (The 4 JSONs)

Each scraper generates reports in its `results/` subfolder:

*   **`match_index.json`**: Mapping of all available matches on the platform.
*   **`sample_data.json`**: Raw data snapshot to verify schema integrity.
*   **`not_found_matches.json`**: Identifies gaps where the platform is missing a match.
*   **`missing_data_matches.json`**: Lists matches with incomplete data (e.g., missing possession).

## How to Run Diagnostics

To run the full diagnostic suite:
```bash
python tests/api_diagnostics/run_all_tests.py
```

To update the global coverage report:
```bash
python tests/api_diagnostics/generate_coverage_report.py
```

## Global Coverage Report

The **`results/field_coverage.json`** file provides a high-level overview of the entire dataset's completeness across the relational core and stats tables.

## Maintenance

When adding new matches to the `data/raw/core/matches.csv` file, run the diagnostic tests to identify data gaps.
