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
        ├── not_found_matches.json   # Matches in our CSV not found on this platform
        └── missing_data_matches.json # Matches found but with missing fields
```

## Data Sources (Scrapers)

The pipeline utilizes five distinct scrapers to ensure 100% data density:

1.  **UEFA API**: Primary source for match start times, referees, and official lineups.
2.  **ESPN API**: Main source for match statistics (possession, shots, fouls) and event timelines.
3.  **Flashscore**: Fallback for granular statistics and real-time event validation.
4.  **Worldfootball**: Fallback for historical lineups and stadium metadata.
5.  **FBref**: High-granularity performance data (assists, tackles, interceptions) with Cloudflare-bypass capabilities.

## Diagnostic Reports (The 4 JSONs)

Each scraper generates four critical reports in its `results/` subfolder:

*   **`match_index.json`**: A comprehensive mapping of all Champions League matches available on the platform from 2010 to 2025.
*   **`sample_data.json`**: A full raw data snapshot from the platform to verify schema integrity.
*   **`not_found_matches.json`**: Identifies gaps where the platform does not have a record of a match present in our master dataset.
*   **`missing_data_matches.json`**: Lists matches that exist but have incomplete data (e.g., missing possession or lineups).

## How to Run Diagnostics

To run the diagnostics for a specific source and update its JSON reports:

```bash
python tests/api_diagnostics/[source]/test_[source].py
```

Example for UEFA:
```bash
python tests/api_diagnostics/uefa/test_uefa.py
```

## Global Coverage Report

The **`results/field_coverage.json`** file provides a high-level overview of the entire dataset's completeness across all 447+ rows.

## Maintenance

When adding new matches to the `data/raw/cl_2010_2025.csv` file, run the diagnostic tests to identify which sources need to be updated or where data gaps still exist.
