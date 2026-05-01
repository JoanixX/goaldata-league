# Source Code (`src/`)

This directory houses the core logic for the UEFA Champions League (UCL) Data Enrichment Pipeline. The architecture is designed to be modular, resilient, and highly extensible, supporting multiple data sources with automated fallback mechanisms.

## Project Architecture

The pipeline operates on a "Source Priority" model, ensuring that the most official data (UEFA) is used whenever possible, while filling gaps with secondary (ESPN) and tertiary (Flashscore, FBref, Worldfootball) sources.

### 1. Main Orchestrator (`main.py`)
`main.py` is the entry point for the enrichment process. Its responsibilities include:
*   **Dataset Management**: Loads the raw CSV (`data/raw/cl_2010_2025.csv`) and manages incremental updates.
*   **Missing Data Discovery**: Identifies rows and specific fields (refs, stats, lineups) that are NULL and need enrichment.
*   **Scraper Orchestration**: Coordinates the five specialized scrapers in a prioritized sequence to maximize data density.
*   **Merge Logic**: Implements sophisticated merging rules (e.g., UEFA officials always override ESPN data due to higher precision).

### 2. Modular Scrapers (`src/scrapers/`)
We have transitioned from monolithic API clients to a modular package structure:
*   **`uefa.py`**: Interacts with the official UEFA Match API. Primary for kickoff times and referee teams.
*   **`espn.py`**: Uses the ESPN Scoreboard/Summary API. Excellent for match statistics and key events (goals/cards).
*   **`flashscore.py`**: A fallback scraper for match statistics (possession, shots, etc.) using their internal feed system.
*   **`worldfootball.py`**: Specialized in historical lineup recovery and stadium metadata.
*   **`fbref.py`**: Provides deep tactical stats (assists, interceptions). Includes a fallback mechanism via the **Wayback Machine** to bypass bot protections on historical match sheets.
*   **`utils.py`**: Shared utilities for all scrapers, including text normalization, regex helpers, and safe mathematical operations.

### 3. Configuration & Aliases (`config.py`)
To solve the "Entity Resolution" problem (e.g., "Paris Saint-Germain" vs "PSG" vs "Paris"), `config.py` provides:
*   **Team Alias Mappings**: A dictionary of common variations for team names across different platforms.
*   **ID Overrides**: Handles manual overrides for match IDs when automatic discovery fails due to date or name mismatches.

### 4. Utility Scripts
*   **`conversion_csv.py`**: A specialized tool to export the final enriched dataset into seasonal JSON files for easier consumption by frontend applications or data analysts.
*   **`enrich_advanced_metrics.py`**: Adds documented xT/VAEP/xA/pressing/load/discipline proxy columns to processed CSVs. It writes formula provenance and coverage reports under `data/processed/metadata/`.
*   **`advanced_metric_formulas.py`**: Central catalog of required columns, formulas, references, and "not computable from current schema" notes for advanced metrics.

## Enrichment Flow

1.  **Phase 1: UEFA/ESPN Lookup** - Checks official APIs for core match data.
2.  **Phase 2: Index-Based Discovery** - If data is missing, the system looks up the match in the Flashscore/Worldfootball indices.
3.  **Phase 3: Deep Fallback (FBref)** - If stats are still missing, the FBref scraper is triggered as a final attempt.
4.  **Phase 4: Normalization & Cleanup** - All extracted data is normalized to the project's standard schema before saving.

## How to Extend
To add a new data source:
1.  Create a new class in `src/scrapers/`.
2.  Inherit common methods or use `scrapers.utils`.
3.  Register the new scraper in the `fill_missing_data` loop within `main.py`.
