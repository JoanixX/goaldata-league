# UEFA Champions League Data Scraper Pipeline

This project is a comprehensive Python-based data ingestion and enrichment pipeline designed to reconstruct a complete, granular historical dataset of the UEFA Champions League (2010-2025). 

The tool processes an incomplete raw CSV of match results and systematically crawls official and third-party APIs (UEFA & ESPN) to backfill missing, critical statistical points while enforcing a strict "no-invention" data integrity policy.

## Project Status & Data Completeness
The scraper successfully covers 100% of historical matches for the following features:
- **Lineups & Managers** (`planteles`, `entrenador_local`, `entrenador_visitante`)
- **Shots** (`tiros_totales`, `tiros_puerta`)
- **Possession** (`posesion_local`, `posesion_visitante`)
- **Fouls & Corners** (`faltas_total`, `corners_total`)

*Note: Assist data (`asistencias`) is currently unavailable via the ESPN/UEFA APIs and remains NULL. Yellow cards and Goals are structurally complete (missing values reflect 0-0 draws or matches without cards).*

## Repository Structure

```text
├── data/
│   ├── raw/                  # Initial raw CSV (needs enrichment)
│   └── processed/            # Final 100% enriched CSV output
├── src/
│   ├── main.py               # Main multi-source ingestion pipeline
│   ├── uefa_match_ids.json   # Static mapping of all UEFA match IDs
│   └── espn_id_overrides.json# Manual ID overrides for ESPN search failures
├── tests/
│   └── api_diagnostics/      # Comprehensive API field coverage test suite
└── README.md
```

## How it Works

The pipeline leverages two distinct sources to maximize data density:
1. **UEFA Match API**: The primary source of truth for match officials (distinguishing referees from assistants) and lineup statuses (starters vs bench).
2. **ESPN Summary API**: The primary source for match events (goals, substitutions, cards) and granular statistical box scores (possession, shots, fouls, corners).

### Intelligent Fallbacks & Aliases
Because team names in raw datasets rarely perfectly match external APIs (e.g. "PSG" vs "Paris Saint-Germain"), the pipeline includes an `_ALIASES` mapping layer. Additionally, `espn_id_overrides.json` resolves complex edge cases where venue orientation (Home vs Away) was inverted in the historical data or dates were misrecorded.

## Usage & Execution

### 1. Requirements
Ensure you have Python 3.10+ installed along with the required libraries:
```bash
pip install pandas requests
```

### 2. Running the Data Enrichment Pipeline
To process the raw data and generate the fully complete CSV, run the `main.py` entry point. 
The script is incremental: it will load the existing processed CSV (if any) and only fetch data for rows that are still missing critical fields (like `tiros_totales`), saving hours of redundant network calls.

```bash
python src/main.py
```
*The resulting file will be saved at `data/processed/champions_league_2011_2025_completed.csv`.*

### 3. Running Diagnostics & Tests
To verify data integrity and check the exact completion percentages for each column across the entire 445-match dataset, run the diagnostic suite:

```bash
python tests/api_diagnostics/run_all_tests.py
```
*This will execute targeted tests for UEFA Officials, ESPN Stats, and ESPN Roster validation, alongside a global Coverage Report.*