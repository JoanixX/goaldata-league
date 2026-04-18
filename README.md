# UEFA Champions League — GoalData League

> **Course:** Big Data · 2026-I Cycle  
> **Milestone:** Week 3 — Enhanced Data Pipeline & Scraper Integration  
> **Role:** Engineering, Modeling, and Analysis of UEFA Champions League Matches (2011–2025)

---

## 1. Project Overview

The **GoalData League** project aims to build a robust, end-to-end reproducible data pipeline for **UEFA Champions League** match data from the **2011-12 to 2024-25** seasons. 

The core research question is:
> **How have performance patterns and results evolved in the UEFA Champions League over 14 seasons, and what structures emerge when analyzing match-ups as a network (graph) of teams?**

This project handles data ingestion from seasonal JSON files, consolidation into a master CSV, and enrichment through advanced scrapers to ensure 100% data coverage for all requested fields.

---

## 2. Summary

| Field | Detail |
|---|---|
| **Domain** | Football — UEFA Champions League |
| **Period Covered** | 2011-12 → 2024-25 |
| **Main Entity** | Match |
| **Data Sources** | UEFA API, ESPN API, SofaScore/Flashscore |
| **Output Format** | Master CSV & Seasonal JSON files |

---

## 3. Technology Stack

```
Python 3.x
├── pandas         → DataFrame manipulation and consolidation
├── requests       → API interactions (UEFA, ESPN)
├── json           → Parsing and exporting data
└── pytest         → Automated verification and testing
```

| Tool | Usage |
|---|---|
| `pandas` | Loading, transforming, and exporting the dataset |
| `requests` | Fetching match details, lineups, and statistics from APIs |
| `UefaClient` | Custom client for UEFA V5 API (official data) |
| `EspnClient` | Custom client for ESPN API (match stats fallback) |
| `RatingClient` | Enrichment client for player ratings (SofaScore/Flashscore) |

---

## 4. Project Architecture

### Data Folder Structure

```
data/
├── raw/                # Original source files (Master CSV)
│   └── champions_league_2011_2025.csv
├── processed/          # Enriched and cleaned datasets
│   ├── champions_league_2011_2025_completed.csv
│   └── json_seasons/   # Final output split by season
│       ├── 2010_2011.json
│       ├── 2011_2012.json
│       └── ...
└── [seasonal folders]/ # Original JSON sources from openfootball
```

### Repository Navigation

- `src/main.py`: Main entry point for the data enrichment pipeline.
- `src/api_clients.py`: Implementation of API clients (UEFA, ESPN, Ratings).
- `src/formatter.py`: String formatting logic for lineups, referees, and events.
- `src/conversion_csv.py`: Script to convert the master CSV into seasonal JSON files.
- `tests/test_integration.py`: Automated integration tests.

---

## 5. Execution Guide

Follow these steps to reproduce the data pipeline and enrich the dataset.

### Step 1: Environment Setup
```bash
# Clone the repository
git clone <repo-url>
cd goaldata-league

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install pandas requests pytest
```

### Step 2: Run Data Enrichment Scraper
The enrichment script uses UEFA and ESPN APIs to fill missing fields in the dataset.
```bash
python src/main.py
```
*Output: `data/processed/champions_league_2011_2025_completed.csv`*

### Step 3: Convert CSV to Seasonal JSON
Split the master dataset into individual JSON files for each season.
```bash
python src/conversion_csv.py
```
*Output: `data/processed/json_seasons/*.json`*

### Step 4: Run Automated Tests
Verify that the pipeline is working correctly and data integrity is maintained.
```bash
pytest tests/test_integration.py
```

---

## 6. Dataset Schema (Processed)

Each match record includes:
- **General Info**: Season, Round, Date, Start/End Time.
- **Teams & Score**: Home/Away names, FT/HT Score, Global Score.
- **Officials**: Full refereeing team (Main and Assistants).
- **Tactical Data**: Full lineups grouped by position, Coaches.
- **Enrichment**: Player ratings (SofaScore/Flashscore).
- **Stats**: Shots, Possession, Fouls, Corners (Total and per team).
- **Events**: Goals, Cards, and Substitutions with timestamps.

---

## 7. License and Ethics

- **Data Origin**: Public match data from Open Football Data and public APIs.
- **Privacy**: No Personally Identifiable Information (PII) is stored. Only professional sports performance data.
- **License**: Public Domain / Academic Use.

---