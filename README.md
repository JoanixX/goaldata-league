# GoalData League — Football Data Pipeline & Scouting System

This repository contains a data engineering + analytics pipeline for football
(Champions League and domestic leagues). It merges real match records (final
scores, player profiles, FBref season totals) into a relational schema of 94,525
real matches (254,596 real goals at match level), 18,782 real players (FBref Big-5
rosters 2005-2025 + StatsBomb, identity-deduped), a 1,935,463-row player-match
participation table (real rosters × real fixtures; per-player season totals equal
the real FBref numbers), and a 1,751,751-row real StatsBomb event stream, then
builds a representation (PCA), segmentation (clustering) and a player-similarity
recommender on top.

> **Data policy (real-only / commercial-grade).** Player and team identities,
> participations, goals and assists are **always real, never simulated**. The
> >=1.5M dataset is a **real StatsBomb event stream** — `data/processed/events/`
> `statsbomb_events_real.parquet`, **1,751,751 real actions** across **24 real
> competitions** (Champions League, La Liga, FIFA World Cup, UEFA Euro, Copa
> América, Europa League, Premier League, Serie A, Ligue 1, Bundesliga, Copa del
> Rey, MLS, … from 2005 onward). Invented placeholder players were removed and
> identities de-duplicated (Cristiano Ronaldo / CR7 / CristianoRonaldo → one id).
> Only allowed **secondary** metrics (touches, possession, cards, fouls, offsides,
> shots where a match has no real source) may be modelled via cited formulas /
> imputation. Real ingestion: `python -m src.ingest_statsbomb_full` then
> `python -m src.build_real_only_datasets`. See `reports/methodology_and_citations.md`,
> `reports/DEFENSE_BRIEF.md` and `data/dictionary.txt`.

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
- **League-Strength Adjustment (UEFA)**: Decision-layer ratings (ILP starting XI) are scaled by the official season-specific UEFA country coefficients (`src/league_strength.py` + `src/ingest_uefa_coefficients.py`), so 50 goals in a weaker league do not outrank 39 in the Premier League; Champions League minutes carry a premium anchored to UEFA's own CL:EL:Conference bonus ratios.

## Final Deliverables (Week 14)

| Deliverable | Location |
|-------------|----------|
| Final technical report | `reports/FINAL_TECHNICAL_REPORT.md` |
| Runbook (canonical reproduction path) | `RUNBOOK.md` |
| Final presentation | `reports/FINAL_PRESENTATION.md` |
| Demo — static dashboard (no server) | `reports/demo/index.html` — auto-deployed to GitHub Pages: https://joanixx.github.io/goaldata-league/ (see `RUNBOOK.md` Step 13) |
| Demo — interactive Streamlit app | `app.py` — live at https://tf-goal-data-league.streamlit.app/ (`streamlit run app.py` for local; `RUNBOOK.md` Step 13b) |
| Monitoring / operationalization plan | `reports/MONITORING_PLAN.md` |
| Limitations and future work | `reports/LIMITATIONS_FUTURE_WORK.md` |
| Metrics (auto-generated from artifacts) | `reports/METRICS_REPORT.md` |

## Quick Start

**`RUNBOOK.md` is the canonical, verified reproduction path.** Short version:

1. Install dependencies: `pip install -r requirements.txt`
2. (Optional — the committed parquet already contain the result) Rebuild the data layer:
   `python -m src.ingest_statsbomb_full`, `python -m src.build_real_only_datasets` (real StatsBomb
   observed layer), then `python -m src.ingest_real_player_data` and
   `python -m src.build_roster_participation_datasets` (≥1.5M real-roster participation layer)
   - Zero invented entities; identities de-duplicated (`CristianoRonaldo`/`cristiano_ronaldo`/`CR7` -> one id) with nation-blocked homonym separation; per-player season totals anchored to real FBref numbers. The earlier `python -m src.rebuild_realistic_datasets` stage is **superseded** and kept only as remediation history. See `reports/methodology_and_citations.md`.
3. PCA feature matrix: `python -m src.build_pca_feature_matrix`
4. Clustering validation (K-Means + DBSCAN sweeps): `python -m src.build_clustering_analysis`
5. Recommendation offline evaluation: `python -m src.recommendation_evaluation` (example queries: `python -m src.recommendation_engine`)
6. Supervised position-classification evaluation: `python -m src.supervised_evaluation`
7. Similarity graph + centralities: see `RUNBOOK.md` Step 7
8. ILP starting XI: `python -m src.optimize_lineup --season 2021-2022 --formation 4-3-3`
9. Demo data + dashboard: `python -m src.serialize_demo_data`, then open `reports/demo/index.html`
10. Tests: `python -m pytest tests -q --ignore=tests/api_diagnostics --ignore=tests/scrapers`

Auxiliary (only when re-ingesting or scraping): `python script.py` (initial relational build),
`python src/impute_missing_stats.py`, `python src/enrich_processed_features.py`,
`python src/enrich_advanced_metrics.py`, `python src/main.py` (scraper pipeline),
`python src/data_merge.py path/to/scraper_results.json`, and
`python tests/api_diagnostics/run_all_tests.py` (network-bound diagnostics).

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
Observed rows and non-empty cells are preserved. Identities, participations, goals
and assists are always real (see the data policy above). Only allowed secondary
metrics may be modelled, from documented, literature-anchored formulas (never
unconditioned random constants), marked through the `data_provenance` column, and
validated with `logs/data_quality_report.json` and `logs/realistic_rebuild_report.json`.
See `reports/methodology_and_citations.md`, `reports/data_pipeline_flow.md`, and
`reports/missing_data_policies.md`.

### About the 1.5M-row target
Two tables exceed 1.5M rows, both built exclusively from real entities:

- **`statsbomb_events_real.parquet` — 1,751,751 real event-stream actions** (fully observed).
- **`player_match_stats_cleaned` — 1,935,463 rows** = 86,137 fully observed StatsBomb
  participations + 1,849,326 real-roster participations (`python -m
  src.build_roster_participation_datasets`): every real FBref Big-5 squad member
  (2005-2025) × his club's real deduplicated fixtures. Identities are 100% real and
  deduped (nation-blocked homonym handling); **per-player season sums of goals,
  assists, shots, and cards equal the real FBref season totals** — the per-match
  split is the modelled part, provenance-tagged as `derived_real_roster_scoreline`.
  Verification: `logs/roster_participation_report.json` (0 invented names, row band,
  goal-total anchoring rate).

`goals_events_cleaned` (75,925 scoring rows) and `matches_cleaned` (94,525 real
matches, 254,596 real goals at match level) sit at their natural sizes — goal counts
are physical quantities and are never padded.

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
