# Advanced Metrics Sources and Calculation Policy

This enrichment layer adds the requested Big Data football columns to the
processed CSVs only. Raw files are read-only inputs and are not modified.

## Calculation Policy

- Formulas are centralized in `src/advanced_metric_formulas.py`.
- The executable script is `src/enrich_advanced_metrics.py`.
- Outputs are written to:
  - `data/processed/stats/player_match_stats_cleaned.csv`
  - `data/processed/events/goals_events_cleaned.csv`
  - `data/processed/metadata/advanced_metric_sources.csv`
  - `data/processed/metadata/advanced_metric_coverage.csv`
- Missing required inputs produce `NULL`. The pipeline does not invent
  constants, locations, tracking values, or model coefficients.

## Source Notes

### Expected Threat

The xT concept requires event locations and transitions across pitch zones. The
current processed player-match table has aggregate counts but no pass/carry
start and end coordinates, so the repository stores the requested proxy formula
and reports coverage separately.

Source: Soccermatics, "Expected Threat - Position-based":
https://soccermatics.readthedocs.io/en/latest/lesson4/xTPos.html

### VAEP

VAEP values actions by estimating how each action changes short-term scoring and
conceding probabilities. The current data does not contain action-state
sequences, so only the requested aggregate proxy can be evaluated when all its
input columns exist.

Sources:
- socceraction VAEP documentation:
  https://socceraction.readthedocs.io/en/stable/api/generated/socceraction.vaep.VAEP.html
- KDD 2019 paper page:
  https://www.kdd.org/kdd2019/accepted-papers/view/actions-speak-louder-than-goals-valuing-player-actions-in-soccer

### Expected Goals

Production xG models use shot-level populations and shot characteristics such
as location, angle, goalkeeper position, pressure, and body part. The current
`goals_events_cleaned.csv` contains goal events only, not missed shots, so
`xg_probability` and `shot_quality_index` remain `NULL`.

Source: Anzer and Bauer (2021), "A Goal Scoring Probability Model for Shots":
https://www.frontiersin.org/articles/10.3389/fspor.2021.624475/full

### Injury Load and ACWR

ACWR is a workload ratio built from recent acute workload and longer chronic
workload. The processed table currently has `distance_covered` as `NULL` for all
player-match rows, so `acwr_index` remains `NULL`.

Source: Rossi et al. (2018), PLOS ONE:
https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0201264

### Metabolic Power

Metabolic-power methods rely on tracking-derived movement and energetics. Since
`distance_covered` and `top_speed` are missing in the processed match table, the
requested proxy column remains `NULL`.

Source: Osgnach et al. (2010), PubMed record:
https://pubmed.ncbi.nlm.nih.gov/20010116/
