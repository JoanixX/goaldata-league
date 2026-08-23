# GoalData League — Monitoring & Operationalization Plan

**System:** Football Data Pipeline & Scouting System  
**Audience:** MLOps, Data Engineering, Team Leads  

---

## 1. Overview

This document describes how the GoalData League pipeline would be operationalized in a production environment. It covers:

1. Scheduled execution triggers
2. Data quality monitoring gates
3. Model performance drift detection
4. Alerting and escalation
5. Incident response

The pipeline is currently batch-oriented (no real-time requirement), with natural refresh cadence aligned to football season calendar.

---

## 2. Execution Schedule

### 2.1 Batch Trigger Calendar

| Job | Trigger | Command |
|-----|---------|---------|
| Full match ingestion | End of match day (22:00 local) | `python -m src.download_football_data` |
| StatsBomb event refresh | Weekly (Monday 01:00) | `python -m src.ingest_statsbomb_full` |
| FBref season stats | Bi-weekly (Monday 02:00) | `python -m src.ingest_real_player_data` |
| FBref keeper stats (real GK) | Bi-weekly (Monday 02:20) | `python -m src.ingest_fbref_keepers` |
| Understat per-match refresh | Weekly (Monday 02:40) | `python -m src.ingest_understat` |
| UEFA coefficients | Once per season | `python -m src.ingest_uefa_coefficients` |
| Real-only dataset rebuild | Weekly (Monday 03:00) | `python -m src.build_real_only_datasets` |
| PCA + clustering refresh | Weekly (Monday 04:00) | `python -m src.build_pca_feature_matrix && python -m src.build_clustering_analysis` |
| Recommendation re-index | Weekly (Monday 05:00) | `python -m src.recommendation_evaluation` |
| Graph re-build | Weekly (Monday 05:30) | `python graph/build_similarity_graph.py && python graph/analyze_similarity_graph.py` |
| Dashboard data refresh | After graph rebuild | `python -m src.serialize_demo_data` |

### 2.2 Cron Configuration (Linux/Unix)

```cron
# Match ingestion — daily at 22:00
0 22 * * * cd /opt/goaldata && python -m src.download_football_data >> logs/cron_ingest.log 2>&1

# Full weekly pipeline — Monday 01:00 to 06:00
0 1 * * 1 cd /opt/goaldata && python -m src.ingest_statsbomb_full >> logs/cron_statsbomb.log 2>&1
0 2 * * 1 cd /opt/goaldata && python -m src.ingest_real_player_data >> logs/cron_fbref.log 2>&1
0 3 * * 1 cd /opt/goaldata && python -m src.build_real_only_datasets >> logs/cron_rebuild.log 2>&1
0 4 * * 1 cd /opt/goaldata && python -m src.build_pca_feature_matrix >> logs/cron_pca.log 2>&1
30 4 * * 1 cd /opt/goaldata && python -m src.build_clustering_analysis >> logs/cron_cluster.log 2>&1
0 5 * * 1 cd /opt/goaldata && python -m src.recommendation_evaluation >> logs/cron_rec.log 2>&1
30 5 * * 1 cd /opt/goaldata && python -m src.serialize_demo_data >> logs/cron_demo.log 2>&1
```

### 2.3 Free-Tier Implementation (GitHub Actions)

The same job graph runs at zero cost on GitHub Actions for public repositories, replacing the
dedicated server assumed above. One workflow with a `schedule:` trigger covers the weekly
refresh; each step is the same documented command:

```yaml
# .github/workflows/weekly-pipeline.yml (reference — enable only if weekly refresh is desired)
on:
  schedule:
    - cron: "0 3 * * 1"   # Monday 03:00 UTC
  workflow_dispatch: {}
jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { lfs: true }
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt
      - run: python -m src.build_pca_feature_matrix
      - run: python -m src.build_clustering_analysis
      - run: python -m src.recommendation_evaluation
      - run: python -m src.supervised_evaluation
      - run: python -m src.serialize_demo_data
```

The heavy ingestion/rebuild steps stay manual (they exceed free-runner time budgets and the
underlying open datasets change rarely). Quality-gate JSONs (`logs/*.json`,
`artifacts/*.json`) double as CI assertions: a gate script exiting non-zero fails the workflow,
which is the free-tier equivalent of the CRITICAL alerts in §6 (GitHub sends failure e-mails at
no cost). This workflow is intentionally **not** committed by default to avoid burning Actions
minutes on a course project; the demo deployment workflow (`deploy-pages.yml`) is the only CI
that runs automatically.

---

## 3. Data Quality Gates

Each pipeline step has automated quality gates. A gate **failure blocks the next step** and triggers an alert.

### 3.1 Ingestion Gate

| Check | Threshold | Action on Failure |
|-------|-----------|-------------------|
| New matches ingested ≥ N expected | < 80% of expected | Warn + retry in 1h |
| Zero HTTP 429 / 503 errors | Any error | Exponential backoff, max 3 retries |
| Match schema completeness | Null ratio < 5% for `home_score`, `away_score` | Block rebuild, page on-call |

### 3.2 Dataset Rebuild Gate

| Check | Expected | Alert |
|-------|----------|-------|
| Zero invented entities (`Squad NN` placeholders) | 0 rows | CRITICAL — stops pipeline |
| Zero duplicate player identities after canonicalization | 0 duplicates | CRITICAL |
| `goals_events_rows` within ±5% of previous week | Sudden spike/drop | WARNING |
| `statsbomb_events_real` rows ≥ 1,500,000 | Always | CRITICAL |
| `player_match_stats` rows ≥ 1,500,000 and ≤ 3,000,000 | Out of band | CRITICAL |
| Per-player season goal sums == real FBref totals (`season_total_match_rate` ≥ 0.98) | Below 0.98 | CRITICAL |
| Zero null in numeric columns | 0 nulls after fill | WARNING |

Implementation: `logs/realistic_rebuild_report.json` is parsed by a lightweight CI check:

```bash
python -c "
import json, sys
r = json.load(open('logs/realistic_rebuild_report.json'))
v = r['verification']
if not v['player_goals_equal_real']:
    print('GATE FAIL: goals mismatch'); sys.exit(1)
print('Gate passed:', v)
"
```

### 3.3 PCA Gate

| Check | Threshold | Alert |
|-------|-----------|-------|
| Rows in feature matrix ≥ 3,500 | Fewer rows than expected | WARNING |
| Cumulative variance at 11 PCs ≥ 0.88 | Below 88% | WARNING — model degrading |
| PC1 top loading is volume-based feature | Manual spot-check monthly | — |

### 3.4 Recommendation Gate

| Check | Expected | Alert |
|-------|----------|-------|
| Stronger MRR ≥ 0.15 | Drop below 0.12 | WARNING — potential data issue |
| PosPurity@5 ≥ 0.99 | Drop below 0.95 | CRITICAL — pool contamination |
| n_queries ≥ 1,500 | Too few multi-season players | INFO |

---

## 4. Data Drift Detection

### 4.1 Input Distribution Drift

Monitor the distribution of key features in the player season feature matrix over time.

```python
# Weekly snapshot comparison (PSI — Population Stability Index)
from scipy.stats import ks_2samp

def check_drift(baseline_col, current_col, threshold=0.1):
    stat, p = ks_2samp(baseline_col.dropna(), current_col.dropna())
    return {"ks_stat": stat, "p_value": p, "drifted": stat > threshold}
```

| Feature | Drift Threshold (KS) | Action |
|---------|---------------------|--------|
| `goals_per90` | 0.10 | Retrain PCA if drifted |
| `passes_completed_per90` | 0.10 | Retrain PCA if drifted |
| `minutes_played` | 0.15 | Warn if season transition |
| Position distribution | Chi-sq p < 0.01 | Investigate new positions |

### 4.2 Recommendation Performance Drift

Run the offline leave-one-out evaluation weekly. Compare to the previous week's MRR.

| Metric | Baseline | Yellow Alert | Red Alert |
|--------|----------|-------------|-----------|
| MRR | 0.179 | < 0.15 | < 0.10 |
| Recall@5 | 0.094 | < 0.07 | < 0.04 |
| PosPurity@5 | 0.998 | < 0.98 | < 0.90 |

**Drift response:** If yellow or red alert, check for:
1. New players without position labels (position_group = NULL)
2. Sudden influx of simulated/imputed player rows lowering real signal
3. Data source schema change (FBref / StatsBomb API update)

### 4.3 Graph Topology Drift

| Metric | Expected | Alert |
|--------|----------|-------|
| `num_nodes` | ~3,670 ± 200/wk | Spike > 500 in one week |
| `num_connected_components` | 1 | > 1 component |
| `density` | 0.0055 ± 0.001 | Large delta |
| `isolated_nodes` | 0 | Any isolated node |

---

## 5. Health Monitoring Dashboard

The following metrics should be tracked in a monitoring tool (e.g. Grafana, Datadog):

```
goaldata.pipeline.rebuild.duration_seconds
goaldata.pipeline.rebuild.goals_allocated_total
goaldata.pipeline.pca.rows
goaldata.pipeline.pca.variance_at_11
goaldata.pipeline.rec.mrr
goaldata.pipeline.rec.recall_at_5
goaldata.pipeline.rec.pos_purity
goaldata.pipeline.graph.num_nodes
goaldata.pipeline.graph.num_edges
goaldata.pipeline.graph.isolated_nodes
```

Each metric emitted as a Prometheus counter/gauge via a lightweight wrapper:

```python
# logs/metrics.jsonl — append-only line-delimited metrics
import json, datetime
def emit_metric(name: str, value: float, tags: dict = {}):
    with open("logs/metrics.jsonl", "a") as f:
        f.write(json.dumps({
            "ts": datetime.datetime.utcnow().isoformat(),
            "name": name, "value": value, **tags
        }) + "\n")
```

---

## 6. Alerting and Escalation

### 6.1 Alert Channels

| Severity | Channel | Response Time |
|----------|---------|---------------|
| INFO | Slack `#pipeline-logs` | Next business day |
| WARNING | Slack `#pipeline-alerts` | Same day |
| CRITICAL | PagerDuty + Email | Within 1 hour |

### 6.2 Critical Alert Conditions

| Condition | Severity |
|-----------|----------|
| Goals mismatch: allocated != real total | CRITICAL |
| Pipeline step fails with unhandled exception | CRITICAL |
| PCA cumulative variance < 80% | CRITICAL |
| PosPurity@5 < 0.90 | CRITICAL |
| Graph splits into > 5 components | CRITICAL |
| Any `data_provenance` column missing | CRITICAL |

---

## 7. Incident Response Playbook

### 7.1 Goals Mismatch

```
1. Inspect logs/realistic_rebuild_report.json → verification.player_goals_equal_real
2. Check if new matches were added with NULL home_score/away_score
3. Patch null scores from secondary source (football-data.co.uk)
4. Re-run: python -m src.rebuild_realistic_datasets
5. Re-verify gate
```

### 7.2 MRR Drops Below 0.12

```
1. Check if new player-seasons were added without StatsBomb coverage
2. Run: python -m src.recommendation_evaluation --verbose
3. Inspect recommendation_error_analysis.md for failure patterns
4. If caused by data issue: re-run rebuild + PCA + clustering
5. If caused by schema drift: update feature engineering pipeline
```

### 7.3 Graph Fragmentation (Multiple Components)

```
1. Run: python graph/analyze_similarity_graph.py --check-components
2. Lower k-NN threshold to re-connect: --threshold 0.10
3. If isolated nodes remain, check if new players have degenerate PCA vectors (all zeros)
4. Remove zero-vector rows from clustering input before graph rebuild
```

---

## 8. Backup and Recovery

| Artifact | Backup Frequency | Retention |
|----------|-----------------|-----------|
| `data/processed/core/*.parquet` | Daily | 90 days |
| `artifacts/*.csv` | Weekly | 52 weeks |
| `artifacts/*.json` | Weekly | 52 weeks |
| `logs/*.json` | Daily | 180 days |
| `reports/demo/data.js` | Weekly | 52 weeks |

Recovery: restore from latest successful backup, re-run pipeline from failing step onward.

---

## 9. Security Considerations

- All data sources accessed via HTTPS
- No PII beyond public football player names
- StatsBomb Open Data: CC BY-SA 4.0 license (attribution required)
- FBref data: accessed via `soccerdata` wrapper, rate-limited to respect scraping policies
- No API keys or credentials stored in the repository (use `.env` file + `.gitignore`)
