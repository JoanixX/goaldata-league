# GoalData League — Runbook

**System:** Football Data Pipeline & Scouting System  
**Version:** Week 15 — Final Delivery  
**Python:** 3.10+  
**OS:** Windows / Linux / macOS (paths use `/`; substitute `\` on Windows)

---

## Prerequisites

| Tool | Version |
|------|---------|
| Python | ≥ 3.10 |
| pip | latest |
| Git | any |

All Python dependencies are pinned in `requirements.txt`. No Docker or external database is required.

---

## Step 0 — Clone and Install

```bash
git clone <repo-url> goaldata-league
cd goaldata-league
pip install -r requirements.txt
```

Verify installation:

```bash
python -c "import pandas, sklearn, networkx, pulp; print('OK')"
```

---

## Step 1 — Inspect Committed Data Anchors

The repository commits three real-data parquet anchors:

| File | Description |
|------|-------------|
| `data/processed/core/matches_cleaned.parquet` | 94,525 real matches with final scores |
| `data/processed/core/players_cleaned.parquet` | 18,782 real player profiles (FBref 2005-2025 + StatsBomb, identity-deduped) |
| `data/processed/core/teams_cleaned.parquet` | 1,338 real teams |

These are the ground-truth anchors for every downstream step.

```bash
python -c "
import pandas as pd
m = pd.read_parquet('data/processed/core/matches_cleaned.parquet')
p = pd.read_parquet('data/processed/core/players_cleaned.parquet')
t = pd.read_parquet('data/processed/core/teams_cleaned.parquet')
print('Matches:', len(m), '| Players:', len(p), '| Teams:', len(t))
"
```

---

## Step 2 — Rebuild the Data Layer (optional — committed parquet already contain the result)

This is the **core pipeline step**, in two stages:

**2a. Real-only observed layer** — enforces the real-only data policy:
- Removes every invented entity (`{team} {season} Squad NN` placeholder players)
- Resolves duplicate player identities (CR7 / CristianoRonaldo to one canonical ID)
- Real StatsBomb participations (86,137 rows) and real event-stream goals

```bash
# Requires data/raw StatsBomb inputs (not versioned) — ingest first:
python -m src.ingest_statsbomb_full
python -m src.build_real_only_datasets
```

**2b. Real-roster participation layer (≥1.5M rows)** — expands real FBref Big-5
rosters (2005-2025) over their clubs' real deduplicated fixtures:
- 100% real identities, nation-blocked homonym separation (two same-named players
  from different countries stay distinct entities)
- Per-player season sums of goals/assists/shots/cards **equal the real FBref
  season totals** (verified in `logs/roster_participation_report.json`)
- Observed StatsBomb rows always take precedence over derived rows

```bash
python -m src.ingest_real_player_data          # FBref Big-5 rosters 2005-2025
python -m src.build_roster_participation_datasets
```

Skip this step entirely when working from a fresh clone: the committed LFS parquet
under `data/processed/` are the verified output of this step.

Resulting sizes: 18,782 players · 1,338 teams · **1,935,463 player-match rows** ·
75,925 player-attributed scoring rows · 52,387 player-seasons · 1,751,751
event-stream rows.

> **Deprecated:** the earlier `python -m src.rebuild_realistic_datasets`
> (scoreline-anchored multinomial goal allocation, `logs/realistic_rebuild_report.json`)
> was the Week-10 remediation stage and is superseded by the stages above.
> Do not run it on top of the current tables — it would reintroduce simulated
> per-player allocations.

---

## Step 3 — Enrich Advanced Metrics (optional)

Adds xG proxy, progressive pass index, etc. where real event data is present:

```bash
python src/enrich_advanced_metrics.py
```

---

## Step 4 — Build PCA Feature Matrix

Produces the 11-component PCA embedding used by clustering and recommendations:

```bash
python -m src.build_pca_feature_matrix
```

Key outputs:
- `artifacts/pca_player_season_2d.csv` — all 11 PC coordinates per player-season
- `artifacts/pca_explained_variance.csv` — variance per component
- `artifacts/pca_component_loadings.csv` — feature loadings
- `artifacts/pca_player_season_2d.png` — 2D scatter plot

---

## Step 5 — Clustering Analysis (K-Means + DBSCAN)

Runs parameter sweeps and selects the best model by silhouette score:

```bash
python -m src.build_clustering_analysis
```

Key outputs:
- `artifacts/player_season_cluster_labels.csv` — PC coords + cluster IDs per player-season
- `artifacts/clustering_kmeans_2d.png` — K-Means scatter
- `artifacts/clustering_dbscan_2d.png` — DBSCAN scatter
- `artifacts/clustering_kmeans_silhouette.png` — silhouette sweep
- `reports/clustering_validation_report.md`

---

## Step 6 — Recommendation Offline Evaluation

Runs leakage-safe leave-one-out evaluation of baseline vs. stronger recommender:

```bash
python -m src.recommendation_evaluation
```

Key outputs:
- `artifacts/recommendation_evaluation.json`
- `reports/recommendation_evaluation_report.md`
- `reports/recommendation_error_analysis.md`

To run example queries interactively:

```bash
python -m src.recommendation_engine
```

Supervised representation probe (position classification, real subset in the 0.85–0.95 band):

```bash
python -m src.supervised_evaluation
```

Outputs: `artifacts/supervised_evaluation.json`, `reports/supervised_evaluation_report.md`

---

## Step 7 — Build Similarity Graph

Constructs the player-similarity k-NN graph and computes centralities:

```bash
python graph/build_similarity_graph.py \
  --input-csv artifacts/player_season_cluster_labels.csv \
  --out-graph data/processed/similarity_graph.pkl \
  --k-neighbors 8 \
  --threshold 0.15

python graph/analyze_similarity_graph.py
```

Key outputs:
- `artifacts/graph_centralities.csv`
- `artifacts/graph_statistics.json`
- `artifacts/graph_ranking_comparison_full.csv`

---

## Step 8 — ILP Starting-XI Optimization

Selects the optimal 11-player lineup using Integer Linear Programming:

```bash
python -m src.optimize_lineup --season 2021-2022 --formation 4-3-3
```

Output: `artifacts/optimal_xi_2021-2022_4-3-3.csv`

---

## Step 9 — Passing Network and xG

Builds match-level passing networks and player xG from StatsBomb events:

```bash
python -m src.build_passing_network
```

Outputs:
- `artifacts/passing_network_Liverpool_22912.csv`
- `artifacts/passing_network_Tottenham_Hotspur_22912.csv`
- `artifacts/player_xg_22912.csv`

---

## Step 10 — Generate Demo Data

Serializes all artifact data into a single self-contained JavaScript file
for the interactive HTML dashboard:

```bash
python -m src.serialize_demo_data
```

Output: `reports/demo/data.js`

---

## Step 11 — Open Interactive Dashboard

No server required. Just open in a browser:

```bash
# Windows
start reports/demo/index.html

# macOS
open reports/demo/index.html

# Linux
xdg-open reports/demo/index.html
```

The dashboard shows:
- PCA 2D scatter with cluster coloring and position filtering
- Graph centrality leaderboard
- Optimal Starting XI (4-3-3)
- Passing network visualization (Liverpool UCL)
- Player similarity search

---

## Step 12 — Run Diagnostic Tests

```bash
# Core reproducibility gate (offline, fast):
python -m pytest tests -q --ignore=tests/api_diagnostics --ignore=tests/scrapers

# Network-bound source diagnostics (optional):
python tests/api_diagnostics/run_all_tests.py
```

---

## Step 13 — Deploy the Demo (free tier only)

### 13a. Static dashboard → GitHub Pages (primary, zero cost)

The workflow `.github/workflows/deploy-pages.yml` publishes `reports/demo/` to GitHub Pages on
every push to `main` that touches the demo. One-time setup by the repo owner:

1. GitHub → repo **Settings → Pages → Build and deployment → Source: GitHub Actions**
2. Push to `main` (or run the workflow manually from the Actions tab)
3. The dashboard is served at `https://<owner>.github.io/goaldata-league/`

The demo is fully self-contained (`data.js` is inlined) — no build step, no server, no cost.

### 13b. Streamlit app → Streamlit Community Cloud (optional, also free)

1. Sign in at https://share.streamlit.io with the GitHub account
2. New app → repository `JoanixX/goaldata-league`, branch `main`, main file `app.py`
3. Dependencies install from `requirements.txt` automatically

Note: the app reads LFS-tracked parquet files. If the free tier fails to fetch LFS objects,
keep the Pages dashboard as the deployed demo and run the app locally:
`streamlit run app.py`.

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: rapidfuzz` | Run `pip install rapidfuzz` |
| `FileNotFoundError: ...parquet` | Run Step 2 first |
| `KeyError: team_id` | Already handled in `rebuild_realistic_datasets.py` via fuzzy-match fallback |
| `pulp` solver not found | Run `pip install pulp` and confirm CBC solver is installed |
| Dashboard shows no data | Run Step 10 (`serialize_demo_data`) to regenerate `data.js` |

---

## File Structure Reference

```
goaldata-league/
├── src/                         # Core pipeline scripts
│   ├── build_real_only_datasets.py     # Step 2 — real-only data rebuild
│   ├── build_pca_feature_matrix.py     # Step 4 — PCA
│   ├── build_clustering_analysis.py    # Step 5 — clustering
│   ├── recommendation_engine.py        # Scouting recommender
│   ├── recommendation_evaluation.py    # Step 6 — offline eval
│   ├── optimize_lineup.py              # Step 8 — ILP XI
│   ├── build_passing_network.py        # Step 9 — passing nets
│   └── serialize_demo_data.py          # Step 10 — demo data
├── graph/                       # Graph construction and analysis
├── data/
│   ├── processed/core/          # Cleaned parquet anchors
│   ├── processed/stats/         # Season/match stats
│   └── features/                # Feature matrices
├── artifacts/                   # Generated artifacts (plots, CSVs, JSONs)
├── reports/
│   ├── demo/                    # Interactive HTML dashboard
│   └── *.md                     # All milestone reports
├── tests/                       # Integration and diagnostic tests
├── requirements.txt
└── RUNBOOK.md                   # This file
```

---

## Citation

Pipeline methods:
- Maher (1982) — Poisson scoring model
- Dixon and Coles (1997) — Score modelling
- Decroos et al. (2019) — Action value / VAEP
- Little and Rubin (2002) — Missing data imputation
- StatsBomb Open Data, FBref via `soccerdata`
