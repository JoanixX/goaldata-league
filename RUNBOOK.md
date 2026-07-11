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
| `data/processed/core/players_cleaned.parquet` | Real player profiles (FBref-sourced) |
| `data/processed/core/teams_cleaned.parquet` | 1,538 real teams |

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

## Step 2 — Rebuild Realistic Datasets

This is the **core pipeline step**. It:
- Anchors all goals to real match scorelines (sum per team == real score)
- Resolves duplicate player identities (CR7 / CristianoRonaldo to one canonical ID)
- Applies real FBref per-90 rates (shots, passes, tackles) from StatsBomb
- Tags every row with `data_provenance`

```bash
python -m src.rebuild_realistic_datasets
```

Expected output:
```
Loading anchors from committed parquet...
Resolving player identities (name normalisation + fuzzy merge)...
  merged N duplicate player ids into canonical entities
Allocating goals to real scorelines...
  allocated 254,xxx player-goals (real total = 254,xxx)
Rebuilding base player_match (shots/cards consistent)...
Building goal-events base (real-sized goal table)...
Writing matches: 94,525 rows x N cols
Writing players: N rows x N cols
```

Output files written to `data/processed/core/` (CSV + Parquet).  
**Verification report:** `logs/realistic_rebuild_report.json`

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
python tests/api_diagnostics/run_all_tests.py
```

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
│   ├── rebuild_realistic_datasets.py   # Step 2 — data rebuild
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
