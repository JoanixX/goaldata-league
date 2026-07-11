# GoalData League — Final Defense Presentation

**Project:** Football Data Pipeline & Scouting System  
**Delivery:** Week 15 — Academic Defense  
**Format:** Markdown slide deck (one `---` separator = one slide)

---

---

# GoalData League
## Football Data Pipeline & Scouting System

**End-to-End Analytics from Raw Match Data to ILP Squad Selection**

> Week 15 Final Defense  
> July 2026

---

---

## Agenda

1. Problem Statement & Business Case
2. Data Sources & Architecture
3. Data Cleaning & Entity Resolution
4. PCA Representation
5. Tactical Clustering
6. Player Similarity Recommendation
7. Graph Analysis & Centrality
8. ILP Starting-XI Optimization
9. Passing Networks
10. Limitations & Future Work
11. Demo

---

---

## 1. The Problem We Solved

### Traditional scouting is broken

- **94,525 matches** across 10+ leagues, 2004–2025
- Scouts can watch ~200 matches per year — that's **0.2% coverage**
- Player profiles are scattered across StatsBomb, FBref, UEFA, ESPN — incompatible schemas, encoding mismatches, duplicate identities

### Our system answers three questions

| Question | Method |
|----------|--------|
| Who plays like this player? | Content-based similarity (11-PC PCA space) |
| Who are the most tactically central players? | k-NN graph PageRank / betweenness |
| Which 11 players maximize our rating? | Integer Linear Programming |

---

---

## 2. Data Sources & Real-Only Policy

| Source | Type | Volume |
|--------|------|--------|
| StatsBomb Open Data | Event stream | **1,751,751 real actions** |
| FBref via soccerdata | Season totals & rosters (2005-2025) | 54,908 player-season-club rows |
| UEFA / OpenFootball | Match results | 94,525 real matches |

### Non-negotiable constraint

> **Zero invented data.** Player identities, goals, and participations are always real.  
> Only secondary metrics (cards, fouls, shots where no real source exists) are modelled  
> via cited formulas — and tagged with `data_provenance`.

---

---

## 3. What We Fixed (Audit → Remediation)

### The root cause: 96% synthetic entities

| Metric | Before | After |
|--------|--------|-------|
| Real players | 190,979 (~4%) | **18,782 (100%)** |
| Impossible GK seasons (>3 goals/90) | 506 | **0** |
| Duplicate identities (CR7 / CristianoRonaldo) | Unresolved | **0** |
| 1.5M dataset | 1.95M simulated rows | **1.75M real events + 1.94M real-roster rows** |
| Recommender MRR | 0.0018 | **0.179 (×99)** |
| Position purity@5 | 0.59 (random) | **0.998** |
| Position classification macro-F1 (real subset) | — | **0.884** (HistGradientBoosting) |

---

---

## 4. Entity Resolution

### Three-stage deduplication pipeline

```
Raw names
  → Unicode NFC normalization + accent stripping
  → RapidFuzz ratio ≥ 85 within (nationality, birth_year) bucket
  → Canonical ID selection (most-observed spelling wins)
```

### Examples resolved

| Duplicate cluster | Canonical |
|------------------|-----------|
| CR7, CristianoRonaldo, cristiano_ronaldo, Cristiano Ronaldo | Cristiano Ronaldo |
| Alexis Sánchez, Alexis Sanchez | Alexis Sánchez |
| Edinson Cavani, Edison Cavani | Edinson Cavani |

---

---

## 5. PCA Feature Matrix

### 33 features → 11 principal components → 90.2% variance

```
Features: volume (goals, shots, passes) + per-90 rates + efficiency ratios
Pre-processing: winsorize → log1p → RobustScaler
```

| PC | Top loadings | Interpretation |
|----|-------------|----------------|
| PC1 (+) | shots, goals, fouls_committed | Attacking volume & involvement |
| PC2 (+) | defensive_actions/90, passes/90 | Defensive/possesion profile |

### Variance explained

| Components | Cumulative Variance |
|-----------|-------------------|
| 2 | 45.5% |
| 5 | 68.2% |
| 11 | **90.2%** |

> Cluster-0: attackers → Cluster-1: keepers/defenders → Cluster-2: midfield/possession

---

---

## 6. Tactical Clustering (K=3)

### Why K=3?

Silhouette score peaks at K=3 (0.4536); inertia elbow confirms it.

| Cluster | Size | Profile | Example Players |
|---------|------|---------|-----------------|
| **0** | 1,544 | High-volume attackers & dynamic midfielders | Vinícius Jr., Salah, Mbappé |
| **1** | 1,089 | Low-volume defensive & GK profiles | Courtois, Oblak, Mendy |
| **2** | 1,037 | High-possession midfield & central defenders | Modrić, Kroos, Alaba |

### DBSCAN (eps=0.25)

- 2 dense clusters, noise ratio **0.84%**
- Noise cluster captures genuine outliers (extreme one-season profiles)
- Used as a density audit, not a replacement for K-Means

---

---

## 7. Player Similarity Recommendation

### Content-based item-item system

```
Query: player-season
Pool: same position_group (position-aware)
Distance: standardized Euclidean over all 11 PCs
```

### Offline Evaluation (Leave-One-Out, 1,662 queries)

| System | MRR | Recall@5 | NDCG@10 | PosPurity@5 |
|--------|-----|----------|---------|-------------|
| Baseline (2D) | 0.077 | 0.032 | 0.043 | 0.589 |
| **Stronger (11 PCs)** | **0.179** | **0.094** | **0.115** | **0.998** |

**The stronger model is ×2.3 better by MRR and ×3.0 by Recall@5.**

---

---

## 8. Player Similarity Graph

### Construction

- **3,670 nodes** (player-seasons)
- **17,777 edges** (k=8 nearest neighbours, similarity ≥ 0.15)
- **1 connected component** (100% of nodes, 0 isolated)

### Centrality interpretation

| Centrality | Meaning in this graph |
|-----------|----------------------|
| Degree | How many similar profiles exist |
| PageRank | Typicality-prestige (central profiles among central profiles) |
| Betweenness | Bridge between playing styles — versatile/hybrid profiles |
| Eigenvector | Influence from high-prestige neighbors |

### Top by PageRank

Fernandinho, Maximilian Arnold, Kyle Walker, Dayot Upamecano — robust multi-role profiles

---

---

## 9. ILP Starting-XI Optimization

### Formulation

```
Maximize:  Σ rating_i × x_i
Subject to:
  Σ x_i(GK) = 1,  Σ x_i(DEF) = 4
  Σ x_i(MID) = 3,  Σ x_i(FW)  = 3
  x_i ∈ {0, 1}  ∀ i
```

### League-strength adjustment (new)

Ratings are scaled by the **official UEFA country coefficient of that season**
(50 Primeira Liga goals ≠ 39 Premier League goals). Champions League minutes add a
1.10 premium (UEFA's own CL:EL:Conference = 1.5:1.0:0.5 bonus ratios).

### Optimal XI — Season 2021-2022, 4-3-3 (pool: 1,639 real players, UEFA-adjusted)

```
             Alisson (GK)
  Cancelo  Bensebaini  Coufal  Pereira
     T. Hazard  De Bruyne  Palacios
       Haaland  Salah  Schick
```

---

---

## 10. Passing Networks (Real StatsBomb)

### Match 22912: Liverpool vs Tottenham

**Liverpool key playmakers:**

| Player | Weighted Degree | Betweenness | Eigenvector |
|--------|----------------|-------------|-------------|
| Andrew Robertson | **63** | 0.033 | 0.474 |
| Virgil van Dijk | 43 | 0.060 | 0.238 |
| Sadio Mané | 46 | 0.052 | **0.503** |
| Trent Alexander-Arnold | 32 | **0.157** | 0.208 |

> Robertson = volume distributor; Alexander-Arnold = critical bridge; Mané = attacking hub

---

---

## 11. Limitations

### Data coverage
- StatsBomb style rates (shots, passes, tackles/90) cover **1,435 players** in StatsBomb-tracked competitions; remaining players use position-median imputation
- Historical matches (pre-2011 FBref) have no player-level data — only match results

### Model limitations
- K-Means assumes spherical clusters in Euclidean space; football profiles form gradients, not perfect spheres
- Recommendation system is cross-sectional — does not model player development or age trajectories
- Graph centrality measures typicality, not quality: a "central" player is not necessarily better

### Ground truth sparsity
- With few multi-season players in early data, the leave-one-out query set covers ~45% of players; MRR is underestimated for single-season players

---

---

## 12. Future Work

| Direction | Method |
|-----------|--------|
| Expected Threat (xT) modeling | Markov chain on pitch zones |
| Player development tracking | Time-series PCA / LSTM on season sequences |
| Transfer value estimation | Gradient boosted regression on market data |
| Real-time data ingestion | Apache Kafka + streaming aggregation |
| Computer vision scouting | YOLOv8 player tracking from broadcast video |
| Multi-league transfer recommendation | Cross-league normalization + domain adaptation |

---

---

## 13. System Summary

```
StatsBomb (1.75M events)
FBref (54,908 roster rows, 2005-2025)
UEFA / OpenFootball (94,525 matches)
           ↓
Entity Resolution → Schema Normalization → Provenance Tagging
           ↓
PCA (11 components, 90.2% variance)
           ↓
K-Means Clustering (k=3, silhouette=0.454)
           ↓
Similarity Recommender (MRR 0.179, PosPurity 99.8%)
           ↓
k-NN Graph (3,670 nodes, 17,777 edges, 1 component)
           ↓
ILP Optimizer → Optimal Starting XI
           ↓
Interactive Dashboard (reports/demo/index.html)
```

---

---

# Thank You

**Demo:** Open `reports/demo/index.html` in any browser — no server required.

**Reproduce everything** (committed parquet already contain the data-build result):
```bash
python -m src.build_pca_feature_matrix
python -m src.build_clustering_analysis
python -m src.recommendation_evaluation
python -m src.supervised_evaluation
python -m src.serialize_demo_data
```

**Questions?**
