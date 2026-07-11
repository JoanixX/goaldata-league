# GoalData League — Limitations and Future Work

**System:** Football Data Pipeline & Scouting System  
**Delivery:** Week 15 — Final Report  
**Date:** July 2026

---

## 1. Overview

This document provides an honest critique of the GoalData League system's current limitations and outlines concrete research and engineering extensions. Transparency about what the system cannot do is as important as what it can.

---

## 2. Data Coverage Limitations

### 2.1 StatsBomb Event Coverage

StatsBomb Open Data is not a complete census of professional football. It covers:
- Select Champions League seasons (subset of 2004–2022)
- Full event streams for certain domestic league seasons
- 24 competitions total

**Impact:** Real per-90 style rates (shots, passes, tackles, interceptions) are only available for **~1,435 players** covered by StatsBomb-tracked matches (out of 7,086 in the full catalog). The remaining ~5,651 players receive position-median imputed values. This creates a two-tier system where StatsBomb-covered players have genuine individual fingerprints, and others have shared position profiles that reduce recommendation resolution.

**Evidence:** Recommendation MRR is significantly higher for players with StatsBomb coverage than for those relying on imputation.

**Mitigation path:** License additional StatsBomb data or integrate Opta / Wyscout event streams for broader coverage.

### 2.2 Historical Match Coverage Gap

Match results exist for 94,525 games dating back to 2004, but player-level participation data (who played, for how many minutes) only comes from StatsBomb events and FBref season totals. Matches from before 2011 have real scorelines but no player-match resolution.

**Impact:** Pre-2011 player profiles cannot be individually reconstructed; they exist only as squad-level aggregates.

### 2.3 FBref Data Latency

FBref data accessed via `soccerdata` is scraped from the public website. Current season data may be delayed by 24–48 hours after match completion. This limits the system's ability to reflect in-season form for mid-season scouting.

### 2.4 Geographic Bias

The pipeline focuses on the Big 5 European leagues (Premier League, La Liga, Bundesliga, Serie A, Ligue 1) plus the Champions League and select international competitions. It excludes:
- Eredivisie, Primeira Liga, Premier Liga (Russian), J-League
- South American leagues (other than Copa América participants)
- African, Asian, and MLS competition data

This creates a survivorship bias: only players who appeared in covered competitions are in the catalog. Emerging talent from excluded leagues is invisible to the recommender.

---

## 3. Modeling Limitations

### 3.1 PCA Assumptions

Principal Component Analysis assumes linear relationships among features. Football performance is not purely linear — a player's value may derive from complex non-linear interactions (e.g., pressing intensity combined with positional awareness) that PCA collapses into a weighted sum.

Additionally, PCA is sensitive to the **choice of feature set**. The current 33 features cover volume, efficiency, and discipline. Missing:
- Spatial features (average position, heat maps, territory coverage)
- Temporal features (form trends, fatigue, injury history)
- Opponent-adjusted statistics (performance vs. top-5 vs. bottom-5 opposition)

### 3.2 K-Means Cluster Shape Assumption

K-Means minimizes within-cluster sum of squares, which implicitly assumes roughly spherical, equally-sized clusters in Euclidean space. Player profiles form continuous gradients rather than discrete spheres — a deep-lying midfielder and an advanced midfielder exist on a continuum, not in separate bubbles.

**Evidence:** DBSCAN with a moderate eps parameter mostly assigns all players to one dense cluster, confirming the data is one continuous manifold rather than a collection of disconnected blobs.

**Alternative:** Gaussian Mixture Models (soft cluster membership) or hierarchical clustering may better capture the gradient structure.

### 3.3 Recommendation System Scope

The current recommender is a **cross-sectional item-item system**: it retrieves the most statistically similar player in their current season. It does **not** model:
- **Player trajectory:** a 22-year-old with an improving trend may be a better signing than a 30-year-old at peak
- **Positional fit:** two statistically similar midfielders may play completely different roles in different formations
- **Physical attributes:** height, pace, and physicality are absent from the feature set
- **Contract/availability:** the recommender suggests similar players regardless of transfer feasibility

### 3.4 Graph Centrality Interpretation

Graph centrality measures **statistical typicality within the sampled player pool**. It is not a measure of player quality, market value, or tactical fit. A player with high PageRank is one whose statistical profile resembles many others — which can mean they are "typical" (potentially replaceable) rather than elite.

This is explicitly documented in the graph analysis report, but it is a common misinterpretation risk in presentations.

### 3.5 ILP Lineup Optimization

The ILP objective function is a composite rating built from season-aggregate statistics. It does not account for:
- Player chemistry or historical co-performance
- Opponent formation and tactical match-up
- Fatigue, suspension, or injury risk
- Home/away performance differential
- Set-piece specialists or other situational roles

The resulting "optimal XI" is optimal only with respect to the defined rating formula under the formation constraint — it is a starting point for decision-making, not a definitive squad selection.

---

## 4. Evaluation Limitations

### 4.1 Leave-One-Out Query Set Size

The recommendation evaluation uses a leave-one-out protocol: a player is a valid query only if they appear in ≥2 seasons (so there is a holdout season to retrieve). Only 1,662 of 3,670 player-seasons meet this criterion (~45%). Players with exactly one season in the catalog are never evaluated, so MRR and recall metrics underestimate real-world performance for the majority of players.

### 4.2 Ground Truth Sparsity

The ground truth label "same player in another season" is a proxy for similarity, not a true measure of scouting equivalence. A 28-year-old Modrić and a 22-year-old Modrić are labeled as similar by the protocol, even though a scout might not consider them interchangeable as targets.

### 4.3 Lack of Human Expert Validation

The evaluation is fully automated against statistical ground truth. No human expert (scout, coach, analyst) has reviewed the specific recommendations for football validity. This is the highest-risk gap for commercial deployment.

---

## 5. Future Work

### 5.1 Expected Threat (xT) Modeling

**Priority: HIGH**

Expected Threat (Singh 2019) assigns a value to each ball position on the pitch based on the probability of scoring from that zone. Incorporating xT would:
- Replace the current per-90 shot volume proxy with a genuine chance-creation measure
- Enable separating chance creators (high xT from carries/passes) from finishers (high xT from shots)
- Require pitch coordinate data — available in StatsBomb events already ingested

**Implementation:** Discretize the pitch into a grid, compute transition probabilities using the StatsBomb event stream (already at 1.75M events), and assign xT delta to each action.

### 5.2 Player Development Trajectory Modeling

**Priority: HIGH**

Replace the single cross-sectional season profile with a time-series representation:
- LSTM or Transformer encoder over 3–5 seasons of statistics
- Predict next-season performance, enabling "buy low" identification
- Age-adjusted curves (peak age varies by position: forwards peak ~24–26, defenders ~27–30)

### 5.3 Opponent-Adjusted Statistics

**Priority: MEDIUM**

Normalize statistics by opponent strength:
- Goals scored against top-5 defenses are weighted more than against bottom-5
- Requires opposition quality ratings (Elo or similar) for every match in the catalog

### 5.4 Transfer Market Integration

**Priority: MEDIUM**

Add market value data (Transfermarkt) to the ILP optimizer as a budget constraint. Enable:
- Pareto-optimal squad selection (maximize rating subject to budget cap)
- Value-for-money ranking (rating per million euros)

### 5.5 Spatial Feature Engineering

**Priority: MEDIUM**

From StatsBomb coordinates:
- Average player position (x, y centroid per match)
- Heat map features (territory coverage, pressing lines)
- Progressive distance covered (passes/carries into final third)

These would significantly improve the PCA embedding for position-hybrid roles (false nines, inverted wingers, half-backs).

### 5.6 Real-Time Data Pipeline

**Priority: LOW (production requirement)**

Replace the weekly batch job with an event-driven architecture:
- Apache Kafka for streaming match events
- Apache Flink or Spark Structured Streaming for real-time feature aggregation
- Incremental PCA updates without full re-computation

### 5.7 Computer Vision Integration

**Priority: RESEARCH**

Use YOLOv8 player detection + DeepSORT tracking from broadcast video to extract:
- Off-ball movement (pressing distance, space creation)
- Tactical positioning (defensive line height, width)
- Physical metrics (sprint counts, acceleration)

This would provide data for leagues currently excluded from StatsBomb coverage, dramatically expanding geographic scope.

### 5.8 Contextual Recommendation

Add formation-aware and role-aware recommendation filters:
- Filter candidate pool by "same tactical role in same formation" rather than broad position group
- Use formation data from StatsBomb lineups (already ingested) as an additional constraint

---

## 6. Summary Table

| Limitation | Severity | Mitigation |
|-----------|----------|-----------|
| StatsBomb covers only 1,435 players | HIGH | License more event data |
| Pre-2011 player gaps | MEDIUM | Accept as historical limit |
| PCA assumes linearity | MEDIUM | Try GMM / UMAP |
| No trajectory modeling | HIGH | Implement xT + LSTM |
| No human expert validation | HIGH | Commission scout review |
| No spatial features | MEDIUM | Add from StatsBomb coords |
| No opponent adjustment | MEDIUM | Add Elo normalization |
| No transfer budget constraint | LOW | Add Transfermarkt data |
| Batch only (no real-time) | LOW | Kafka + Flink for production |
