# Review Week 10 — Recommendation / Ranking Engine

**Project:** GoalData League (`https://github.com/JoanixX/goaldata-league.git`)
**Branches reviewed:** `mauricio_branch` vs `main` (Part 1); work implemented on `joaquincito_branch` (Part 2)
**Date:** 2026-06-13
**Deliverable:** Week 10 — *Recommendation, Ranking, or Predictive Decision Engine* (20 pts)

---

## 0. TL;DR

| Topic | Status |
| --- | --- |
| Sync with GitHub | All local branches matched the remote (0 commits of difference). |
| PR `mauricio_branch` -> `main` conflicts? | No. Clean fast-forward; `main` had no commits missing from `mauricio_branch`. |
| Week 10 rubric coverage (original) | Baseline + stronger present, but the highest-value items (offline evaluation, error analysis, written framing) were missing. |
| Recommender quality (original) | Worked but produced unreliable rankings (cosine on 2 PCs gave ~1.0 similarity to almost everyone; mixed positions). |
| Dataset quality (original) | Fabricated data (uniform-random imputation) and a mislabelled `goals_events` (a StatsBomb event-stream, not goals). |

Part 2 documents the remediation implemented on `joaquincito_branch`.

---

## 1. PR analysis: `mauricio_branch` -> `main`

`mauricio_branch` was 2 commits ahead of `main` with no diverging commits on `main`,
so a merge is a clean fast-forward (no conflicts). The branch only **added** the
recommendation scripts and an artifact, and **deleted** a stray root CSV. No
pipeline file, README, or processed data was touched. The issue was never the
merge — it was the content/rubric coverage and the underlying data.

## 2. Week 10 rubric gaps found in the original work

- **Baseline system (1.5):** present (global Euclidean on PC1/PC2) but not justified in writing.
- **Stronger system (2.0):** cosine similarity within the K-Means cluster, on the *same* 2D representation, so not clearly "stronger"; cosine on 2 PCs collapsed almost all similarities to ~1.0.
- **Offline evaluation report (2.5):** absent — no metrics, candidate-pool definition, or train/test logic.
- **Error analysis (1.5):** absent.
- **Task framing (1.0):** only in code comments.
- **Data alignment doc (1.5):** the recommender was not wired into the pipeline or documented.

## 3. Dataset problems found in the original work

- `goals_events_cleaned` was a raw StatsBomb **event stream** (pass/carry/pressure) mislabelled as goals: 1.6M rows over only 1,712 matches (~937 "goals"/match).
- `player_match_stats` showed a **uniform-random fabrication signature** (goals/shots buckets 3/4/5 nearly identical), matching the `dictionary.txt` prompt that instructed random fill.
- Per-match goals did **not** match real scorelines (only 11.6% consistent; player goals were 1.74x the real total).
- `data/dictionary.txt` was an LLM generation prompt, not a real data dictionary.

---

# PART 2 — Changes implemented on `joaquincito_branch`

> This section documents the remediation done on `joaquincito_branch`
> (PR -> `main`, conflict-free). Summary for the defense.

## Root cause

`src/enrich_processed_features.py` **fabricated** data to reach 1.5M rows: synthetic
rosters (`"{team} {season} Squad NN"`), ~1.5M invented player-match rows, and
`rng.choice`/`rng.integers` draws with arbitrary buckets (the uniform 3/4/5 plateau).
`goals_events_cleaned` was a StatsBomb event dump mislabelled as goals. The heavy
raw sources are gone from the repo, so it is not re-ingestable; hence the agreed
approach: **simulate from real anchors with cited models and provenance tags.**

## What was implemented

1. **`src/rebuild_realistic_datasets.py`** (new) — principled regeneration:
   - **Goals anchored to the real scoreline**: each team's real goals are distributed
     across its players (multinomial by position propensity x minutes). Verified:
     **player goals sum to the real score in 100% of matches** (254,596 = 254,596).
   - **Shots derived from goals** by position conversion rate; `shots >= goals` and
     `shots = on_target + off_target + blocked` hold.
   - **No uniform plateau**: goals now taper naturally (1.72M -> 204k -> 22k -> 2.2k -> 197 -> 17 -> 3).
   - Cards from fouls x position factor (no uniform tail).
   - Cited models: Maher (1982), Dixon-Coles (1997), Decroos et al. (2019),
     Pollard-Reep (1997), Little-Rubin (2002), van Buuren (2018).
2. **`goals_events_cleaned` rebuilt** to its real size (~254,596 rows, all
   `event_type='goal'`), with minute (increasing hazard), goal type (real base rates:
   ~8.5% penalties, ~2.2% own goals) and assists by propensity. Documented as a
   logical exception to the 1.5M target (like the players table).
3. **`src/entity_resolution.py`** (new, NLP) — consistent character normalisation +
   variant merge: `CristianoRonaldo` / `cristiano_ronaldo` / `CR7` / `C. Ronaldo`
   -> one `player_id` (NFKD + camelCase split + nickname map + `rapidfuzz`).
   ~1,236 duplicate ids merged.
4. **`data_provenance` column** on every table (`observed`/`derived`/`simulated`).
5. **Recommender rewritten** (`src/recommendation_engine.py`): uses all 12 retained
   PCs (not just PC1/PC2), standardized distance (instead of cosine-on-2D which gave
   ~1.0 to everyone), and a **position-aware pool** (fixes the "striker -> defender"
   case). Baseline (PC1/PC2 global) vs stronger (PCs + position).
6. **Offline evaluation** (`src/recommendation_evaluation.py`): leakage-safe
   leave-one-out (same player in another season should be retrieved), metrics
   Recall@k/MRR/MAP/NDCG vs baseline + error analysis
   (`reports/recommendation_evaluation_report.md`, `...error_analysis.md`).
7. **Position fix** in `build_pca_feature_matrix.py` (previously 99.6% "Other"
   because GK/DEF/MID/FW were not mapped -> now a realistic split) and PCA now
   exports the retained PCs, not just 2. Clustering made robust at large n.
8. **Documentation**: `data/dictionary.txt` rewritten as a real dictionary with
   provenance + citations; new `reports/methodology_and_citations.md`; updated
   `README.md` and `data/README.md`; `requirements.txt` + `rapidfuzz`.

## Data treatment / outliers in the representation (justified)

The `artifacts/` outputs looked "uneven" because a few extreme values stretched the
ranges (PC1 reached **454** while 99.98% of points sat within +-5, collapsing every
plot). Causes and treatment (each step justified):

1. **Invalid-row removal**: 41 `player_season` rows with no `player_id` (aggregating
   thousands of unmatched records into one "Unknown" bucket with minutes in the
   millions). Not real player-seasons -> dropped at the source and in PCA.
2. **Winsorization (1st/99th pct)**: per-90 rates explode for tiny-minute players
   (e.g. 1 goal in a few minutes -> 225 goals/90 = small-sample noise, not skill).
   Robust quantile capping evens the ranges; PCA is variance-based and sensitive to tails.
3. **log1p** (inside the pipeline): counts/per-90 are heavily right-skewed; the log
   stabilises variance (all features are non-negative).
4. **RobustScaler** (median/IQR) instead of StandardScaler: robust to residual
   outliers (scikit-learn's recommendation for data with outliers).
5. **Robust plot axes** (1st-99th pct limits) as a safety net.

Verified result: PC1 went from `[-0.29, 454.84]` to `[-10.44, 6.49]`;
`minutes_played` from 8.2M to ~2.2k; `goals_per90` from 225 to 2.23. Clustering
stopped being degenerate (was 190913/85/22) and now yields 4 balanced clusters that
are **interpretable by role** (forwards / attacking midfielders / defenders+keepers /
defenders). Documented in `reports/pca_feature_matrix_report.md`.

## Reproducibility

```
python -m src.rebuild_realistic_datasets      # realistic data + identities
python -m src.build_pca_feature_matrix        # PCA (retains N components)
python -m src.build_clustering_analysis       # Week 7 clustering
python -m src.recommendation_evaluation       # Week 10 metrics
```
Fixed seed (`RNG_SEED=20260613`). Verification in `logs/realistic_rebuild_report.json`.

## Honest limitations (for the defense)

- Most of `players`/`player_match` is still **simulated** (no real granular data
  exists for 94k matches); it is labelled as such. The **real** anchors are
  scorelines, ~8k real player profiles, and FBref rates that drive the simulation.
- Offline evaluation: the stronger model beats the baseline on every metric (MRR,
  Hit@k, NDCG, position purity), but absolute same-player retrieval is low because
  the catalog is saturated with ~191k similar simulated profiles — reported honestly.

---

*Part 1 was produced in read-only mode; Part 2 documents the changes made on `joaquincito_branch`.*
