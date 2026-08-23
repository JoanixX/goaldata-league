# Data Methodology, Provenance and Citations

This document explains **how every value in `data/processed/` is produced** and
**why each modelling choice is justified**. It is the reference for the question
"which data is observed and which is modelled, and on what basis?".

> **2026-07 update.** The volume layer is now built by
> `src/build_roster_participation_datasets.py` (real FBref Big-5 rosters 2005-2025
> expanded over real deduplicated fixtures; per-player season totals equal the real
> FBref numbers). Decision-layer ratings are additionally scaled by the **official
> UEFA 5-year country coefficients per season** (`src/ingest_uefa_coefficients.py`
> from uefa.com methodology via the kassiesa.net archive; `src/league_strength.py`),
> so cross-league output is comparable. §3.1 below documents the earlier
> scoreline-allocation stage, retained as remediation history.

## 1. Provenance policy

Every processed table carries a `data_provenance` column. Each value is one of:

| Tag | Meaning |
| --- | --- |
| `observed` | Taken directly from a real source (real final scores, real player profiles, real FBref season totals, real team/competition metadata). |
| `derived` | Deterministic function of observed values (per-90 rates, goal difference, BMI, win/draw flags, indices). |
| `simulated` | Produced by a documented statistical model **anchored on observed quantities**. Never an unconditioned random constant. |

The guiding rule (Little & Rubin, 2002): observed values are never overwritten;
only genuinely missing or unavailable granular values are modelled, and always
from statistically related observed information.

## 2. What is real (the anchors)

- **Match final scores** (`home_score`, `away_score`) — real, from the source
  match data. These anchor everything downstream.
- **Player identities / profiles** for ~8k real-source players (name, position,
  nationality, physical profile where available).
- **FBref 2021-2022 top-5-league season totals** — real per-90 distributions by
  position, used to parameterise the simulation rates.
- **Team and competition metadata.**

## 3. What is modelled, and how

### 3.1 Goals per player-match (scoreline-anchored allocation)
For each match and team we take the team's **real** goals and distribute them
across that team's players with a multinomial whose weights are
`position_scoring_propensity x (minutes/90)`. Consequence: the sum of player
goals equals the real team score **for 100% of matches** (verified:
254,596 allocated = 254,596 real). Team goal counts are never invented.

- Poisson nature of football scoring: **Maher (1982)**, **Dixon & Coles (1997)**.
- Per-90 normalisation and position-conditioned action rates: **Decroos et al. (2019)**.

### 3.2 Shots and shot breakdown
A scorer needs shots, so `shots` are drawn from `goals / conversion_rate(position)`
plus a baseline position shot-volume (Poisson), enforcing `shots >= goals` and the
identity `shots = on_target + off_target + blocked`.

- Shot effectiveness / conversion by play context: **Pollard & Reep (1997)**.

### 3.3 goals_events_cleaned (real-sized goal table, ~254,596 rows)
One row per goal, consistent with the allocation above:
- **minute**: increasing-hazard profile over 1-90 + small extra-time tail (goals
  are empirically more frequent later in halves).
- **goal_type**: real base rates (~8.5% penalties, ~2.2% own goals).
- **assist**: ~74% of non-penalty goals assisted by a same-team team-mate chosen
  by position creation-propensity.

This table is **intentionally below 1.5M rows**. The number of goals is a physical
quantity (~2.69 goals/match), so a 1.5M target would be illogical — exactly like
the players table is allowed to be "small" by domain logic.

### 3.4 Other per-match counts (passes, tackles, cards, distance, ...)
Position-conditioned draws calibrated to realistic per-90 ranges; tagged
`simulated`. Cards are modelled from fouls x position factor (no uniform tail).
These are honest stand-ins where granular real event data is unavailable.

### 3.5 Goalkeeper stats
`goals_conceded` is anchored to the **real** scoreline (goals the keeper's team
conceded); saves and the rest follow a Poisson model around conceded.

### 3.6 Missing-value imputation
Where real fields have gaps, imputation uses statistically related observed data
(grouped medians, per-90 priors), never unconditioned constants
(**Little & Rubin, 2002**; **van Buuren, 2018**).

## 4. Entity resolution (consistent identities and characters)

`src/entity_resolution.py` collapses surface variants of a name to one canonical
`player_id`:
1. **Canonical text**: Unicode NFKD ASCII fold, lower-case, unify separators,
   split camelCase. (`CristianoRonaldo` / `cristiano_ronaldo` / `Cristiano Ronaldo`
   -> `cristiano ronaldo`.)
2. **Nickname map** for non-derivable aliases (`cr7 -> cristiano ronaldo`).
3. **Fuzzy merge** with `rapidfuzz` token-sort similarity + surname-subset rule,
   blocked by surname to stay cheap; applied to real-source players only.

All `player_id` / `assist_player_id` across every table are remapped to the
canonical id (verified: ~1,236 duplicate ids merged on the current data).

## 5. Reproducibility

```
# 1. (real raw sources) -> base processed tables
python -m src.build_processed
# 2. realistic, paper-grounded synthesis + entity resolution (this work)
python -m src.rebuild_realistic_datasets
# 3. representation + segmentation
python -m src.build_pca_feature_matrix
python -m src.build_clustering_analysis
# 4. recommendation + evaluation
python -m src.recommendation_evaluation
```

The rebuild is seeded (`RNG_SEED = 20260613`) so simulated values are reproducible.
A verification report is written to `logs/realistic_rebuild_report.json`
(checks that allocated player-goals == real total goals).

## 6. References

- Maher, M. J. (1982). Modelling association football scores. *Statistica Neerlandica*, 36(3), 109-118.
- Dixon, M. J., & Coles, S. G. (1997). Modelling association football scores and inefficiencies in the football betting market. *JRSS-C*, 46(2), 265-280.
- Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019). Actions Speak Louder than Goals: Valuing Player Actions in Soccer. *KDD 2019*. arXiv:1802.07127.
- Pollard, R., & Reep, C. (1997). Measuring the effectiveness of playing strategies at soccer. *JRSS-D*, 46(4), 541-550.
- Little, R. J. A., & Rubin, D. B. (2002). *Statistical Analysis with Missing Data*. Wiley.
- van Buuren, S. (2018). *Flexible Imputation of Missing Data* (2nd ed.). CRC Press.
