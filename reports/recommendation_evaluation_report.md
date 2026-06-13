# Week 10 - Offline Evaluation Report (Recommendation / Ranking)

Generated: `2026-06-13T10:41:43`

## Task framing
Content-based **item-item similarity / ranking** for scouting: given a
player-season, rank the most similar player-seasons (comparables / replacements).
Unit ranked = `(player, season)`. No interaction log exists, so this is not
collaborative filtering.

## Candidate pool
- **Productive system pool:** same `position_group` as the query (and optionally
  the same K-Means cluster), query player excluded.
- **Evaluation pool:** all other player-seasons (identical for both systems) so
  the metric measures the *representation*, not the pool.

## Evaluation protocol (leakage-safe, label-free)
Ground truth = "same canonical player in another season should be retrieved".
Leave-one-out over the 190,979 player-seasons; query set =
players present in >= 2 seasons. The query row is removed from the pool, and
`player_id` is used only to build relevance labels, never as a model feature.

## Results

| representation | n_queries | MRR | MAP | Recall@5 | Recall@10 | Hit@5 | NDCG@10 | PosPurity@5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline (PC1-PC2, 2 dims) | 4374 | 0.0008 | 0.0002 | 0.0 | 0.0001 | 0.0005 | 0.0001 | 0.5147 |
| stronger (standardized 12 PCs) | 4374 | 0.0018 | 0.0004 | 0.0004 | 0.0007 | 0.0014 | 0.0006 | 0.9068 |

`PosPurity@5` is a sanity check: share of the top-5 sharing the query position.

## How to read this
- If the stronger (full standardized PC) representation beats the baseline on
  MRR / Recall / NDCG, the extra components carry real same-player signal that
  the 2D map discards. If it does **not** beat the baseline, that must be stated
  honestly in the defense (a complex model that does not beat a simple baseline
  is a finding, not a failure to hide).
- Limitation: with few seasons in the current data, the query set is small; the
  metric is a representation probe, not a production accuracy guarantee.

See `recommendation_error_analysis.md` for concrete good/bad cases.
