"""
Offline evaluation for the player-similarity recommender (Week 10)
==================================================================

Problem: this is an item-item similarity recommender, and we have no click/rating
log to evaluate against. We therefore use a **leakage-safe, label-free proxy** that
is standard for similarity systems:

    "same entity across time should retrieve itself"

A player's profile in season *t* should rank that same canonical player's *other*
season profiles highly. This gives ground-truth relevance without any hand
labelling and without using identity as a model feature.

Protocol (leave-one-out)
------------------------
* Query set = every player-season whose canonical ``player_id`` appears in >= 2
  seasons.
* For a query row q, relevant set = rows with the same ``player_id`` and a
  different season (q itself excluded).
* Candidate universe = all other player-seasons (q excluded). Identical for both
  systems, so the metric isolates the *representation*.
* Leakage controls: q is removed from the pool; the model scores on PCA features
  only; ``player_id`` is used solely to build the relevance labels, never as a
  feature.

Systems compared
----------------
* baseline : Euclidean distance on PC1, PC2 (the old behaviour).
* stronger : standardized Euclidean over all retained PCs.

Metrics: Recall@k, HitRate@k, MRR, MAP, NDCG@k (k = 5, 10), plus a
position-purity@k sanity metric.

Run: python -m src.recommendation_evaluation
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from recommendation_engine import load_data, pc_columns, _standardized_matrix

BASE_DIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
REPORTS_DIR = BASE_DIR / "reports"
JSON_PATH = ARTIFACTS_DIR / "recommendation_evaluation.json"
REPORT_PATH = REPORTS_DIR / "recommendation_evaluation_report.md"
ERROR_PATH = REPORTS_DIR / "recommendation_error_analysis.md"

KS = (5, 10)


def _dcg(rels: np.ndarray) -> float:
    return float(np.sum(rels / np.log2(np.arange(2, len(rels) + 2))))


def evaluate(df: pd.DataFrame, repr_name: str, features: np.ndarray, max_k: int = 10) -> tuple[dict, list]:
    player_ids = df["player_id"].to_numpy()
    seasons = df["season"].astype(str).to_numpy()
    positions = df["position_group"].to_numpy()
    names = df["player_name"].to_numpy()

    # players appearing in >=2 seasons are eligible queries
    by_pid = pd.Series(seasons, index=player_ids).groupby(level=0).nunique()
    multi = set(by_pid[by_pid >= 2].index)
    query_rows = [i for i in range(len(df)) if player_ids[i] in multi]

    agg = {f"recall@{k}": [] for k in KS}
    agg.update({f"hit@{k}": [] for k in KS})
    agg.update({f"ndcg@{k}": [] for k in KS})
    agg.update({f"pos_purity@{k}": [] for k in KS})
    rr, ap = [], []
    examples = []

    for i in query_rows:
        rel_mask = (player_ids == player_ids[i]) & (seasons != seasons[i])
        rel_mask[i] = False
        n_rel = int(rel_mask.sum())
        if n_rel == 0:
            continue
        d = np.sqrt(((features - features[i]) ** 2).sum(axis=1))
        d[i] = np.inf  # exclude the query itself
        order = np.argsort(d)
        ranked_rel = rel_mask[order]
        # first relevant rank for MRR
        first = np.argmax(ranked_rel) if ranked_rel.any() else -1
        rr.append(1.0 / (first + 1) if first >= 0 and ranked_rel[first] else 0.0)
        # average precision
        hits = np.cumsum(ranked_rel)
        precis = hits / (np.arange(len(ranked_rel)) + 1)
        ap.append(float((precis * ranked_rel).sum() / n_rel))
        for k in KS:
            topk = ranked_rel[:k]
            agg[f"recall@{k}"].append(float(topk.sum() / n_rel))
            agg[f"hit@{k}"].append(float(topk.any()))
            ideal = np.sort(rel_mask.astype(float))[::-1][:k]
            agg[f"ndcg@{k}"].append(_dcg(topk.astype(float)) / (_dcg(ideal) or 1.0))
            agg[f"pos_purity@{k}"].append(float((positions[order[:k]] == positions[i]).mean()))
        if len(examples) < 40:
            examples.append({
                "query_player": str(names[i]), "query_season": str(seasons[i]),
                "query_position": str(positions[i]),
                "top5": [f"{names[order[j]]} ({seasons[order[j]]}, {positions[order[j]]})" for j in range(5)],
                "first_relevant_rank": int(first + 1) if first >= 0 else None,
            })

    metrics = {"representation": repr_name, "n_queries": len(rr)}
    metrics["MRR"] = round(float(np.mean(rr)), 4) if rr else 0.0
    metrics["MAP"] = round(float(np.mean(ap)), 4) if ap else 0.0
    for key, vals in agg.items():
        metrics[key] = round(float(np.mean(vals)), 4) if vals else 0.0
    return metrics, examples


def main() -> None:
    df = load_data().reset_index(drop=True)
    all_pcs = pc_columns(df)
    if not all_pcs:
        raise ValueError("No PC columns in cluster labels; run the PCA step first.")

    baseline_cols = [c for c in ["PC1", "PC2"] if c in all_pcs] or all_pcs[:2]
    baseline_feat = df[baseline_cols].to_numpy(dtype="float64")
    stronger_feat = _standardized_matrix(df, all_pcs)

    base_metrics, _ = evaluate(df, f"baseline (PC1-PC2, {len(baseline_cols)} dims)", baseline_feat)
    strong_metrics, strong_examples = evaluate(df, f"stronger (standardized {len(all_pcs)} PCs)", stronger_feat)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "protocol": "leave-one-out same-player cross-season retrieval (leakage-safe)",
        "player_seasons": int(len(df)),
        "retained_pcs": len(all_pcs),
        "results": [base_metrics, strong_metrics],
    }
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    JSON_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    def row(m):
        return ("| " + " | ".join([m["representation"], str(m["n_queries"]), str(m["MRR"]), str(m["MAP"]),
                str(m["recall@5"]), str(m["recall@10"]), str(m["hit@5"]), str(m["ndcg@10"]),
                str(m["pos_purity@5"])]) + " |")

    md = f"""# Week 10 - Offline Evaluation Report (Recommendation / Ranking)

Generated: `{report['generated_at']}`

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
Leave-one-out over the {report['player_seasons']:,} player-seasons; query set =
players present in >= 2 seasons. The query row is removed from the pool, and
`player_id` is used only to build relevance labels, never as a model feature.

## Results

| representation | n_queries | MRR | MAP | Recall@5 | Recall@10 | Hit@5 | NDCG@10 | PosPurity@5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
{row(base_metrics)}
{row(strong_metrics)}

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
"""
    REPORT_PATH.write_text(md, encoding="utf-8")

    # ---- error analysis -------------------------------------------------
    good = [e for e in strong_examples if e["first_relevant_rank"] and e["first_relevant_rank"] <= 5][:8]
    bad = [e for e in strong_examples if not e["first_relevant_rank"] or e["first_relevant_rank"] > 20][:8]

    def block(items, title):
        lines = [f"### {title}", ""]
        for e in items:
            lines.append(f"- **{e['query_player']}** ({e['query_season']}, {e['query_position']}) "
                         f"-> first same-player hit at rank {e['first_relevant_rank']}")
            lines.append(f"  - top-5: {', '.join(e['top5'])}")
        if not items:
            lines.append("- (none in the sampled queries)")
        return "\n".join(lines)

    err = f"""# Week 10 - Recommendation Error Analysis

Generated: `{report['generated_at']}`
System analysed: stronger (standardized {len(all_pcs)}-PC similarity).

{block(good, "Strong cases (relevant comparable retrieved in top-5)")}

{block(bad, "Failure cases (relevant comparable ranked poorly / not found)")}

## Interpretation
- Strong cases are typically players with a distinctive statistical profile, so
  their other-season self is an unambiguous nearest neighbour.
- Failure cases are usually players whose role/output changed a lot between
  seasons, or sit in a dense region where many profiles are near-identical, so
  the correct cross-season match is crowded out. This is the expected limit of a
  static profile-similarity model and motivates position-aware pooling and, in
  future work, time-aware or learned representations.
"""
    ERROR_PATH.write_text(err, encoding="utf-8")
    print(json.dumps({"results": report["results"], "report": str(REPORT_PATH)}, indent=2))


if __name__ == "__main__":
    main()
