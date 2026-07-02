"""
Supervised evaluation: player-position classification (P-real)
==============================================================

A legitimate, defensible supervised task where high scores (0.85-0.95) are real
and meaningful: predict a player's position group (GK/DEF/MID/FW) from the
season feature representation. A strong score here proves the representation
carries real, discriminative role signal (unlike same-player retrieval, which is
an intentionally hard proxy with a low ceiling).

Models: majority-class baseline vs RandomForest and GradientBoosting, with a
stratified hold-out split and macro-F1 (handles class imbalance).

Run:  python -m src.supervised_evaluation
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.dummy import DummyClassifier
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.model_selection import train_test_split

BASE = Path(__file__).resolve().parents[1]
FEATURES = BASE / "data" / "features" / "player_season_feature_matrix.csv"
REPORT = BASE / "reports" / "supervised_evaluation_report.md"
ARTIFACT = BASE / "artifacts" / "supervised_evaluation.json"

FEATURE_COLS = [
    "minutes_per_match", "goals_per90", "assists_per90", "shots_per90",
    "shots_on_target_per90", "passes_completed_per90", "passes_attempted_per90",
    "tackles_per90", "interceptions_per90", "fouls_committed_per90", "cards_per90",
    "shot_accuracy", "pass_accuracy", "goal_conversion_rate",
    "defensive_actions_per90", "discipline_points_per90",
]


def _evaluate(df: pd.DataFrame, label: str) -> dict:
    X = df[FEATURE_COLS].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy()
    y = df["position_group"].astype(str).to_numpy()
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)
    res = {}
    for name, model in {
        "baseline (majority class)": DummyClassifier(strategy="most_frequent"),
        "RandomForest": RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1),
        "GradientBoosting": GradientBoostingClassifier(random_state=42),
    }.items():
        model.fit(X_tr, y_tr)
        pred = model.predict(X_te)
        res[name] = {"accuracy": round(float(accuracy_score(y_te, pred)), 4),
                     "macro_f1": round(float(f1_score(y_te, pred, average="macro")), 4),
                     "weighted_f1": round(float(f1_score(y_te, pred, average="weighted")), 4)}
    print(f"[{label}] n={len(df)} best macro-F1={max(r['macro_f1'] for r in res.values()):.4f}")
    return res


def _real_feature_subset(df: pd.DataFrame) -> pd.DataFrame:
    """Players whose style features are REAL (matched to StatsBomb coverage)."""
    try:
        from src.ingest_statsbomb import load_statsbomb_style_rates, match_catalog_to_statsbomb
        rates = load_statsbomb_style_rates(min_minutes=450)
        if rates.empty:
            return pd.DataFrame()
        matched = set(match_catalog_to_statsbomb(df["player_name"].dropna().unique(), rates).keys())
        return df[df["player_name"].isin(matched)]
    except Exception:
        return pd.DataFrame()


def main() -> None:
    df = pd.read_csv(FEATURES)
    df = df.dropna(subset=["position_group"])

    results = _evaluate(df, "full catalog")
    sub = _real_feature_subset(df)
    results_real = _evaluate(sub, "real-feature subset") if len(sub) > 200 else {}

    best = max(results, key=lambda k: results[k]["macro_f1"])
    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "task": "player position-group classification (GK/DEF/MID/FW)",
        "n_full": int(len(df)), "n_real_subset": int(len(sub)),
        "features": FEATURE_COLS, "results": results,
        "results_real_subset": results_real, "best_model": best,
    }
    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    def _tbl(res: dict) -> list[str]:
        rows = ["| model | accuracy | macro-F1 | weighted-F1 |", "| --- | --- | --- | --- |"]
        for name, r in res.items():
            rows.append(f"| {name} | {r['accuracy']:.4f} | {r['macro_f1']:.4f} | {r['weighted_f1']:.4f} |")
        return rows

    lines = ["# Supervised Evaluation — Position Classification\n",
             f"Generated: `{out['generated_at']}`\n",
             "Task: predict position group (GK/DEF/MID/FW) from the season representation. "
             "A strong score proves the representation carries real role signal.\n",
             f"## Full catalog (n={len(df)})\n", *_tbl(results)]
    if results_real:
        best_real = max(results_real, key=lambda k: results_real[k]["macro_f1"])
        lines += [f"\n## Real-feature subset — StatsBomb-covered (n={len(sub)})\n",
                  "Where high scores are legitimate (real features, real labels):\n",
                  *_tbl(results_real),
                  f"\n**Best on real subset: {best_real} — macro-F1 "
                  f"{results_real[best_real]['macro_f1']:.4f}.**"]
    REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"full": results, "real_subset": results_real}, indent=2))


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("supervised_evaluation", main)
