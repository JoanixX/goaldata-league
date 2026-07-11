"""
Serialize Demo Data for Interactive Dashboard
==============================================

Reads artifact CSVs and JSON files produced by the GoalData League pipeline and
emits a self-contained ``reports/demo/data.js`` file that the dashboard
``index.html`` can import directly without a backend server.

Outputs a single JS variable assignment:
    window.DEMO_DATA = { ... }

Run:
    python -m src.serialize_demo_data
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
DEMO_DIR = BASE_DIR / "reports" / "demo"
DATA_JS = DEMO_DIR / "data.js"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_read_csv(path: Path, nrows: int | None = None) -> pd.DataFrame:
    if not path.exists():
        print(f"  [SKIP] {path.name} not found")
        return pd.DataFrame()
    return pd.read_csv(path, nrows=nrows)


def safe_read_json(path: Path) -> dict:
    if not path.exists():
        print(f"  [SKIP] {path.name} not found")
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def df_to_records(df: pd.DataFrame, round_floats: int = 4) -> list[dict]:
    """Convert DataFrame to JSON-serializable list of dicts, rounding floats."""
    if df.empty:
        return []
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if isinstance(v, float):
                if v != v:  # NaN
                    clean[k] = None
                else:
                    clean[k] = round(v, round_floats)
            else:
                clean[k] = v
        records.append(clean)
    return records


# ---------------------------------------------------------------------------
# Load & slim datasets
# ---------------------------------------------------------------------------

def load_player_clusters(n: int = 500) -> list[dict]:
    """PCA 2D coordinates + cluster labels — sampled for dashboard scatter plot."""
    df = safe_read_csv(ARTIFACTS_DIR / "player_season_cluster_labels.csv")
    if df.empty:
        return []
    # Keep real players only (no Squad fillers), current season preference
    real = df[~df["player_name"].str.contains("Squad", case=False, na=False)
              & (df["player_name"].str.casefold() != "unknown")]
    # One row per player (most recent season)
    def season_start(s: object) -> int:
        s = str(s)
        return int(s[:4]) if s[:4].isdigit() else -1

    latest = real.copy()
    latest["_yr"] = latest["season"].map(season_start)
    latest = (latest.sort_values("_yr", ascending=False)
              .drop_duplicates("player_name", keep="first")
              .drop(columns=["_yr"]))

    sample = latest.head(n)
    cols = ["player_name", "season", "position_group", "PC1", "PC2", "kmeans_cluster", "dbscan_cluster"]
    cols = [c for c in cols if c in sample.columns]
    return df_to_records(sample[cols])


def load_graph_centralities(n: int = 200) -> list[dict]:
    df = safe_read_csv(ARTIFACTS_DIR / "graph_centralities.csv", nrows=n)
    if df.empty:
        return []
    cols = ["player_name", "position_group", "degree", "weighted_degree", "pagerank",
            "betweenness", "closeness", "eigenvector"]
    cols = [c for c in cols if c in df.columns]
    return df_to_records(df[cols])


def load_optimal_xi() -> list[dict]:
    df = safe_read_csv(ARTIFACTS_DIR / "optimal_xi_2021-2022_4-3-3.csv")
    if df.empty:
        return []
    return df_to_records(df)


def load_passing_network(team: str) -> list[dict]:
    path = ARTIFACTS_DIR / f"passing_network_{team}.csv"
    df = safe_read_csv(path)
    if df.empty:
        return []
    return df_to_records(df)


def load_cluster_profiles() -> list[dict]:
    df = safe_read_csv(ARTIFACTS_DIR / "clustering_kmeans_profiles.csv")
    if df.empty:
        return []
    return df_to_records(df)


def load_similar_players_index(n_players: int = 80) -> list[dict]:
    """Pre-compute top-5 similar players for a sample set using the recommender."""
    df_full = safe_read_csv(ARTIFACTS_DIR / "player_season_cluster_labels.csv")
    if df_full.empty:
        return []

    import re
    _PC_RE = re.compile(r"^PC\d+$")
    all_pcs = sorted([c for c in df_full.columns if _PC_RE.match(c)], key=lambda c: int(c[2:]))
    if not all_pcs:
        return []

    import numpy as np
    real = df_full[~df_full["player_name"].str.contains("Squad", case=False, na=False)
                   & (df_full["player_name"].str.casefold() != "unknown")].copy().reset_index(drop=True)

    def sy(s):
        s = str(s)
        return int(s[:4]) if s[:4].isdigit() else -1

    real["_yr"] = real["season"].map(sy)
    query_pool = (real.sort_values("_yr", ascending=False)
                  .drop_duplicates("player_name", keep="first")
                  .head(n_players)
                  .reset_index(drop=True))

    xs = real[all_pcs].to_numpy(dtype="float64")
    mu, sd = xs.mean(axis=0), xs.std(axis=0)
    sd[sd == 0] = 1.0
    xs_std = (xs - mu) / sd

    results = []
    for _, qrow in query_pool.iterrows():
        q_idx = real.index[real["player_name"] == qrow["player_name"]]
        if len(q_idx) == 0:
            continue
        q_idx = q_idx[0]
        q_vec = xs_std[q_idx]
        dists = np.sqrt(((xs_std - q_vec) ** 2).sum(axis=1))
        pool_mask = (real["player_name"] != qrow["player_name"]) & (real["position_group"] == qrow["position_group"])
        candidate_idx = real.index[pool_mask].tolist()
        if not candidate_idx:
            continue
        cand_dists = [(real.iloc[i]["player_name"], real.iloc[i]["position_group"], round(float(dists[i]), 4))
                      for i in candidate_idx]
        top5 = sorted(cand_dists, key=lambda x: x[2])[:5]
        results.append({
            "player": qrow["player_name"],
            "position": qrow["position_group"],
            "season": str(qrow["season"]),
            "similar": [{"name": t[0], "position": t[1], "distance": t[2]} for t in top5]
        })
    return results


def load_pca_variance() -> list[dict]:
    df = safe_read_csv(ARTIFACTS_DIR / "pca_explained_variance.csv")
    if df.empty:
        return []
    return df_to_records(df)


def load_kmeans_sweep() -> list[dict]:
    df = safe_read_csv(ARTIFACTS_DIR / "clustering_kmeans_sweep.csv")
    if df.empty:
        return []
    return df_to_records(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    print("Serializing demo data...")

    graph_stats = safe_read_json(ARTIFACTS_DIR / "graph_statistics.json")
    pca_report = safe_read_json(ARTIFACTS_DIR / "pca_feature_matrix_report.json")
    clustering_report = safe_read_json(ARTIFACTS_DIR / "clustering_validation_report.json")
    rec_eval = safe_read_json(ARTIFACTS_DIR / "recommendation_evaluation.json")

    print("  Loading player clusters (PCA 2D)...")
    clusters = load_player_clusters(n=400)
    print(f"  -> {len(clusters)} player-seasons")

    print("  Loading graph centralities...")
    centralities = load_graph_centralities(n=200)
    print(f"  -> {len(centralities)} records")

    print("  Loading optimal XI...")
    optimal_xi = load_optimal_xi()
    print(f"  -> {len(optimal_xi)} players")

    print("  Loading passing networks...")
    passing_liverpool = load_passing_network("Liverpool_22912")
    passing_tottenham = load_passing_network("Tottenham_Hotspur_22912")

    print("  Loading cluster profiles...")
    cluster_profiles = load_cluster_profiles()

    print("  Computing similar players index (this may take a moment)...")
    similarity_index = load_similar_players_index(n_players=80)
    print(f"  -> {len(similarity_index)} players indexed")

    print("  Loading PCA variance & K-Means sweep...")
    pca_variance = load_pca_variance()
    kmeans_sweep = load_kmeans_sweep()

    payload = {
        "meta": {
            "project": "GoalData League — Football Data Pipeline & Scouting System",
            "generated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "n_players_indexed": len(clusters),
            "n_centralities": len(centralities),
            "pca_components_90pct": pca_report.get("optimal_components_90", 11),
            "pca_variance_2d": round(pca_report.get("first_two_components_variance", 0.4551), 4),
            "kmeans_k": clustering_report.get("selected_kmeans_k", 3),
            "graph_nodes": graph_stats.get("num_nodes", 2531),
            "graph_edges": graph_stats.get("num_edges", 17777),
        },
        "pca_variance": pca_variance,
        "kmeans_sweep": kmeans_sweep,
        "clusters": clusters,
        "cluster_profiles": cluster_profiles,
        "centralities": centralities,
        "optimal_xi": optimal_xi,
        "passing_liverpool": passing_liverpool,
        "passing_tottenham": passing_tottenham,
        "similarity_index": similarity_index,
        "recommendation_eval": rec_eval.get("results", []),
        "graph_statistics": graph_stats,
    }

    js_content = f"// Auto-generated by src/serialize_demo_data.py — DO NOT EDIT MANUALLY\nwindow.DEMO_DATA = {json.dumps(payload, indent=2, ensure_ascii=False)};\n"
    DATA_JS.write_text(js_content, encoding="utf-8")
    print(f"\nWritten: {DATA_JS} ({len(js_content):,} bytes)")
    print("Done! Open reports/demo/index.html in a browser.")


if __name__ == "__main__":
    main()
