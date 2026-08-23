"""compare_rankings.py

Compare a centrality-based ranking against a baseline (goals/assists/minutes).
Produces Spearman, Kendall and top-k overlap metrics.
"""
import argparse
import pandas as pd
from scipy.stats import spearmanr, kendalltau


def compare_rankings(graph_scores: pd.Series, baseline_scores: pd.Series, ks=[10,20,50]):
    df = pd.concat([graph_scores, baseline_scores], axis=1, keys=['graph','baseline']).dropna()
    if df.empty:
        return {}
    sp = spearmanr(df['graph'], df['baseline'])
    kd = kendalltau(df['graph'], df['baseline'])
    results = {"spearman_r": float(sp.correlation) if sp else None, "spearman_p": float(sp.pvalue) if sp else None,
               "kendall_tau": float(kd.correlation) if kd else None, "kendall_p": float(kd.pvalue) if kd else None}
    for k in ks:
        topg = set(df['graph'].nlargest(k).index)
        topb = set(df['baseline'].nlargest(k).index)
        results[f'top{k}_overlap'] = len(topg & topb)/k
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare graph vs baseline rankings')
    parser.add_argument('--graph-centralities', required=True, help='CSV of centralities (indexed by player_id)')
    parser.add_argument('--baseline', required=True, help='CSV of baseline metrics (indexed by player_id, e.g. goals)')
    parser.add_argument('--graph-col', default='pagerank', help='Centrality column to compare')
    parser.add_argument('--baseline-col', default='goals', help='Baseline column to use')
    parser.add_argument('--out', help='Output CSV with the results (single row)')
    args = parser.parse_args()

    #dfg = pd.read_csv(args.graph_centralities, index_col=0)
    #dfb = pd.read_csv(args.baseline, index_col=0)
    # 1. Read the CSV files flat, without restrictive indexes
    dfg = pd.read_csv(args.graph_centralities)
    dfb = pd.read_csv(args.baseline)
    
    # 2. Validate that the requested columns exist in the data
    if args.graph_col not in dfg.columns:
        raise ValueError(f"graph column {args.graph_col} not found in {args.graph_centralities}")
    if args.baseline_col not in dfb.columns:
        raise ValueError(f"baseline column {args.baseline_col} not found in {args.baseline}")
        
    # 3. Align on player_id with a merge (NOT positionally). The previous code
    #    sorted each CSV separately and took the columns by row position; if the
    #    two player sets differ, that compares different players.
    if 'player_id' not in dfg.columns or 'player_id' not in dfb.columns:
        raise ValueError("both files must have a 'player_id' column to align by id")
    merged = dfg[['player_id', args.graph_col]].merge(
        dfb[['player_id', args.baseline_col]], on='player_id', how='inner'
    )
    # Index by player_id so the top-k overlap uses ids, not positions.
    merged = merged.set_index('player_id')
    graph_scores = merged[args.graph_col]
    baseline_scores = merged[args.baseline_col]

    # 5. Run the statistical comparison
    res = compare_rankings(graph_scores, baseline_scores)
    res['n_compared'] = int(len(merged))
    
    # 6. Write the results to the output file
    if args.out:
        pd.DataFrame([res]).to_csv(args.out, index=False)
        print(f'Done. Results written to: {args.out}')
    else:
        print(res)