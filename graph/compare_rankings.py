"""compare_rankings.py

Comparar ranking generado por centralidad con baseline (goles/asistencias/minutos).
Produce métricas: Spearman, Kendall, top-k overlap.
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
    parser = argparse.ArgumentParser(description='Comparar rankings graph vs baseline')
    parser.add_argument('--graph-centralities', required=True, help='CSV con centralities (index player_id)')
    parser.add_argument('--baseline', required=True, help='CSV con metricas baseline (index player_id e.g. goals)')
    parser.add_argument('--graph-col', default='pagerank', help='Columna de centrality a comparar')
    parser.add_argument('--baseline-col', default='goals', help='Columna baseline a usar')
    parser.add_argument('--out', help='CSV de salida con resultados (single-row)')
    args = parser.parse_args()

    #dfg = pd.read_csv(args.graph_centralities, index_col=0)
    #dfb = pd.read_csv(args.baseline, index_col=0)
    # 1. Leer los archivos CSV de forma plana sin índices restrictivos
    dfg = pd.read_csv(args.graph_centralities)
    dfb = pd.read_csv(args.baseline)
    
    # 2. Validar que las columnas que solicitas existan en la data
    if args.graph_col not in dfg.columns:
        raise ValueError(f"graph column {args.graph_col} no encontrada en {args.graph_centralities}")
    if args.baseline_col not in dfb.columns:
        raise ValueError(f"baseline column {args.baseline_col} no encontrada en {args.baseline}")
        
    # 3. Alinear por player_id con un merge (NO posicionalmente). El código previo
    #    ordenaba cada CSV por separado y tomaba las columnas por posición de fila;
    #    si los conjuntos de jugadores difieren, eso compara jugadores distintos.
    if 'player_id' not in dfg.columns or 'player_id' not in dfb.columns:
        raise ValueError("ambos archivos deben tener columna 'player_id' para alinear por id")
    merged = dfg[['player_id', args.graph_col]].merge(
        dfb[['player_id', args.baseline_col]], on='player_id', how='inner'
    )
    # Indexar por player_id para que el top-k overlap use ids, no posiciones.
    merged = merged.set_index('player_id')
    graph_scores = merged[args.graph_col]
    baseline_scores = merged[args.baseline_col]

    # 5. Ejecutar la comparación estadística
    res = compare_rankings(graph_scores, baseline_scores)
    res['n_compared'] = int(len(merged))
    
    # 6. Escribir los resultados en el archivo de salida
    if args.out:
        pd.DataFrame([res]).to_csv(args.out, index=False)
        print(f'¡Éxito! Archivo guardado con datos en: {args.out}')
    else:
        print(res)