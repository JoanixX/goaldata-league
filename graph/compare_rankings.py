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
        
    # 3. Limpieza estricta: asegurar ordenación idéntica por ID del jugador
    dfg = dfg.sort_values(by=['player_id']).reset_index(drop=True)
    dfb = dfb.sort_values(by=['player_id']).reset_index(drop=True)

    # 4. Extraer las series alineadas posicionalmente
    graph_scores = dfg[args.graph_col]
    baseline_scores = dfb[args.baseline_col]
    
    # 5. Ejecutar la comparación estadística
    res = compare_rankings(graph_scores, baseline_scores)
    
    # 6. Escribir los resultados en el archivo de salida
    if args.out:
        pd.DataFrame([res]).to_csv(args.out, index=False)
        print(f'¡Éxito! Archivo guardado con datos en: {args.out}')
    else:
        print(res)