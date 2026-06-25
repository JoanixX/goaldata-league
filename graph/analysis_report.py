"""analysis_report.py

Cálculos básicos: componentes, degree/weighted degree, centralidades, PageRank.
Exporta CSVs con resultados que luego se pueden usar para comparar.
"""
import argparse
import networkx as nx
import pandas as pd


def compute_basic_stats(G: nx.Graph) -> dict:
    n = G.number_of_nodes()
    m = G.number_of_edges()
    density = nx.density(G)
    if not G.is_directed():
        comps = list(nx.connected_components(G))
    else:
        comps = list(nx.weakly_connected_components(G))
    comps_sorted = sorted(comps, key=lambda c: len(c), reverse=True)
    largest_sizes = [len(c) for c in comps_sorted[:5]]
    degrees = dict(G.degree(weight=None))
    wdegrees = dict(G.degree(weight='weight'))
    return {
        'n_nodes': n,
        'n_edges': m,
        'density': density,
        'n_components': len(comps_sorted),
        'largest_component_sizes': largest_sizes,
        'degree_mean': sum(degrees.values())/n if n else 0,
        'weighted_degree_mean': sum(wdegrees.values())/n if n else 0
    }


def compute_centralities(G: nx.Graph, approx_betweenness_k: int = None) -> dict:
    out = {}
    out['degree'] = dict(G.degree())
    out['strength'] = dict(G.degree(weight='weight'))
    # PageRank (works for directed and undirected)
    try:
        out['pagerank'] = nx.pagerank(G, weight='weight')
    except Exception:
        out['pagerank'] = {}
    # eigenvector
    try:
        out['eigenvector'] = nx.eigenvector_centrality_numpy(G, weight='weight')
    except Exception:
        out['eigenvector'] = {}
    # betweenness: approximate if k provided
    try:
        if approx_betweenness_k is not None:
            out['betweenness'] = nx.betweenness_centrality(G, k=approx_betweenness_k, weight='weight')
        else:
            out['betweenness'] = nx.betweenness_centrality(G, weight='weight')
    except Exception:
        out['betweenness'] = {}
    return out


def centralities_to_df(centralities: dict) -> pd.DataFrame:
    df = pd.DataFrame(centralities).fillna(0)
    df.index.name = 'player_id'
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analizar grafo y exportar metrics')
    parser.add_argument('--graph', required=True, help='Archivo .gpickle del grafo')
    parser.add_argument('--out-prefix', required=True, help='Prefijo para output CSVs')
    parser.add_argument('--approx-betweenness-k', type=int, default=200, help='k para betweenness approx (opcional)')
    args = parser.parse_args()

    G = nx.read_gpickle(args.graph)
    stats = compute_basic_stats(G)
    central = compute_centralities(G, approx_betweenness_k=args.approx_betweenness_k)

    # guardar stats
    stats_flat = {k: v for k, v in stats.items() if k != 'largest_component_sizes'}
    stats_flat['largest_component_sizes'] = ','.join(map(str, stats['largest_component_sizes']))
    pd.DataFrame([stats_flat]).to_csv(f"{args.out_prefix}_basic_stats.csv", index=False)

    # centralities
    dfc = centralities_to_df(central)
    dfc.to_csv(f"{args.out_prefix}_centralities.csv")
    print(f"Wrote centralities to {args.out_prefix}_centralities.csv")
