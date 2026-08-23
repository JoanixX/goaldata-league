"""build_similarity_graph.py

Construye un Grafo de Similitud de Características (No Dirigido) basado en componentes de PCA.
Optimizado para grandes volúmenes de datos usando procesamiento por lotes para evitar MemoryErrors.
"""
import pandas as pd
import numpy as np
import networkx as nx
import pickle
import argparse
from sklearn.neighbors import NearestNeighbors

def build_feature_similarity_graph(csv_path: str, max_k_neighbors: int = 5, similarity_threshold: float = 0.15) -> nx.Graph:
    """
    Construye el grafo eficientemente indexando el espacio vectorial con Scikit-Learn.
    Nodos: player_id (perfiles de jugador-temporada).
    Aristas: Conexiones con los K vecinos estadísticamente más cercanos.
    """
    print(" Loading cluster file...")
    df = pd.read_csv(csv_path)
    
    # Pick up whichever PCx latent variables are available (the PCA may retain
    # fewer than 12 components on the cleaned catalog).
    pc_cols = sorted([c for c in df.columns if c.startswith('PC') and c[2:].isdigit()],
                     key=lambda c: int(c[2:]))
    if not pc_cols:
        raise ValueError("no PC columns found in input (expected PC1, PC2, ...)")
    print(f" Using {len(pc_cols)} components: {pc_cols}")
    features = df[pc_cols].to_numpy().astype(np.float32) # float32 to reduce memory
    player_ids = df['player_id'].tolist()
    
    G = nx.Graph()
    
    print(" Attaching metadata to nodes...")
    for idx, row in df.iterrows():
        G.add_node(
            row['player_id'], 
            player_name=row['player_name'],
            season=row['season'],
            position_group=row['position_group'],
            kmeans_cluster=row['kmeans_cluster']
        )
    
    print(f" Fitting nearest-neighbor index over {len(player_ids)} records...")
    # Query max_k_neighbors + 1 because a node's nearest neighbor is always itself
    nn = NearestNeighbors(n_neighbors=max_k_neighbors + 1, metric='euclidean', algorithm='auto', n_jobs=-1)
    nn.fit(features)
    
    print(" Querying nearest neighbors in batches...")
    distances, indices = nn.kneighbors(features)
    
    print(" Building edges from the similarity threshold...")
    for i, pid_i in enumerate(player_ids):
        for j_idx, distance in zip(indices[i], distances[i]):
            pid_j = player_ids[j_idx]
            
            if pid_i == pid_j:
                continue # Evitar bucles automáticos
                
            similarity = 1.0 / (1.0 + float(distance))
            
            if similarity >= similarity_threshold:
                G.add_edge(pid_i, pid_j, weight=similarity, distance=float(distance))
                
    return G

def _cli():
    parser = argparse.ArgumentParser(description='Build the PCA-component similarity graph in a memory-bounded way')
    parser.add_argument('--input-csv', required=True, help='Path to player_season_cluster_labels.csv')
    parser.add_argument('--out-graph', required=True, help='Output path for the graph .pickle file')
    parser.add_argument('--k-neighbors', type=int, default=5, help='Nearest neighbors per node')
    parser.add_argument('--threshold', type=float, default=0.15, help='Similarity cutoff threshold')
    args = parser.parse_args()

    G = build_feature_similarity_graph(args.input_csv, max_k_neighbors=args.k_neighbors, similarity_threshold=args.threshold)

    print(f" Guardando grafo binario en {args.out_graph}...")
    with open(args.out_graph, 'wb') as f:
        pickle.dump(G, f)

    print(f" Done. Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")


if __name__ == '__main__':
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))
    from src.logging_utils import run_logged
    run_logged("graph_build_similarity", _cli)