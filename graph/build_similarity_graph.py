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
    print(" Cargando archivo de clusters...")
    df = pd.read_csv(csv_path)
    
    # Extraer las variables latentes PCx disponibles (robusto: el PCA puede
    # retener menos de 12 componentes sobre el catalogo real limpio).
    pc_cols = sorted([c for c in df.columns if c.startswith('PC') and c[2:].isdigit()],
                     key=lambda c: int(c[2:]))
    if not pc_cols:
        raise ValueError("no PC columns found in input (expected PC1, PC2, ...)")
    print(f" Usando {len(pc_cols)} componentes: {pc_cols}")
    features = df[pc_cols].to_numpy().astype(np.float32) # float32 para reducir memoria
    player_ids = df['player_id'].tolist()
    
    G = nx.Graph()
    
    print(" Inyectando metadatos a los nodos...")
    for idx, row in df.iterrows():
        G.add_node(
            row['player_id'], 
            player_name=row['player_name'],
            season=row['season'],
            position_group=row['position_group'],
            kmeans_cluster=row['kmeans_cluster']
        )
    
    print(f" Entrenando indexador de vecinos cercanos para {len(player_ids)} registros...")
    # Buscamos max_k_neighbors + 1 porque el vecino más cercano a un nodo es siempre sí mismo
    nn = NearestNeighbors(n_neighbors=max_k_neighbors + 1, metric='euclidean', algorithm='auto', n_jobs=-1)
    nn.fit(features)
    
    print(" Buscando vecinos mas cercanos por lotes...")
    distances, indices = nn.kneighbors(features)
    
    print(" Construyendo aristas basadas en el umbral de similitud...")
    for i, pid_i in enumerate(player_ids):
        for j_idx, distance in zip(indices[i], distances[i]):
            pid_j = player_ids[j_idx]
            
            if pid_i == pid_j:
                continue # Evitar bucles automáticos
                
            similarity = 1.0 / (1.0 + float(distance))
            
            if similarity >= similarity_threshold:
                G.add_edge(pid_i, pid_j, weight=similarity, distance=float(distance))
                
    return G

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Construir Grafo de Similitud por Componentes de PCA sin fallos de Memoria')
    parser.add_argument('--input-csv', required=True, help='Ruta al archivo player_season_cluster_labels.csv')
    parser.add_argument('--out-graph', required=True, help='Ruta de salida para el archivo .pickle del grafo')
    parser.add_argument('--k-neighbors', type=int, default=5, help='Vecinos cercanos por nodo')
    parser.add_argument('--threshold', type=float, default=0.15, help='Umbral de corte de similitud')
    args = parser.parse_args()
    
    G = build_feature_similarity_graph(args.input_csv, max_k_neighbors=args.k_neighbors, similarity_threshold=args.threshold)
    
    print(f" Guardando grafo binario en {args.out_graph}...")
    with open(args.out_graph, 'wb') as f:
        pickle.dump(G, f)
        
    print(f" ¡Éxito! Grafo construido con {G.number_of_nodes()} nodos y {G.number_of_edges()} aristas.")