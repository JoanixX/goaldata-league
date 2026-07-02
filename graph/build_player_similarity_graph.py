#!/usr/bin/env python3
"""
build_player_similarity_graph.py
================================
Constructs a player-similarity graph for Week 13 Graph Analytics Report.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_and_validate_data(input_suitability: str, input_form: str) -> pd.DataFrame:
    logger.info("Loading input data...")
    try:
        suitability = pd.read_parquet(input_suitability)
        form = pd.read_parquet(input_form)
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    
    df = suitability.merge(
        form[['player_id', 'recent_form_index', 'goals', 'assists', 'tackles', 'interceptions']],
        on='player_id',
        how='left'
    )
    logger.info(f"Loaded {len(df)} players")
    return df


def build_feature_matrix(df: pd.DataFrame) -> tuple[np.ndarray, pd.Index]:
    logger.info("Building feature matrix...")
    feature_cols = [
        'position_suitability_score', 'recent_form_index', 'player_age', 'height_cm',
        'goals_per90', 'assists_per90', 'tackles_per90', 'pass_completion_rate'
    ]
    
    X = df[feature_cols].copy()
    medians = X.median().fillna(0.0)
    X = X.fillna(medians).fillna(0.0)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    return X_scaled, df.index


def compute_similarity_matrix(X_scaled: np.ndarray) -> np.ndarray:
    logger.info("Computing cosine-similarity matrix...")
    return cosine_similarity(X_scaled)


def construct_graph(df: pd.DataFrame, similarity_matrix: np.ndarray, player_index: pd.Index, threshold: float = 0.75) -> nx.Graph:
    logger.info(f"Constructing graph with threshold={threshold}...")
    G = nx.Graph()
    df_clean = df.replace({pd.NA: np.nan})
    
    for idx in player_index:
        player = df_clean.iloc[idx]
        G.add_node(
            idx,
            player_id=player['player_id'],
            player_name=player['player_name'],
            position_group=player['player_position_group'],
            player_age=float(player['player_age']) if pd.notnull(player['player_age']) else 0.0,
            position_suitability_score=float(player['position_suitability_score']) if pd.notnull(player['position_suitability_score']) else 0.0,
            recent_form_index=float(player['recent_form_index']) if pd.notnull(player['recent_form_index']) else np.nan,
            height_cm=float(player['height_cm']) if pd.notnull(player['height_cm']) else np.nan,
            matches_played=int(player['matches_played']) if pd.notnull(player['matches_played']) else 0
        )
    
    n = len(player_index)
    for i in range(n):
        for j in range(i+1, n):
            sim = similarity_matrix[i, j]
            if sim >= threshold:
                G.add_edge(i, j, weight=float(sim), similarity=float(sim))
    
    logger.info(f"Graph constructed: Nodes={G.number_of_nodes()}, Edges={G.number_of_edges()}")
    return G


def compute_graph_statistics(G: nx.Graph, df: pd.DataFrame) -> dict:
    logger.info("Computing graph statistics...")
    stats = {
        "basic": {
            "num_nodes": G.number_of_nodes(),
            "num_edges": G.number_of_edges(),
            "density": nx.density(G),
            "is_connected": nx.is_connected(G),
            "num_connected_components": nx.number_connected_components(G)
        }
    }
    
    degrees = dict(G.degree())
    weighted_degrees = dict(G.degree(weight='weight'))
    
    stats["degree"] = {
        "min": min(degrees.values()) if degrees else 0, "max": max(degrees.values()) if degrees else 0,
        "mean": float(np.mean(list(degrees.values()))), "median": float(np.median(list(degrees.values()))),
        "isolated_nodes": sum(1 for d in degrees.values() if d == 0)
    }
    
    stats["weighted_degree"] = {
        "min": float(min(weighted_degrees.values())), "max": float(max(weighted_degrees.values())),
        "mean": float(np.mean(list(weighted_degrees.values()))), "median": float(np.median(list(weighted_degrees.values())))
    }
    
    # Centrality (Optimized)
    logger.info("  Computing betweenness centrality (ultra-fast unweighted sampling k=20)...")
    betweenness = nx.betweenness_centrality(G, k=20, weight=None, seed=42)

    logger.info("  Computing closeness centrality (ultra-fast unweighted BFS sampling k=20)...")
    import random
    random.seed(42)
    nodes_sample = random.sample(list(G.nodes()), min(20, len(G)))
    closeness = {node: 0.0 for node in G.nodes()}
    for source in nodes_sample:
        try:
            path_lengths = nx.single_source_shortest_path_length(G, source)
            for target, dist in path_lengths.items():
                if dist > 0:
                    closeness[target] += (1.0 / dist) / len(nodes_sample)
        except Exception:
            continue
    
    stats["centrality_ranges"] = {
        "betweenness": {"min": float(min(betweenness.values())), "max": float(max(betweenness.values())), "mean": float(np.mean(list(betweenness.values())))},
        "closeness": {"min": float(min(closeness.values())), "max": float(max(closeness.values())), "mean": float(np.mean(list(closeness.values())))}
    }
    
    logger.info("  Computing PageRank...")
    pagerank = nx.pagerank(G, weight='weight')
    pagerank_scores = list(pagerank.values())
    stats["pagerank"] = {"min": float(min(pagerank_scores)), "max": float(max(pagerank_scores)), "mean": float(np.mean(pagerank_scores))}
    
    edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
    stats["edge_weights"] = {
        "min": float(min(edge_weights)) if edge_weights else 0, "max": float(max(edge_weights)) if edge_weights else 0,
        "mean": float(np.mean(edge_weights)), "median": float(np.median(edge_weights))
    }
    
    # SOLUCIÓN DE CLUSTERING VELOZ (weight=None para usar BFS rápido en vez de triángulos ponderados pesados)
    logger.info("  Computing clustering coefficient (fast unweighted clustering)...")
    clustering_coeffs = list(nx.clustering(G, weight=None).values())
    stats["clustering"] = {
        "mean": float(np.mean(clustering_coeffs)) if clustering_coeffs else 0,
        "median": float(np.median(clustering_coeffs)) if clustering_coeffs else 0
    }
    
    return stats


def save_artifacts(G: nx.Graph, df: pd.DataFrame, stats: dict, output_dir: str, threshold: float):
    logger.info(f"Saving artifacts to {output_dir}...")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    nodes_data = [{'node_id': n, **G.nodes[n]} for n in G.nodes()]
    nodes_df = pd.DataFrame(nodes_data)
    nodes_file = output_path / 'graph_nodes.parquet'
    nodes_df.to_parquet(nodes_file, index=False)
    
    edges_data = [{
        'source_node_id': u, 'target_node_id': v, 'weight': attrs['weight'],
        'source_player': G.nodes[u]['player_name'], 'target_player': G.nodes[v]['player_name']
    } for u, v, attrs in G.edges(data=True)]
    
    edges_df = pd.DataFrame(edges_data)
    edges_file = output_path / 'graph_edges.parquet'
    edges_df.to_parquet(edges_file, index=False)
    
    definition = {
        "node_entity": "Player", "edge_definition": "Cosine similarity",
        "similarity_metric": "cosine", "threshold": threshold, "directionality": "undirected", "is_weighted": True
    }
    with open(output_path / 'graph_definition.json', 'w') as f:
        json.dump(definition, f, indent=2)
        
    with open(output_path / 'graph_statistics.json', 'w') as f:
        json.dump(stats, f, indent=2)
        
    import pickle
    with open(output_path / 'graph_networkx.pkl', 'wb') as f:
        pickle.dump(G, f)
        
    return {'nodes': nodes_file, 'edges': edges_file}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-suitability', required=True)
    parser.add_argument('--input-form', required=True)
    parser.add_argument('--threshold', type=float, default=0.75)
    parser.add_argument('--output-dir', default='artifacts')
    args = parser.parse_args()
    
    df = load_and_validate_data(args.input_suitability, args.input_form)
    X_scaled, player_index = build_feature_matrix(df)
    similarity_matrix = compute_similarity_matrix(X_scaled)
    G = construct_graph(df, similarity_matrix, player_index, threshold=args.threshold)
    stats = compute_graph_statistics(G, df)
    save_artifacts(G, df, stats, args.output_dir, args.threshold)
    logger.info("Graph construction complete successfully!")


if __name__ == '__main__':
    main()