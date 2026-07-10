#!/usr/bin/env python3
"""
build_graph_final.py
====================
Week 15 Final Player-Similarity Graph Construction

AUDITABLE GRAPH CONSTRUCTION:
  - Loads: data/processed/stats/player_season_stats_cleaned.parquet
  - Features: StandardScaler + cosine similarity
  - Output: nodes, edges, statistics, sensitivity analysis
  - Threshold: 0.75 (default, configurable)

Outputs:
  - graphs/player_similarity_graph_w15.gpickle (NetworkX object)
  - artifacts/graph_nodes_audit.parquet (all nodes with features)
  - artifacts/graph_edges_sample.parquet (edge sample + metadata)
  - artifacts/graph_centrality_rankings_full.csv (all nodes ranked)
  - artifacts/graph_statistics_w15.json (full metrics)
  - artifacts/sensitivity_analysis.csv (thresholds × metrics)
  - logs/graph_construction_audit.json (audit trail)
"""

import argparse
import json
import logging
import pickle
import sys
from pathlib import Path
from typing import Dict, Any, Tuple

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


class GraphBuilder:
    """Manages player-similarity graph construction with full auditability."""
    
    def __init__(self, input_file: str, output_dir: str = 'artifacts', threshold: float = 0.75):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.threshold = threshold
        self.audit_trail = {}
        self.df = None
        self.X_scaled = None
        self.player_index = None
        self.similarity_matrix = None
        self.G = None
        
    def load_and_validate_data(self) -> pd.DataFrame:
        """Load player-season data and validate shape."""
        logger.info(f"Loading data from {self.input_file}...")
        try:
            if self.input_file.endswith('.parquet'):
                df = pd.read_parquet(self.input_file)
            else:
                df = pd.read_csv(self.input_file)
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            sys.exit(1)
        
        logger.info(f"✓ Loaded {len(df)} rows × {len(df.columns)} columns")
        self.audit_trail['input_shape'] = [len(df), len(df.columns)]
        self.audit_trail['columns'] = list(df.columns)
        
        self.df = df
        return df
    
    def build_feature_matrix(self) -> Tuple[np.ndarray, pd.Index]:
        """Build standardized feature matrix for similarity computation."""
        logger.info("Building feature matrix...")
        
        # Features used for similarity (must match graph_report.py logic)
        feature_cols = [
            'position_suitability_score', 'recent_form_index', 'player_age', 'height_cm',
            'goals_per90', 'assists_per90', 'tackles_per90', 'pass_completion_rate'
        ]
        
        # Verify all features exist
        missing = [col for col in feature_cols if col not in self.df.columns]
        if missing:
            logger.error(f"Missing features: {missing}")
            sys.exit(1)
        
        logger.info(f"Using {len(feature_cols)} features: {feature_cols}")
        
        X = self.df[feature_cols].copy()
        
        # Impute with median (per feature_cols strategy)
        medians = X.median().fillna(0.0)
        X = X.fillna(medians).fillna(0.0)
        
        # StandardScaler
        logger.info("Applying StandardScaler...")
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        self.audit_trail['feature_matrix'] = {
            'num_features': len(feature_cols),
            'features': feature_cols,
            'scaling': 'StandardScaler',
            'shape': list(X_scaled.shape),
            'scaler_means': scaler.mean_.tolist()[:3],  # Sample first 3
            'scaler_scales': scaler.scale_.tolist()[:3]
        }
        
        logger.info(f"✓ Feature matrix: {X_scaled.shape}")
        self.X_scaled = X_scaled
        self.player_index = self.df.index
        return X_scaled, self.player_index
    
    def compute_similarity_matrix(self) -> np.ndarray:
        """Compute cosine similarity between all player profiles."""
        logger.info("Computing cosine-similarity matrix...")
        sim_matrix = cosine_similarity(self.X_scaled)
        
        # Statistics on similarities
        np.fill_diagonal(sim_matrix, 0)  # Exclude self-loops
        
        logger.info(f"✓ Similarity matrix: {sim_matrix.shape}")
        logger.info(f"  - Min similarity: {sim_matrix.min():.6f}")
        logger.info(f"  - Mean similarity: {sim_matrix.mean():.6f}")
        logger.info(f"  - Max similarity: {sim_matrix.max():.6f}")
        
        self.audit_trail['similarity_matrix'] = {
            'metric': 'cosine_similarity',
            'shape': list(sim_matrix.shape),
            'min': float(sim_matrix.min()),
            'mean': float(sim_matrix.mean()),
            'max': float(sim_matrix.max()),
            'std': float(sim_matrix.std())
        }
        
        self.similarity_matrix = sim_matrix
        return sim_matrix
    
    def construct_graph(self) -> nx.Graph:
        """Construct graph with threshold-based edge filtering."""
        logger.info(f"Constructing graph with threshold={self.threshold}...")
        G = nx.Graph()
        
        # Add all nodes with attributes
        df_clean = self.df.replace({pd.NA: np.nan})
        
        for idx in self.player_index:
            player = df_clean.iloc[idx]
            G.add_node(
                idx,
                player_id=str(player['player_id']) if pd.notnull(player['player_id']) else 'NA',
                player_name=str(player['player_name']) if pd.notnull(player['player_name']) else 'Unknown',
                position_group=str(player['player_position_group']) if pd.notnull(player['player_position_group']) else 'NA',
                player_age=float(player['player_age']) if pd.notnull(player['player_age']) else 0.0,
                height_cm=float(player['height_cm']) if pd.notnull(player['height_cm']) else 0.0,
                position_suitability_score=float(player['position_suitability_score']) if pd.notnull(player['position_suitability_score']) else 0.0,
                recent_form_index=float(player['recent_form_index']) if pd.notnull(player['recent_form_index']) else 0.0,
                matches_played=int(player['matches_played']) if pd.notnull(player['matches_played']) else 0
            )
        
        # Add edges with threshold filtering
        n = len(self.player_index)
        edges_added = 0
        
        for i in range(n):
            for j in range(i + 1, n):
                sim = self.similarity_matrix[i, j]
                if sim >= self.threshold:
                    G.add_edge(i, j, weight=float(sim), similarity=float(sim))
                    edges_added += 1
        
        logger.info(f"✓ Graph constructed:")
        logger.info(f"  - Nodes: {G.number_of_nodes()}")
        logger.info(f"  - Edges: {G.number_of_edges()}")
        logger.info(f"  - Density: {nx.density(G):.6f}")
        
        self.audit_trail['graph_construction'] = {
            'threshold': self.threshold,
            'num_nodes': G.number_of_nodes(),
            'num_edges': G.number_of_edges(),
            'edges_added': edges_added,
            'potential_edges': n * (n - 1) // 2,
            'density': float(nx.density(G))
        }
        
        self.G = G
        return G
    
    def compute_graph_statistics(self) -> Dict[str, Any]:
        """Compute comprehensive graph-level statistics."""
        logger.info("Computing graph statistics...")
        G = self.G
        
        stats = {
            'basic': {
                'num_nodes': G.number_of_nodes(),
                'num_edges': G.number_of_edges(),
                'density': float(nx.density(G)),
                'is_connected': nx.is_connected(G),
                'num_connected_components': nx.number_connected_components(G),
                'diameter': float(nx.diameter(G)) if nx.is_connected(G) else None
            }
        }
        
        # Degree statistics
        degrees = dict(G.degree())
        weighted_degrees = dict(G.degree(weight='weight'))
        
        stats['degree'] = {
            'min': int(min(degrees.values())) if degrees else 0,
            'max': int(max(degrees.values())) if degrees else 0,
            'mean': float(np.mean(list(degrees.values()))),
            'median': float(np.median(list(degrees.values()))),
            'std': float(np.std(list(degrees.values()))),
            'isolated_nodes': sum(1 for d in degrees.values() if d == 0)
        }
        
        stats['weighted_degree'] = {
            'min': float(min(weighted_degrees.values())) if weighted_degrees else 0.0,
            'max': float(max(weighted_degrees.values())) if weighted_degrees else 0.0,
            'mean': float(np.mean(list(weighted_degrees.values()))),
            'median': float(np.median(list(weighted_degrees.values()))),
            'std': float(np.std(list(weighted_degrees.values())))
        }
        
        # Edge weights
        edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
        stats['edge_weights'] = {
            'min': float(min(edge_weights)) if edge_weights else 0.0,
            'max': float(max(edge_weights)) if edge_weights else 0.0,
            'mean': float(np.mean(edge_weights)),
            'median': float(np.median(edge_weights)),
            'std': float(np.std(edge_weights))
        }
        
        # Centrality (PageRank - fast and scalable)
        logger.info("  Computing PageRank...")
        pagerank = nx.pagerank(G, weight='weight', max_iter=100)
        pagerank_scores = list(pagerank.values())
        stats['pagerank'] = {
            'min': float(min(pagerank_scores)),
            'max': float(max(pagerank_scores)),
            'mean': float(np.mean(pagerank_scores)),
            'median': float(np.median(pagerank_scores))
        }
        
        # Clustering coefficient
        logger.info("  Computing clustering coefficient...")
        clustering_coeffs = list(nx.clustering(G, weight=None).values())
        stats['clustering'] = {
            'mean': float(np.mean(clustering_coeffs)) if clustering_coeffs else 0.0,
            'median': float(np.median(clustering_coeffs)) if clustering_coeffs else 0.0,
            'std': float(np.std(clustering_coeffs)) if clustering_coeffs else 0.0
        }
        
        logger.info(f"✓ Statistics computed")
        self.audit_trail['statistics'] = stats
        return stats
    
    def compute_centrality_rankings(self) -> pd.DataFrame:
        """Compute and rank all nodes by PageRank."""
        logger.info("Computing centrality rankings...")
        G = self.G
        
        pagerank = nx.pagerank(G, weight='weight', max_iter=100)
        
        rankings = []
        for node_id, rank_score in pagerank.items():
            player = self.df.iloc[node_id]
            rankings.append({
                'node_id': node_id,
                'player_id': player['player_id'],
                'player_name': player['player_name'],
                'player_position_group': player['player_position_group'],
                'pagerank_score': rank_score,
                'degree': G.degree(node_id),
                'weighted_degree': sum([G[node_id][neighbor]['weight'] for neighbor in G.neighbors(node_id)]),
                'position_suitability_score': player['position_suitability_score'],
                'goals_per90': player['goals_per90'],
                'assists_per90': player['assists_per90']
            })
        
        rankings_df = pd.DataFrame(rankings).sort_values('pagerank_score', ascending=False).reset_index(drop=True)
        rankings_df['rank'] = rankings_df.index + 1
        
        logger.info(f"✓ Computed rankings for {len(rankings_df)} players")
        return rankings_df
    
    def sensitivity_analysis(self) -> pd.DataFrame:
        """Compute graph metrics across multiple thresholds."""
        logger.info("Running sensitivity analysis...")
        thresholds = [0.60, 0.65, 0.70, 0.75, 0.80]
        results = []
        
        for thresh in thresholds:
            logger.info(f"  - Testing threshold {thresh}...")
            G_test = nx.Graph()
            
            # Add nodes
            for idx in self.player_index:
                G_test.add_node(idx)
            
            # Add edges
            n = len(self.player_index)
            for i in range(n):
                for j in range(i + 1, n):
                    sim = self.similarity_matrix[i, j]
                    if sim >= thresh:
                        G_test.add_edge(i, j, weight=float(sim))
            
            # Compute metrics
            degrees = dict(G_test.degree())
            isolated = sum(1 for d in degrees.values() if d == 0)
            
            results.append({
                'threshold': thresh,
                'num_nodes': G_test.number_of_nodes(),
                'num_edges': G_test.number_of_edges(),
                'density': float(nx.density(G_test)),
                'mean_degree': float(np.mean(list(degrees.values()))) if degrees else 0.0,
                'isolated_nodes': isolated,
                'num_components': nx.number_connected_components(G_test)
            })
        
        sensitivity_df = pd.DataFrame(results)
        logger.info(f"✓ Sensitivity analysis complete")
        return sensitivity_df
    
    def save_artifacts(self):
        """Save all output artifacts."""
        logger.info(f"Saving artifacts to {self.output_dir}...")
        
        # 1. NetworkX graph (pickle)
        graph_file = self.output_dir / 'player_similarity_graph_w15.gpickle'
        with open(graph_file, 'wb') as f:
            pickle.dump(self.G, f)
        logger.info(f"✓ Saved graph pickle: {graph_file}")
        
        # 2. Nodes with all attributes (parquet)
        nodes_data = []
        for node_id in self.G.nodes():
            node_attrs = dict(self.G.nodes[node_id])
            node_attrs['node_id'] = node_id
            nodes_data.append(node_attrs)
        nodes_df = pd.DataFrame(nodes_data)
        nodes_file = self.output_dir / 'graph_nodes_audit.parquet'
        nodes_df.to_parquet(nodes_file, index=False)
        logger.info(f"✓ Saved nodes: {nodes_file} ({len(nodes_df)} nodes)")
        
        # 3. Edges sample (parquet)
        edges_data = []
        for u, v, attrs in self.G.edges(data=True):
            edges_data.append({
                'source_node_id': u,
                'target_node_id': v,
                'source_player_name': self.G.nodes[u]['player_name'],
                'target_player_name': self.G.nodes[v]['player_name'],
                'similarity_weight': attrs['weight']
            })
        edges_df = pd.DataFrame(edges_data)
        edges_file = self.output_dir / 'graph_edges_sample.parquet'
        edges_df.to_parquet(edges_file, index=False)
        logger.info(f"✓ Saved edges sample: {edges_file} ({len(edges_df)} edges)")
        
        # 4. Centrality rankings (full) (CSV)
        rankings_df = self.compute_centrality_rankings()
        rankings_file = self.output_dir / 'graph_centrality_rankings_full.csv'
        rankings_df.to_csv(rankings_file, index=False)
        logger.info(f"✓ Saved rankings: {rankings_file} ({len(rankings_df)} ranked players)")
        
        # 5. Statistics (JSON)
        stats = self.compute_graph_statistics()
        stats_file = self.output_dir / 'graph_statistics_w15.json'
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"✓ Saved statistics: {stats_file}")
        
        # 6. Sensitivity analysis (CSV)
        sensitivity_df = self.sensitivity_analysis()
        sensitivity_file = self.output_dir / 'sensitivity_analysis.csv'
        sensitivity_df.to_csv(sensitivity_file, index=False)
        logger.info(f"✓ Saved sensitivity analysis: {sensitivity_file}")
        
        # 7. Graph definition (JSON) - AUDITABLE SCHEMA
        definition = {
            "node_entity": "Player-Season Profile",
            "node_granularity": "One row per unique player_id",
            "edge_definition": "Cosine similarity between player profiles",
            "edge_directionality": "Undirected",
            "edge_weighted": True,
            "similarity_metric": "cosine_similarity (sklearn.metrics.pairwise)",
            "feature_preprocessing": "StandardScaler",
            "threshold": self.threshold,
            "threshold_rationale": "Hard cutoff at 0.75 to identify strongly similar player profiles",
            "source_data": "data/processed/stats/player_season_stats_cleaned.parquet",
            "features_used": [
                'position_suitability_score', 'recent_form_index', 'player_age', 'height_cm',
                'goals_per90', 'assists_per90', 'tackles_per90', 'pass_completion_rate'
            ]
        }
        definition_file = self.output_dir / 'graph_definition.json'
        with open(definition_file, 'w') as f:
            json.dump(definition, f, indent=2)
        logger.info(f"✓ Saved graph definition: {definition_file}")
        
        # 8. Audit trail (JSON)
        audit_file = Path('logs/graph_construction_audit.json')
        audit_file.parent.mkdir(parents=True, exist_ok=True)
        with open(audit_file, 'w') as f:
            json.dump(self.audit_trail, f, indent=2)
        logger.info(f"✓ Saved audit trail: {audit_file}")
    
    def run(self):
        """Execute full pipeline."""
        logger.info("=" * 80)
        logger.info("WEEK 15: FINAL PLAYER-SIMILARITY GRAPH CONSTRUCTION")
        logger.info("=" * 80)
        
        self.load_and_validate_data()
        self.build_feature_matrix()
        self.compute_similarity_matrix()
        self.construct_graph()
        self.save_artifacts()
        
        logger.info("=" * 80)
        logger.info("✓ GRAPH CONSTRUCTION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Graph: {self.G.number_of_nodes()} nodes, {self.G.number_of_edges()} edges")
        logger.info(f"Artifacts saved to: {self.output_dir}")
        logger.info("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Week 15: Build final auditable player-similarity graph'
    )
    parser.add_argument(
        '--input',
        default='data/processed/stats/player_season_stats_cleaned.parquet',
        help='Input parquet file (default: data/processed/stats/player_season_stats_cleaned.parquet)'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.75,
        help='Similarity threshold for edges (default: 0.75)'
    )
    parser.add_argument(
        '--output-dir',
        default='artifacts',
        help='Output directory for artifacts (default: artifacts/)'
    )
    args = parser.parse_args()
    
    builder = GraphBuilder(args.input, args.output_dir, args.threshold)
    builder.run()


if __name__ == '__main__':
    main()
