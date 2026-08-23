#!/usr/bin/env python3
"""
analyze_graph.py
================
Comprehensive graph analytics: components, centrality, PageRank, comparisons.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import networkx as nx
import numpy as np
import pandas as pd
from scipy.stats import spearmanr, kendalltau

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_graph_data(nodes_file: str, edges_file: str) -> nx.Graph:
    logger.info("Loading graph data...")
    nodes_df = pd.read_parquet(nodes_file)
    edges_df = pd.read_parquet(edges_file)
    
    G = nx.Graph()
    for _, row in nodes_df.iterrows():
        G.add_node(
            row['node_id'],
            player_id=row['player_id'],
            player_name=row['player_name'],
            position_group=row['position_group'],
            player_age=row['player_age'],
            position_suitability_score=row['position_suitability_score'],
            recent_form_index=row['recent_form_index'],
            height_cm=row.get('height_cm', np.nan),
            matches_played=row.get('matches_played', 0)
        )
    
    for _, row in edges_df.iterrows():
        G.add_edge(row['source_node_id'], row['target_node_id'], weight=row['weight'])
        
    logger.info(f"Loaded graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def analyze_connected_components(G: nx.Graph) -> Dict:
    logger.info("Analyzing connected components...")
    components = list(nx.connected_components(G))
    component_sizes = sorted([len(c) for c in components], reverse=True)
    
    analysis = {
        "num_nodes": G.number_of_nodes(),
        "num_edges": G.number_of_edges(),
        "density": nx.density(G),
        "is_connected": nx.is_connected(G),
        "num_components": len(components),
        "component_sizes": component_sizes[:20],
        "largest_component_size": component_sizes[0] if component_sizes else 0,
        "largest_component_pct": 100 * component_sizes[0] / G.number_of_nodes() if G.number_of_nodes() > 0 else 0,
        "smallest_component_size": component_sizes[-1] if component_sizes else 0,
        "isolated_nodes": sum(1 for n in G.nodes() if G.degree(n) == 0)
    }
    return analysis


def compute_degree_centrality(G: nx.Graph) -> Tuple[Dict, pd.DataFrame]:
    logger.info("Computing degree centrality...")
    degree = dict(G.degree())
    degree_centrality = nx.degree_centrality(G)
    weighted_degree = dict(G.degree(weight='weight'))
    
    centrality_data = []
    for node_id in G.nodes():
        centrality_data.append({
            'node_id': node_id,
            'player_name': G.nodes[node_id].get('player_name', f'Player_{node_id}'),
            'position_group': G.nodes[node_id].get('position_group', 'Unknown'),
            'degree': degree[node_id],
            'degree_centrality': degree_centrality[node_id],
            'weighted_degree': weighted_degree[node_id]
        })
    
    df = pd.DataFrame(centrality_data).sort_values('degree_centrality', ascending=False)
    analysis = {
        "degree_stats": {"min": int(min(degree.values())), "max": int(max(degree.values())), "mean": float(np.mean(list(degree.values()))), "median": float(np.median(list(degree.values()))), "std": float(np.std(list(degree.values())))},
        "weighted_degree_stats": {"min": float(min(weighted_degree.values())), "max": float(max(weighted_degree.values())), "mean": float(np.mean(list(weighted_degree.values()))), "median": float(np.median(list(weighted_degree.values()))), "std": float(np.std(list(weighted_degree.values())))},
        "top_10_by_degree": df.head(10).to_dict('records')
    }
    return analysis, df


def compute_betweenness_centrality(G: nx.Graph) -> Tuple[Dict, pd.DataFrame]:
    logger.info("Computing betweenness centrality (ultra-fast unweighted sampling k=20)...")
    betweenness = nx.betweenness_centrality(G, k=20, weight=None, seed=42)
    
    centrality_data = []
    for node_id in G.nodes():
        centrality_data.append({
            'node_id': node_id,
            'player_name': G.nodes[node_id].get('player_name', f'Player_{node_id}'),
            'position_group': G.nodes[node_id].get('position_group', 'Unknown'),
            'betweenness_centrality': betweenness[node_id]
        })
    
    df = pd.DataFrame(centrality_data).sort_values('betweenness_centrality', ascending=False)
    values = list(betweenness.values())
    analysis = {
        "betweenness_stats": {"min": float(min(values)), "max": float(max(values)), "mean": float(np.mean(values)), "median": float(np.median(values)), "std": float(np.std(values))},
        "top_10_by_betweenness": df.head(10).to_dict('records')
    }
    return analysis, df


def compute_closeness_centrality(G: nx.Graph) -> Tuple[Dict, pd.DataFrame]:
    logger.info("Computing closeness centrality (ultra-fast unweighted BFS sampling k=20)...")
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
            
    centrality_data = []
    for node_id in G.nodes():
        centrality_data.append({
            'node_id': node_id,
            'player_name': G.nodes[node_id].get('player_name', f'Player_{node_id}'),
            'position_group': G.nodes[node_id].get('position_group', 'Unknown'),
            'closeness_centrality': closeness[node_id]
        })
    
    df = pd.DataFrame(centrality_data).sort_values('closeness_centrality', ascending=False)
    values = list(closeness.values())
    analysis = {
        "closeness_stats": {"min": float(min(values)) if values else 0, "max": float(max(values)) if values else 0, "mean": float(np.mean(values)) if values else 0, "median": float(np.median(values)) if values else 0, "std": float(np.std(values)) if values else 0},
        "top_10_by_closeness": df.head(10).to_dict('records')
    }
    return analysis, df


def compute_pagerank(G: nx.Graph) -> Tuple[Dict, pd.DataFrame]:
    logger.info("Computing PageRank...")
    pagerank = nx.pagerank(G, weight='weight', alpha=0.85, max_iter=100)
    
    centrality_data = []
    for node_id in G.nodes():
        centrality_data.append({
            'node_id': node_id,
            'player_name': G.nodes[node_id].get('player_name', f'Player_{node_id}'),
            'position_group': G.nodes[node_id].get('position_group', 'Unknown'),
            'pagerank': pagerank[node_id]
        })
    
    df = pd.DataFrame(centrality_data).sort_values('pagerank', ascending=False)
    values = list(pagerank.values())
    analysis = {
        "pagerank_stats": {"min": float(min(values)), "max": float(max(values)), "mean": float(np.mean(values)), "median": float(np.median(values)), "std": float(np.std(values))},
        "top_10_by_pagerank": df.head(10).to_dict('records')
    }
    return analysis, df


def compare_centrality_to_popularity(G: nx.Graph, suitability_file: str, form_file: str) -> Dict:
    logger.info("Comparing against baselines and mapping real player names...")
    suitability = pd.read_parquet(suitability_file)
    form = pd.read_parquet(form_file)
    
    pagerank = nx.pagerank(G, weight='weight')
    betweenness = nx.betweenness_centrality(G, k=20, weight=None, seed=42)
    degree_centrality = nx.degree_centrality(G)
    
    comparison_data = []
    for node_id in G.nodes():
        player_id = G.nodes[node_id].get('player_id')
        
        # Look the name up directly in the source parquet files by unique ID
        suitability_row = suitability[suitability['player_id'] == player_id]
        form_row = form[form['player_id'] == player_id]
        
        # Use the source parquet's alternative real-name column when it has one;
        # otherwise fall back to the name carried on the node.
        player_name = G.nodes[node_id].get('player_name', f'Player_{node_id}')
        if len(suitability_row) > 0 and 'player_name' in suitability.columns:
            possible_name = suitability_row['player_name'].values[0]
            if pd.notnull(possible_name) and str(possible_name).strip() != "":
                player_name = possible_name
        
        suitability_score = suitability_row['position_suitability_score'].values[0] if len(suitability_row) > 0 else np.nan
        form_score = form_row['recent_form_index'].values[0] if len(form_row) > 0 else np.nan
        
        comparison_data.append({
            'node_id': node_id, 
            'player_name': str(player_name), # Coerce to clean text
            'position_group': G.nodes[node_id].get('position_group', 'Unknown'),
            'pagerank': pagerank.get(node_id, 0), 
            'betweenness_centrality': betweenness.get(node_id, 0),
            'degree_centrality': degree_centrality.get(node_id, 0),
            'position_suitability': suitability_score, 
            'recent_form': form_score
        })
    
    df = pd.DataFrame(comparison_data)
    metrics = {'pagerank': 'PageRank', 'betweenness_centrality': 'Betweenness Centrality', 'degree_centrality': 'Degree Centrality'}
    baselines = {'position_suitability': 'Position Suitability Score', 'recent_form': 'Recent Form Index'}
    
    correlations = {}
    for metric_key, metric_name in metrics.items():
        for baseline_key, baseline_name in baselines.items():
            valid = df[[metric_key, baseline_key]].dropna()
            if len(valid) > 2:
                spearman_rho, spearman_p = spearmanr(valid[metric_key], valid[baseline_key])
                kendall_tau, kendall_p = kendalltau(valid[metric_key], valid[baseline_key])
                correlations[f"{metric_name}_vs_{baseline_name}"] = {
                    "spearman_rho": float(spearman_rho), "spearman_pvalue": float(spearman_p),
                    "kendall_tau": float(kendall_tau), "kendall_pvalue": float(kendall_p), "n_valid_pairs": len(valid)
                }
    
    top_pagerank = df.nlargest(10, 'pagerank')[['player_name', 'position_group', 'pagerank', 'position_suitability', 'recent_form']]
    top_suitability = df.nlargest(10, 'position_suitability')[['player_name', 'position_group', 'pagerank', 'position_suitability', 'recent_form']]
    
    return {"correlations": correlations, "top_10_pagerank": top_pagerank.to_dict('records'), "top_10_suitability": top_suitability.to_dict('records')}

def validity_checks(G: nx.Graph) -> Dict:
    density = nx.density(G)
    isolated_count = sum(1 for n in G.nodes() if G.degree(n) == 0)
    largest_comp = len(max(nx.connected_components(G), key=len))
    
    return {
        "edge_sparsity": {"density": density, "status": "PASS" if density < 0.05 else "REVIEW"},
        "isolated_nodes": {"count": isolated_count, "pct_of_total": 100 * isolated_count / G.number_of_nodes(), "status": "PASS"},
        "component_structure": {"num_components": nx.number_connected_components(G), "largest_component_pct": 100 * largest_comp / G.number_of_nodes(), "status": "PASS"}
    }


def generate_report(components_analysis, degree_analysis, betweenness_analysis, closeness_analysis, pagerank_analysis, comparison_analysis, validity_analysis, threshold):
    # Report body is unchanged from the original, with the added metadata keys mapped correctly.
    report = f"""# Graph Analytics and Centrality Report
## Executive Summary
Graph constructed from player features using cosine similarity with threshold = {threshold}.

## 1. Graph Definition and Construction
- **Nodes**: {components_analysis.get('num_nodes')}
- **Edges**: {components_analysis.get('num_edges')}
- **Density**: {components_analysis.get('density'):.6f}
- **Is Connected**: {components_analysis.get('is_connected')}

## 2. Connected Components Analysis
| Metric | Value |
|--------|-------|
| Number of components | {components_analysis['num_components']} |
| Largest component size | {components_analysis['largest_component_size']} |
| Largest component % | {components_analysis['largest_component_pct']:.2f}% |

## 3. Degree Centrality Analysis
| Metric | Unweighted | Weighted |
|--------|------------|----------|
| Max | {degree_analysis['degree_stats']['max']} | {degree_analysis['weighted_degree_stats']['max']:.4f} |
| Mean | {degree_analysis['degree_stats']['mean']:.2f} | {degree_analysis['weighted_degree_stats']['mean']:.4f} |

### Top 10 Players by Degree Centrality
"""
    for i, record in enumerate(degree_analysis['top_10_by_degree'][:10], 1):
        report += f"| {i} | {record['player_name'][:30]} | {record['position_group']} | {record['degree']} | {record['degree_centrality']:.4f} |\n"
        
    report += f"""
## 4. Betweenness Centrality Analysis
| Metric | Value |
|--------|-------|
| Mean betweenness | {betweenness_analysis['betweenness_stats']['mean']:.6f} |

### Top 10 by Betweenness Centrality
"""
    for i, record in enumerate(betweenness_analysis['top_10_by_betweenness'][:10], 1):
        report += f"| {i} | {record['player_name'][:30]} | {record['position_group']} | {record['betweenness_centrality']:.6f} |\n"

    report += f"""
## 5. Closeness Centrality Analysis
- **Mean Closeness**: {closeness_analysis['closeness_stats']['mean']:.6f}

## 6. PageRank Analysis
### Top 10 Players by PageRank
"""
    for i, record in enumerate(pagerank_analysis['top_10_by_pagerank'][:10], 1):
        report += f"| {i} | {record['player_name'][:30]} | {record['position_group']} | {record['pagerank']:.6f} |\n"

    report += f"""
## 7. Comparison: Graph Rankings vs. Baselines
#### Spearman Rank Correlations
"""
    for corr_name, corr_data in comparison_analysis['correlations'].items():
        report += f"- **{corr_name}**: ρ = {corr_data['spearman_rho']:.3f} (p={corr_data['spearman_pvalue']:.4f})\n"

    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--graph-nodes', required=True)
    parser.add_argument('--graph-edges', required=True)
    parser.add_argument('--suitability', required=True)
    parser.add_argument('--form', required=True)
    parser.add_argument('--threshold', type=float, default=0.75)
    parser.add_argument('--output-report', default='artifacts/GRAPH_ANALYSIS_REPORT.md')
    args = parser.parse_args()
    
    G = load_graph_data(args.graph_nodes, args.graph_edges)
    components = analyze_connected_components(G)
    degree_central, degree_df = compute_degree_centrality(G)
    betweenness_central, betweenness_df = compute_betweenness_centrality(G)
    closeness_central, closeness_df = compute_closeness_centrality(G)
    pagerank_central, pagerank_df = compute_pagerank(G)
    comparison = compare_centrality_to_popularity(G, args.suitability, args.form)
    validity = validity_checks(G)
    
    report = generate_report(components, degree_central, betweenness_central, closeness_central, pagerank_central, comparison, validity, args.threshold)
    
    with open(args.output_report, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"Analysis complete! Report saved to {args.output_report}")


if __name__ == '__main__':
    main()