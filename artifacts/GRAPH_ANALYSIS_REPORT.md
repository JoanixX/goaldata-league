# Graph Analytics and Centrality Report
## Executive Summary
Graph constructed from player features using cosine similarity with threshold = 0.75.

## 1. Graph Definition and Construction
- **Nodes**: 11269
- **Edges**: 2778027
- **Density**: 0.043756
- **Is Connected**: True

## 2. Connected Components Analysis
| Metric | Value |
|--------|-------|
| Number of components | 1 |
| Largest component size | 11269 |
| Largest component % | 100.00% |

## 3. Degree Centrality Analysis
| Metric | Unweighted | Weighted |
|--------|------------|----------|
| Max | 1606 | 1362.9794 |
| Mean | 493.04 | 409.8965 |

### Top 10 Players by Degree Centrality
| 1 | Dundee 2023-2024 Squad 07 | DEF | 1606 | 0.1425 |
| 2 | West Ham 2024-2025 Squad 08 | DEF | 1603 | 0.1423 |
| 3 | Lyon 2023-2024 Squad 07 | DEF | 1591 | 0.1412 |
| 4 | Ath Bilbao 2024-2025 Squad 07 | DEF | 1588 | 0.1409 |
| 5 | Heidenheim 2024-2025 Squad 05 | DEF | 1587 | 0.1408 |
| 6 | Monaco 2024-2025 Squad 08 | DEF | 1584 | 0.1406 |
| 7 | Athens Kallithea 2024-2025 Squ | DEF | 1579 | 0.1401 |
| 8 | Man. United 2023-2024 Squad 09 | DEF | 1565 | 0.1389 |
| 9 | Twente 2023-2024 Squad 03 | DEF | 1561 | 0.1385 |
| 10 | Paphos FC 2024-2025 Squad 03 | DEF | 1561 | 0.1385 |

## 4. Betweenness Centrality Analysis
| Metric | Value |
|--------|-------|
| Mean betweenness | 0.000180 |

### Top 10 by Betweenness Centrality
| 1 | Maccabi Haifa 2023-2024 Squad  | DEF | 0.011956 |
| 2 | PAOK Saloniki 2024-2025 Squad  | DEF | 0.010333 |
| 3 | Rangers 2023-2024 Squad 09 | DEF | 0.009103 |
| 4 | FK Panevėžys 2024-2025 Squad 2 | FW | 0.007854 |
| 5 | Rangers 2023-2024 Squad 20 | FW | 0.007092 |
| 6 | Brest 2024-2025 Squad 21 | FW | 0.007023 |
| 7 | Vallecano 2024-2025 Squad 05 | DEF | 0.006788 |
| 8 | Augsburg 2023-2024 Squad 09 | DEF | 0.006779 |
| 9 | Víkingur Reykjavík 2024-2025 S | FW | 0.005932 |
| 10 | Borac Banja Luka 2024-2025 Squ | FW | 0.005603 |

## 5. Closeness Centrality Analysis
- **Mean Closeness**: 0.370385

## 6. PageRank Analysis
### Top 10 Players by PageRank
| 1 | Dundee 2023-2024 Squad 07 | DEF | 0.000167 |
| 2 | West Ham 2024-2025 Squad 08 | DEF | 0.000166 |
| 3 | Ath Bilbao 2024-2025 Squad 07 | DEF | 0.000166 |
| 4 | Heidenheim 2024-2025 Squad 05 | DEF | 0.000164 |
| 5 | Lyon 2023-2024 Squad 07 | DEF | 0.000164 |
| 6 | Athens Kallithea 2024-2025 Squ | DEF | 0.000164 |
| 7 | Monaco 2024-2025 Squad 08 | DEF | 0.000164 |
| 8 | Nice 2023-2024 Squad 07 | DEF | 0.000163 |
| 9 | FK RFS 2024-2025 Squad 10 | DEF | 0.000163 |
| 10 | Twente 2023-2024 Squad 03 | DEF | 0.000163 |

## 7. Comparison: Graph Rankings vs. Baselines
#### Spearman Rank Correlations
- **PageRank_vs_Position Suitability Score**: ρ = 0.186 (p=0.0000)
- **Betweenness Centrality_vs_Position Suitability Score**: ρ = -0.192 (p=0.0000)
- **Degree Centrality_vs_Position Suitability Score**: ρ = 0.496 (p=0.0000)
