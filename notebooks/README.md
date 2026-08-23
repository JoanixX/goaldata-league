# Notebooks

This directory holds interactive notebooks for EDA and visualization.

Suggestions:
- notebook_01_build_and_inspect.ipynb: load graphs/*.gpickle, run analysis_report.py, plot distributions and networks.
- notebook_02_comparison.ipynb: compare centralities against goals/assists.

Commands quick-start:
- python graph/build_graph.py --lineups processed/lineups.csv --out graphs/player_graph.gpickle
- python graph/analysis_report.py --graph graphs/player_graph.gpickle --out-prefix reports/player
- python graph/compare_rankings.py --graph-centralities reports/player_centralities.csv --baseline data/baseline_goals.csv --out reports/compare.csv
