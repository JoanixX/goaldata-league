# Notebooks

Este directorio se usa para notebooks interactivos de EDA y visualización.

Sugerencias:
- notebook_01_build_and_inspect.ipynb: cargar graphs/*.gpickle, ejecutar analysis_report.py, mostrar distribuciones y redes.
- notebook_02_comparison.ipynb: comparar centralities vs goles/asistencias.

Commands quick-start:
- python graph/build_graph.py --lineups processed/lineups.csv --out graphs/player_graph.gpickle
- python graph/analysis_report.py --graph graphs/player_graph.gpickle --out-prefix reports/player
- python graph/compare_rankings.py --graph-centralities reports/player_centralities.csv --baseline data/baseline_goals.csv --out reports/compare.csv
