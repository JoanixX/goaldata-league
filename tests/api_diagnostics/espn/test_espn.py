import os
import json
import pandas as pd
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(BASE_DIR, 'src'))

from scrapers.utils import is_null

CSV_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'cl_2010_2025.csv')
RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'espn')

def test_generate_reports():
    if not os.path.exists(CSV_PATH): return
    df = pd.read_csv(CSV_PATH, keep_default_na=False)
    
    not_found = []
    missing_data = []

    for idx, row in df.iterrows():
        has_stats = not is_null(row.get('tiros_totales'))
        
        if not has_stats:
            not_found.append({
                "match": f"{row['local']} vs {row['visitante']}",
                "date": row['fecha']
            })
        else:
            m = []
            for f in ['tiros_totales', 'posesion_local', 'faltas_total', 'corners_total']:
                if is_null(row.get(f)): m.append(f)
            if m:
                missing_data.append({
                    "match": f"{row['local']} vs {row['visitante']}",
                    "missing_fields": m
                })

    with open(os.path.join(RESULTS_DIR, 'not_found_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(not_found, f, indent=2)
    with open(os.path.join(RESULTS_DIR, 'missing_data_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(missing_data, f, indent=2)

if __name__ == "__main__":
    test_generate_reports()
    # Ensure index exists (even if empty for now)
    if not os.path.exists(os.path.join(RESULTS_DIR, 'match_index.json')):
        with open(os.path.join(RESULTS_DIR, 'match_index.json'), 'w') as f: json.dump({}, f)
    print("ESPN Diagnostic JSONs generated.")
