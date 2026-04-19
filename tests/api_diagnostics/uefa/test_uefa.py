import os
import json
import pandas as pd
import sys

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(BASE_DIR, 'src'))

from scrapers.utils import is_null

CSV_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'cl_2010_2025.csv')
RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'uefa')

def test_generate_match_index():
    # In a real scenario, this would crawl the UEFA API.
    # For now, we ensure the index exists in results.
    path = os.path.join(RESULTS_DIR, 'match_index.json')
    assert os.path.exists(path), "match_index.json should be present in results/uefa"

def test_generate_sample_data():
    # Ensure sample_data.json exists
    path = os.path.join(RESULTS_DIR, 'sample_data.json')
    assert os.path.exists(path)

def test_generate_reports():
    if not os.path.exists(CSV_PATH): return
    df = pd.read_csv(CSV_PATH, keep_default_na=False)
    
    not_found = []
    missing_data = []

    for idx, row in df.iterrows():
        # Heuristic for UEFA data presence
        has_uefa = not is_null(row.get('arbitro_principal')) or not is_null(row.get('hora_inicio'))
        
        if not has_uefa:
            not_found.append({
                "match": f"{row['local']} vs {row['visitante']}",
                "date": row['fecha']
            })
        else:
            m = []
            for f in ['arbitro_principal', 'arbitros_linea', 'hora_inicio', 'planteles']:
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
    print("UEFA Diagnostic JSONs generated.")
