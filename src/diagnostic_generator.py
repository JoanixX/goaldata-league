import os
import json
import pandas as pd
import requests
from datetime import datetime

# Add current dir to path for local imports
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.utils import is_null

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_PATH, 'data', 'raw', 'cl_2010_2025.csv')
DIAG_PATH = os.path.join(BASE_PATH, 'tests', 'api_diagnostics')
RESULTS_PATH = os.path.join(DIAG_PATH, 'results')

def load_dataset():
    if not os.path.exists(CSV_PATH): return None
    return pd.read_csv(CSV_PATH, keep_default_na=False)

def generate_uefa_diagnostics(df):
    index_path = os.path.join(DIAG_PATH, 'uefa', 'match_index.json')
    if not os.path.exists(index_path):
        # Try to find it in the old location if move failed
        index_path = os.path.join(RESULTS_PATH, 'uefa_match_ids.json')
    
    if not os.path.exists(index_path):
        print("[!] UEFA index not found.")
        return

    with open(index_path, encoding='utf-8') as f:
        uefa_index = json.load(f)

    found_ids = set(uefa_index.values())
    
    not_found = []
    missing_data = []

    # Simple matching logic to see what's in our CSV vs what's in index
    for idx, row in df.iterrows():
        # Match by date|local|away
        key = f"{row['fecha']}|{row['local']}|{row['visitante']}" # simplified key
        # In reality, we use the complex matching from main.py
        # But for diagnostics, we can check the enrichment status
        
        has_uefa_data = not is_null(row.get('arbitro_principal')) or not is_null(row.get('hora_inicio'))
        
        if not has_uefa_data:
            not_found.append({
                "index": idx,
                "match": f"{row['local']} vs {row['visitante']}",
                "date": row['fecha']
            })
        else:
            # Check for missing fields that UEFA should provide
            missing_fields = []
            for field in ['arbitro_principal', 'arbitros_linea', 'hora_inicio', 'planteles']:
                if is_null(row.get(field)):
                    missing_fields.append(field)
            if missing_fields:
                missing_data.append({
                    "index": idx,
                    "match": f"{row['local']} vs {row['visitante']}",
                    "missing": missing_fields
                })

    with open(os.path.join(RESULTS_PATH, 'uefa', 'not_found_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(not_found, f, indent=2)
    with open(os.path.join(RESULTS_PATH, 'uefa', 'missing_data_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(missing_data, f, indent=2)
    print("[+] UEFA diagnostics generated.")

def generate_espn_diagnostics(df):
    # This would ideally fetch from ESPN API for all seasons
    # For now, we use the results of the already performed enrichment
    not_found = []
    missing_data = []

    for idx, row in df.iterrows():
        has_espn_data = not is_null(row.get('tiros_totales'))
        
        if not has_espn_data:
            not_found.append({
                "index": idx,
                "match": f"{row['local']} vs {row['visitante']}",
                "date": row['fecha']
            })
        else:
            missing_fields = []
            for field in ['tiros_totales', 'posesion_local', 'faltas_total', 'corners_total', 'goles']:
                if is_null(row.get(field)):
                    missing_fields.append(field)
            if missing_fields:
                missing_data.append({
                    "index": idx,
                    "match": f"{row['local']} vs {row['visitante']}",
                    "missing": missing_fields
                })

    with open(os.path.join(RESULTS_PATH, 'espn', 'not_found_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(not_found, f, indent=2)
    with open(os.path.join(RESULTS_PATH, 'espn', 'missing_data_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(missing_data, f, indent=2)
    print("[+] ESPN diagnostics generated.")

if __name__ == "__main__":
    df = load_dataset()
    if df is not None:
        generate_uefa_diagnostics(df)
        generate_espn_diagnostics(df)
        # Placeholders for others
        for scrap in ['fbref', 'flashscore', 'worldfootball']:
            open(os.path.join(RESULTS_PATH, scrap, 'not_found_matches.json'), 'w').write('[]')
            open(os.path.join(RESULTS_PATH, scrap, 'missing_data_matches.json'), 'w').write('[]')
            open(os.path.join(DIAG_PATH, scrap, 'match_index.json'), 'w').write('{}')
