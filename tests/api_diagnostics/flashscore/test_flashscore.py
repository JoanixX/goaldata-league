import os
import json
import pandas as pd
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(BASE_DIR, 'src'))

from scrapers.utils import is_null

CSV_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'core', 'matches.csv')
RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'flashscore')

def test_generate_reports():
    df = pd.read_csv(CSV_PATH)
    not_found = []
    missing_data = []

    for idx, row in df.iterrows():
        # Heuristic for data presence
        has_data = not is_null(row.get('possession_home'))
        
        if not has_data:
            not_found.append({
                "match": f"{row['home_team_id']} vs {row['away_team_id']}",
                "date": row['date']
            })
        else:
            m = []
            for f in ['possession_home', 'referee']:
                if is_null(row.get(f)): m.append(f)
            if m:
                missing_data.append({
                    "match": f"{row['home_team_id']} vs {row['away_team_id']}",
                    "missing_fields": m
                })

    with open(os.path.join(RESULTS_DIR, 'not_found_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(not_found, f, indent=2)
    with open(os.path.join(RESULTS_DIR, 'missing_data_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(missing_data, f, indent=2)

if __name__ == "__main__":
    test_generate_reports()
    if not os.path.exists(os.path.join(RESULTS_DIR, 'match_index.json')):
        with open(os.path.join(RESULTS_DIR, 'match_index.json'), 'w') as f: json.dump({}, f)
    print("Flashscore Diagnostic JSONs generated.")
