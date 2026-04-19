import os
import json
import pandas as pd
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(BASE_DIR, 'src'))

from scrapers.utils import is_null

CSV_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'cl_2010_2025.csv')
RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'fbref')

def test_generate_reports():
    # Placeholder for FBref diagnostics
    not_found = []
    missing_data = []

    # Since FBref is a fallback, we mark matches as 'not found' if not explicitly handled
    with open(os.path.join(RESULTS_DIR, 'not_found_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(not_found, f, indent=2)
    with open(os.path.join(RESULTS_DIR, 'missing_data_matches.json'), 'w', encoding='utf-8') as f:
        json.dump(missing_data, f, indent=2)

if __name__ == "__main__":
    test_generate_reports()
    if not os.path.exists(os.path.join(RESULTS_DIR, 'match_index.json')):
        with open(os.path.join(RESULTS_DIR, 'match_index.json'), 'w') as f: json.dump({}, f)
    print("FBRef Diagnostic JSONs generated.")
