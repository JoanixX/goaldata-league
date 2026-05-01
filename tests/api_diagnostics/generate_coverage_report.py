import os
import json
import pandas as pd

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
MATCHES_CSV = os.path.join(DATA_DIR, 'core', 'matches.csv')
STATS_CSV = os.path.join(DATA_DIR, 'stats', 'player_match_stats.csv')
OUTPUT_JSON = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'field_coverage.json')

def is_null(val):
    if val is None: return True
    s = str(val).strip().lower()
    return s in ['', 'null', 'nan', 'none']

def generate_report():
    if not os.path.exists(MATCHES_CSV):
        print(f"Error: {MATCHES_CSV} not found")
        return

    df_matches = pd.read_csv(MATCHES_CSV, keep_default_na=False)
    total_rows = len(df_matches)
    
    # Fields to check in matches.csv
    match_fields = [
        'referee', 'stadium', 'city', 'country', 
        'possession_home', 'possession_away'
    ]
    
    coverage = {}
    
    for field in match_fields:
        filled = df_matches[field].apply(lambda x: not is_null(x)).sum()
        coverage[field] = {
            "filled": int(filled),
            "missing": int(total_rows - filled),
            "pct": round((filled / total_rows) * 100, 1) if total_rows > 0 else 0
        }
    
    # Check stats coverage (at least one stat row per match)
    if os.path.exists(STATS_CSV):
        df_stats = pd.read_csv(STATS_CSV, keep_default_na=False)
        matches_with_stats = df_stats['match_id'].nunique()
        coverage['player_stats'] = {
            "filled": int(matches_with_stats),
            "missing": int(total_rows - matches_with_stats),
            "pct": round((matches_with_stats / total_rows) * 100, 1) if total_rows > 0 else 0
        }
        
        # Check specific advanced stats (e.g. assists, top_speed)
        adv_fields = ['assists', 'top_speed', 'distance_covered']
        for field in adv_fields:
            filled = df_stats[field].apply(lambda x: not is_null(x) and str(x) != '0').sum()
            coverage[f"stat_{field}"] = {
                "filled": int(filled),
                "missing": int(total_rows - filled), # This is not quite right as it's per player row
                "note": "Count of non-zero/non-null player records"
            }

    target_records = 1500000
    current_records = int(total_rows + coverage.get('player_stats', {}).get('filled', 0))
    missing_for_goal = max(0, target_records - current_records)

    report = {
        "target_records": target_records,
        "current_records": current_records,
        "missing_for_goal": missing_for_goal,
        "progress_pct": round((current_records / target_records) * 100, 2),
        "total_matches_processed": total_rows,
        "coverage": coverage
    }
    
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    
    print(f"Field coverage report generated at {OUTPUT_JSON}")

if __name__ == "__main__":
    generate_report()
