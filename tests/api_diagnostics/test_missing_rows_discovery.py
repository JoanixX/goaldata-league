"""
DIAGNOSTIC 6: ESPN Discovery for Missing Rows
Tries to find ESPN event IDs for the specific rows that still have NULL stats.
Run: python tests/api_diagnostics/test_missing_rows_discovery.py
"""
import sys, os, json, unicodedata, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import requests
import pandas as pd

ESPN_BOARD = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def _norm(s):
    s = s.lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9 ]', ' ', s).strip()


def _date_api(date_csv):
    p = date_csv.split('-')
    return f"{p[2]}{p[1]}{p[0]}"


def find_on_date(date_csv, local, away):
    date_api = _date_api(date_csv)
    try:
        r = requests.get(ESPN_BOARD, params={'dates': date_api}, headers=HEADERS, timeout=10)
        events = r.json().get('events', [])
        # Print all events on this date so we can see what's available
        return events
    except Exception as e:
        return []


BASE = os.path.join(os.path.dirname(__file__), '..', '..')
df = pd.read_csv(os.path.join(BASE, 'data', 'processed', 'champions_league_2011_2025_completed.csv'), keep_default_na=False)
missing = df[df['tiros_totales'] == 'NULL'][['season', 'local', 'visitante', 'fecha']].head(40)

found_cache = {}
not_found = []

print("=== Searching ESPN for missing-stats rows ===\n")

for _, row in missing.iterrows():
    local = str(row['local'])
    away = str(row['visitante'])
    date = str(row['fecha'])
    key = f"{local}|{away}|{date}"

    events = find_on_date(date, local, away)
    if not events:
        not_found.append(key)
        print(f"NO EVENTS ON DATE: {local} vs {away} ({date})")
        continue

    # Print all events on that date
    event_names = [(str(e.get('id')), e.get('name', '?')) for e in events]
    nl = _norm(local)
    na = _norm(away)

    best_id = None
    best_name = None
    for eid, ename in event_names:
        ne = _norm(ename)
        # Try to find any word overlap
        wl = set(nl.split()) - {'fc','sc','ac','rb','as','sl'}
        wa = set(na.split()) - {'fc','sc','ac','rb','as','sl'}
        we = set(ne.split()) - {'fc','sc','ac','rb','as','sl','at','vs'}
        if (wl & we) or (wa & we):
            best_id = eid
            best_name = ename
            break

    if best_id:
        found_cache[key] = best_id
        print(f"✓ FOUND: {local} vs {away} ({date}) -> id={best_id} '{best_name}'")
    else:
        not_found.append(key)
        print(f"✗ NOT FOUND: {local} vs {away} ({date})")
        print(f"    Available: {event_names}")

print(f"\n=== SUMMARY ===")
print(f"Found: {len(found_cache)}, Not found: {len(not_found)}")

os.makedirs('tests/api_diagnostics/results', exist_ok=True)
out = {'found': found_cache, 'not_found': not_found}
with open('tests/api_diagnostics/results/missing_rows_espn_ids.json', 'w', encoding='utf-8') as f:
    json.dump(out, f, ensure_ascii=False, indent=2)
print("Saved to tests/api_diagnostics/results/missing_rows_espn_ids.json")
