"""
DIAGNOSTIC 5: ESPN Discovery (scoreboard date-based search)
Tests that ESPN scoreboard finds the correct event by date for known matches.
Run: python tests/api_diagnostics/test_espn_discovery.py
"""
import sys, os, json, unicodedata, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import requests

ESPN_BOARD = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# (date DD-MM-YYYY, local_csv, visitante_csv, expected_event_id)
TEST_CASES = [
    ('15-02-2011', 'Valencia',          'Schalke 04',          '310989'),
    ('25-05-2013', 'Bayern Munich',     'Borussia Dortmund',   '365094'),
    ('01-06-2024', 'Real Madrid',       'Borussia Dortmund',   '702410'),
    ('10-06-2023', 'Manchester City',   'Inter',               '941581'),
    ('28-05-2022', 'Liverpool',         'Real Madrid',         '813492'),
    ('29-05-2021', 'Manchester City',   'Chelsea',             '665879'),
    ('23-08-2020', 'Bayern Munich',     'PSG',                 '600034'),
    ('31-05-2025', 'PSG',              'Inter Milan',         None),  # Check what ID is found
]


def _norm(s):
    s = s.lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9 ]', ' ', s).strip()


def _match(a, b):
    na, nb = _norm(a), _norm(b)
    stop = {'fc','cf','ac','sc','rb','as','sl','ss','sb','bv','de'}
    wa = set(na.split()) - stop
    wb = set(nb.split()) - stop
    return na in nb or nb in na or bool(wa & wb)


results = {}

for date_csv, local, away, expected_id in TEST_CASES:
    p = date_csv.split('-')
    date_api = f"{p[2]}{p[1]}{p[0]}"

    try:
        r = requests.get(ESPN_BOARD, params={'dates': date_api}, headers=HEADERS, timeout=10)
        events = r.json().get('events', [])
        found_id = None
        found_name = None

        for evt in events:
            comps = evt.get('competitions', [])
            if not comps:
                continue
            comp = comps[0]
            home_name, away_name = '', ''
            for c in comp.get('competitors', []):
                n = c.get('team', {}).get('displayName', '')
                if c.get('homeAway') == 'home':
                    home_name = n
                else:
                    away_name = n
            if (_match(local, home_name) and _match(away, away_name)) or \
               (_match(local, away_name) and _match(away, home_name)):
                found_id = str(evt.get('id'))
                found_name = f"{home_name} vs {away_name}"
                break

        if expected_id:
            ok = (found_id == expected_id)
        else:
            ok = (found_id is not None)
        status = '✓' if ok else '✗'

        result = {
            'status': status, 'date': date_csv,
            'query': f"{local} vs {away}",
            'found_id': found_id, 'found_match': found_name,
            'expected_id': expected_id, 'events_on_date': len(events),
        }
        results[f"{local} vs {away} ({date_csv})"] = result
        print(f"{status} {local} vs {away} ({date_csv})")
        print(f"    Found: id={found_id} '{found_name}'  |  expected={expected_id}  |  events on date={len(events)}")
        if not ok:
            all_evts = [(e.get('id'), e['competitions'][0]['competitors'][0]['team']['displayName'] if e.get('competitions') else '?') for e in events[:5]]
            print(f"    Available events: {all_evts}")

    except Exception as e:
        print(f"✗ {local} vs {away} ({date_csv}) -> ERROR: {e}")
        results[f"{local} vs {away} ({date_csv})"] = {'status': '✗', 'error': str(e)}

os.makedirs('tests/api_diagnostics/results', exist_ok=True)
with open('tests/api_diagnostics/results/espn_discovery.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

passed = sum(1 for v in results.values() if v.get('status') == '✓')
print(f"\nSummary: {passed}/{len(TEST_CASES)} passed | Results saved")
