"""
DIAGNOSTIC 1: UEFA Officials
Tests that UEFA API returns arbitro_principal + arbitros_linea for multiple matches.
Run: python tests/api_diagnostics/test_uefa_officials.py
"""
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import requests

UEFA_BASE = 'https://match.uefa.com/v5/matches'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Known UEFA match IDs and their expected referees
TEST_CASES = [
    ('2003755', 'Valencia vs Schalke 04 (15-02-2011)', 'Aleksei Nikolaev', ['Tikhon Kalugin', 'Anton Averianov']),
    ('2044466', 'PSG vs Inter (31-05-2025)', 'István Kovács', ['Mihai Marica', 'Ferencz Tunyogi']),
    ('2009612', 'Bayern vs Dortmund Final 2013', 'Nicola Rizzoli', None),
    ('2039970', 'Real Madrid vs Dortmund Final 2024', 'Slavko Vinčić', None),
]

ROLE_MAIN = 'REFEREE'
ROLE_ASST = {'ASSISTANT_REFEREE_ONE', 'ASSISTANT_REFEREE_TWO'}

results = {}

for match_id, label, expected_main, expected_asst in TEST_CASES:
    try:
        r = requests.get(UEFA_BASE, params={'matchId': match_id}, headers=HEADERS, timeout=12)
        m = r.json()[0] if r.status_code == 200 else {}
        refs = m.get('referees', [])
        main = next((ref['person']['translations']['name']['EN'] for ref in refs if ref.get('role') == ROLE_MAIN), None)
        assistants = [ref['person']['translations']['name']['EN'] for ref in refs if ref.get('role') in ROLE_ASST]
        kickoff = m.get('kickOffTime', {}).get('dateTime', '')
        time = kickoff.split('T')[1][:5] if 'T' in kickoff else 'N/A'
        ok_main = (main == expected_main) if expected_main else (main is not None)
        ok_asst = (len(assistants) >= 2)
        status = '✓' if (ok_main and ok_asst) else '✗'
        results[label] = {
            'status': status, 'match_id': match_id,
            'arbitro_principal': main, 'arbitros_linea': '; '.join(assistants),
            'hora': time, 'ok_main': ok_main, 'ok_asst': ok_asst,
        }
        print(f"{status} [{match_id}] {label}")
        print(f"    ref: {main}  |  asst: {'; '.join(assistants)}  |  hora: {time}")
        if not ok_main:
            print(f"    WARN: expected '{expected_main}' got '{main}'")
        if not ok_asst:
            print(f"    WARN: only {len(assistants)} assistant referee(s)")
    except Exception as e:
        print(f"✗ [{match_id}] {label} -> ERROR: {e}")
        results[label] = {'status': '✗', 'error': str(e)}

os.makedirs('tests/api_diagnostics/results', exist_ok=True)
with open('tests/api_diagnostics/results/uefa_officials.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

passed = sum(1 for v in results.values() if v.get('status') == '✓')
print(f"\nSummary: {passed}/{len(TEST_CASES)} passed | Results saved to tests/api_diagnostics/results/uefa_officials.json")
