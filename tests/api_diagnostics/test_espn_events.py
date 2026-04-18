"""
DIAGNOSTIC 3: ESPN Events (goals, cards, substitutions)
Tests that ESPN summary returns goles, amarillas, rojas, cambios.
Run: python tests/api_diagnostics/test_espn_events.py
"""
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import requests

ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

TEST_CASES = [
    ('310989', 'Valencia vs Schalke 04 (15-02-2011)'),
    ('365094', 'Bayern vs Dortmund Final (25-05-2013)'),
    ('702410', 'Real Madrid vs Dortmund Final (01-06-2024)'),
]

results = {}

for event_id, label in TEST_CASES:
    try:
        r = requests.get(ESPN_SUMMARY, params={'event': event_id}, headers=HEADERS, timeout=12)
        data = r.json()
        key_events = data.get('keyEvents', [])
        comp = (data.get('header', {}).get('competitions') or [{}])[0]
        details = comp.get('details', [])

        goals, yellows, reds, subs = [], [], [], []

        for evt in key_events:
            etype = evt.get('type', {}).get('text', '').lower()
            clock = evt.get('clock', {}).get('displayValue', '')
            parts = evt.get('participants', [])
            names = [p.get('athlete', {}).get('displayName', '?') for p in parts]
            if 'goal' in etype and names:
                goals.append(f"{names[0]} {clock}'")
            elif 'yellow' in etype and names:
                yellows.append(f"{names[0]} {clock}'")
            elif 'red' in etype and names:
                reds.append(f"{names[0]} {clock}'")
            elif 'substitution' in etype and len(names) >= 2:
                subs.append(f"{clock}' {names[0]} x {names[1]}")

        # Fallback cards from details
        if not yellows:
            for d in details:
                dtype = d.get('type', {}).get('text', '').lower()
                clock = d.get('clock', {}).get('displayValue', '')
                athletes = d.get('athletesInvolved', [])
                if 'yellow' in dtype and athletes:
                    yellows.append(f"{athletes[0].get('displayName','?')} {clock}'")
                elif 'red' in dtype and athletes:
                    reds.append(f"{athletes[0].get('displayName','?')} {clock}'")

        ok = bool(goals)  # At minimum should have goals (unless 0-0)
        status = '✓' if ok else '✗'

        result = {
            'status': status, 'event_id': event_id,
            'goles': '; '.join(goals) or 'NONE',
            'amarillas': '; '.join(yellows) or 'NONE',
            'rojas': '; '.join(reds) or 'NONE',
            'cambios_count': len(subs),
            'cambios_sample': subs[:3],
        }
        results[label] = result
        print(f"{status} [{event_id}] {label}")
        print(f"    goles: {result['goles']}")
        print(f"    amarillas: {result['amarillas']}")
        print(f"    rojas: {result['rojas']}")
        print(f"    cambios: {len(subs)} total, sample: {subs[:2]}")

    except Exception as e:
        print(f"✗ [{event_id}] {label} -> ERROR: {e}")
        results[label] = {'status': '✗', 'error': str(e)}

os.makedirs('tests/api_diagnostics/results', exist_ok=True)
with open('tests/api_diagnostics/results/espn_events.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

passed = sum(1 for v in results.values() if v.get('status') == '✓')
print(f"\nSummary: {passed}/{len(TEST_CASES)} passed | Results saved")
