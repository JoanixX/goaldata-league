"""
DIAGNOSTIC 4: ESPN Rosters (lineups + coaches)
Tests that ESPN returns 11 starters and coach for both teams.
Run: python tests/api_diagnostics/test_espn_rosters.py
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
    ('941581', 'Man City vs Inter Final (10-06-2023)'),
    ('1151780', 'Real Madrid vs Atletico Final (28-05-2016)'),
]

results = {}

for event_id, label in TEST_CASES:
    try:
        r = requests.get(ESPN_SUMMARY, params={'event': event_id}, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            print(f"✗ [{event_id}] {label} -> HTTP {r.status_code}")
            results[label] = {'status': '✗', 'http': r.status_code}
            continue

        data = r.json()
        rosters = data.get('rosters', [])

        lineup_str = []
        coaches = {}
        issues = []

        for roster_data in rosters:
            tname = roster_data.get('team', {}).get('displayName', 'Team')
            entries = roster_data.get('roster', [])
            starters = [e for e in entries if e.get('starter')]
            coach_list = roster_data.get('coaches', [])
            coach = coach_list[0].get('displayName', 'N/A') if coach_list else 'N/A'
            coaches[tname] = coach

            names = [e.get('athlete', {}).get('displayName', '?') for e in starters]
            lineup_str.append(f"{tname}: {'; '.join(names)}")

            if len(starters) < 11:
                issues.append(f"{tname} only {len(starters)} starters")
            if coach == 'N/A':
                issues.append(f"{tname} missing coach")

        ok = (len(rosters) == 2 and not issues)
        status = '✓' if ok else '✗'

        result = {
            'status': status, 'event_id': event_id,
            'rosters_count': len(rosters),
            'coaches': coaches,
            'lineup_preview': ' | '.join(lineup_str)[:200],
            'issues': issues,
        }
        results[label] = result
        print(f"{status} [{event_id}] {label}")
        for tname, coach in coaches.items():
            print(f"    coach [{tname}]: {coach}")
        print(f"    lineup: {' | '.join(lineup_str)[:180]}")
        if issues:
            print(f"    ISSUES: {issues}")

    except Exception as e:
        print(f"✗ [{event_id}] {label} -> ERROR: {e}")
        results[label] = {'status': '✗', 'error': str(e)}

os.makedirs('tests/api_diagnostics/results', exist_ok=True)
with open('tests/api_diagnostics/results/espn_rosters.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

passed = sum(1 for v in results.values() if v.get('status') == '✓')
print(f"\nSummary: {passed}/{len(TEST_CASES)} passed | Results saved")
