"""
DIAGNOSTIC 2: ESPN Stats
Tests that ESPN summary returns all required stats fields for multiple matches.
Run: python tests/api_diagnostics/test_espn_stats.py
Required fields: posesion_local/visitante, tiros_totales/puerta (local+visitante+total),
                 faltas (local+visitante+total), corners (local+visitante+total)
"""
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import requests

ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

TEST_CASES = [
    ('310989', 'Valencia vs Schalke 04 (15-02-2011)', {'possessionPct': '64.6', 'totalShots': '15', 'shotsOnTarget': '6', 'foulsCommitted': '9', 'wonCorners': '5'}),
    ('365094', 'Bayern vs Dortmund Final 2013', None),
    ('702410', 'Real Madrid vs Dortmund Final 2024', None),
    ('941581', 'Man City vs Inter Final 2023', None),
]

REQUIRED_STATS = [
    'possessionPct', 'totalShots', 'shotsOnTarget',
    'foulsCommitted', 'wonCorners',
]

results = {}

for event_id, label, expected_home_stats in TEST_CASES:
    try:
        r = requests.get(ESPN_SUMMARY, params={'event': event_id}, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            print(f"✗ [{event_id}] {label} -> HTTP {r.status_code}")
            results[label] = {'status': '✗', 'http': r.status_code}
            continue

        data = r.json()
        comp = (data.get('header', {}).get('competitions') or [{}])[0]
        home_id = None
        away_id = None
        for c in comp.get('competitors', []):
            if c.get('homeAway') == 'home':
                home_id = c.get('team', {}).get('id')
            else:
                away_id = c.get('team', {}).get('id')

        bs_teams = data.get('boxscore', {}).get('teams', [])
        home_stats, away_stats = {}, {}
        for t in bs_teams:
            tid = t.get('team', {}).get('id')
            stats = {s.get('name'): s.get('displayValue') for s in t.get('statistics', [])}
            if tid == home_id:
                home_stats = stats
            else:
                away_stats = stats

        missing_home = [f for f in REQUIRED_STATS if f not in home_stats]
        missing_away = [f for f in REQUIRED_STATS if f not in away_stats]
        ok = (not missing_home and not missing_away)
        status = '✓' if ok else '✗'

        summary = {
            'posesion_local': f"{round(float(home_stats.get('possessionPct', 0)))}%",
            'posesion_visitante': f"{round(float(away_stats.get('possessionPct', 0)))}%",
            'tiros_totales_local': home_stats.get('totalShots', 'N/A'),
            'tiros_totales_visitante': away_stats.get('totalShots', 'N/A'),
            'tiros_puerta_local': home_stats.get('shotsOnTarget', 'N/A'),
            'tiros_puerta_visitante': away_stats.get('shotsOnTarget', 'N/A'),
            'faltas_local': home_stats.get('foulsCommitted', 'N/A'),
            'faltas_visitante': away_stats.get('foulsCommitted', 'N/A'),
            'corners_local': home_stats.get('wonCorners', 'N/A'),
            'corners_visitante': away_stats.get('wonCorners', 'N/A'),
        }

        results[label] = {'status': status, 'event_id': event_id, **summary,
                          'missing_home': missing_home, 'missing_away': missing_away}
        print(f"{status} [{event_id}] {label}")
        for k, v in summary.items():
            print(f"    {k}: {v}")
        if missing_home:
            print(f"    WARN missing home stats: {missing_home}")
        if missing_away:
            print(f"    WARN missing away stats: {missing_away}")

    except Exception as e:
        print(f"✗ [{event_id}] {label} -> ERROR: {e}")
        results[label] = {'status': '✗', 'error': str(e)}

os.makedirs('tests/api_diagnostics/results', exist_ok=True)
with open('tests/api_diagnostics/results/espn_stats.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

passed = sum(1 for v in results.values() if v.get('status') == '✓')
print(f"\nSummary: {passed}/{len(TEST_CASES)} passed | Results saved to tests/api_diagnostics/results/espn_stats.json")
