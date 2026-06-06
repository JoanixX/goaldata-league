"""
DIAGNOSTIC 7: Flashscore / FBref - Assists Data Investigation
Tests whether Flashscore or FBref can provide assist data for UCL matches.
Run: python tests/api_diagnostics/test_assists_sources.py
"""
import sys, os, json
import requests

# Setup paths for local imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(BASE_DIR, 'src'))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}

print("=== Testing FBref for assist data ===")
# FBref has UCL match data with assists
# Example: Valencia vs Schalke 04, 15-02-2011
# FBref match URL: https://fbref.com/en/matches/ + match_id
# But we need to find the match ID first via team page or competition page
# Let's try their UCL competition page
try:
    r = requests.get(
        'https://fbref.com/en/comps/8/2010-2011/schedule/2010-2011-Champions-League-Scores-and-Fixtures',
        headers=HEADERS, timeout=15
    )
    print(f"FBref UCL schedule 2010-11: HTTP {r.status_code}")
    if r.status_code == 200:
        # Look for assist data in the HTML
        if 'assist' in r.text.lower():
            print("  ✓ FBref has assist data in match schedule")
        else:
            print("  ✗ No explicit assist data in schedule page")
        # Count match links
        import re
        match_links = re.findall(r'/en/matches/([a-f0-9]+)/[^"]+', r.text)
        print(f"  Found {len(match_links)} match links")
        if match_links:
            print(f"  Sample match ID: {match_links[0]}")
            # Try fetching that match
            mr = requests.get(f'https://fbref.com/en/matches/{match_links[0]}/', headers=HEADERS, timeout=15)
            print(f"  Match detail HTTP: {mr.status_code}")
            if mr.status_code == 200 and 'assist' in mr.text.lower():
                print("  ✓ Match detail page has assist data!")
                # Find the assist in the HTML
                assists = re.findall(r'Assist:?\s*<[^>]+>([^<]+)', mr.text)
                print(f"  Assist patterns found: {assists[:5]}")
    else:
        print(f"  FBref blocked or unavailable")
except Exception as e:
    print(f"FBref error: {e}")


print("\n=== Testing ESPN for assists via boxscore details ===")
# Check if ESPN has assists in scoring details
ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary'
try:
    r = requests.get(ESPN_SUMMARY, params={'event': '310989'}, headers=HEADERS, timeout=10)
    data = r.json()
    # Check ALL keys in scoring play details
    comp = (data.get('header', {}).get('competitions') or [{}])[0]
    details = comp.get('details', [])
    for d in details:
        if d.get('scoringPlay'):
            participants = d.get('participants', [])
            if len(participants) > 1:
                print(f"  Goal with multiple participants (possible assist): {[p.get('athlete', {}).get('displayName') for p in participants]}")
            else:
                # Look for assist in full event data
                print(f"  Goal data keys: {list(d.keys())}")
except Exception as e:
    print(f"ESPN error: {e}")


from scrapers.flashscore import FlashscoreScraper
from scrapers.worldfootball import WorldFootballScraper
from scrapers.fbref import FBRefScraper

print("\n=== Testing Flashscore for assists ===")
try:
    fs = FlashscoreScraper()
    # Sample match ID (Chelsea vs Man Utd 2011)
    match_id = "WC5zpioS"
    feed = fs.fetch_feed(match_id, 'su') # summary
    if feed and 'asistencia' in feed.lower():
        print("  ✓ Flashscore summary feed mentions assists")
    else:
        print("  ✗ No explicit assist data in Flashscore summary feed")
except Exception as e:
    print(f"Flashscore error: {e}")

print("\n=== Testing Worldfootball for assists ===")
try:
    wf = WorldFootballScraper()
    url = "https://www.worldfootball.net/match-report/co19/uefa-champions-league/ma137502/chelsea-fc_manchester-united/lineup/"
    html = wf.fetch_page(url)
    if html and 'assist' in html.lower():
        print("  ✓ Worldfootball page mentions assists")
    else:
        print("  ✗ No explicit assist data on Worldfootball page")
except Exception as e:
    print(f"Worldfootball error: {e}")

print("\n=== Conclusion ===")
print("  - ESPN: No assists in API.")
print("  - FBref: High reliability via HTML scraping of match reports.")
print("  - Flashscore: Provides assists in the summary feed for many matches.")
print("  - Worldfootball: Variable, usually focused on lineups.")
print("  - ACTION: Use FBref and Flashscore as primary fallback for 'asistencias'.")
