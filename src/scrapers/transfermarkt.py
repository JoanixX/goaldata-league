import os
import sys
import json
import time
import re
import pandas as pd
from playwright.sync_api import sync_playwright

# Setup paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from formatter import generate_player_id, soft_norm

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'transfermarkt')


def search_player(page, player_name):
    """Search Transfermarkt for a player and return their profile URL slug + ID."""
    search_url = f"https://www.transfermarkt.pe/schnellsuche/ergebnis/schnellsuche?query={player_name}"
    try:
        page.goto(search_url, timeout=30000, wait_until='domcontentloaded')
        time.sleep(1)
        
        # First player result link
        link = page.locator('table.items tbody tr:first-child td.hauptlink a').first
        if link.count() > 0:
            href = link.get_attribute('href')
            if href and '/profil/spieler/' in href:
                parts = href.split('/')
                tm_id = parts[-1]
                slug = parts[1]
                return slug, tm_id
    except Exception:
        pass
    return None, None


def scrape_player_profile(page, slug, tm_id, player_name):
    """Scrape a player's profile page for personal details.
    
    Transfermarkt stores info as list items with format:
        - F. Nacim./Edad: 28/07/1987 (38)
        - Altura: 1,67 m
        - Posición: Extremo derecho
        - Nacionalidad: España
        - Lugar de nac.: Santa Cruz de Tenerife
    """
    url = f"https://www.transfermarkt.pe/{slug}/profil/spieler/{tm_id}"
    try:
        page.goto(url, timeout=30000, wait_until='domcontentloaded')
        time.sleep(1)
        
        details = {
            'player_name': player_name,
            'birth_date': 'NULL',
            'height_cm': 'NULL',
            'position': 'NULL',
            'nationality': 'NULL',
            'birth_place': 'NULL'
        }
        
        # Get full page text and parse with regex
        text = page.inner_text('body')
        
        # Birth date: "F. Nacim./Edad:\n28/07/1987 (38)" or "Date of birth/Age:\n..."
        m = re.search(r'(?:Nacim|birth|Edad)[^:]*:\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
        if m:
            details['birth_date'] = m.group(1)
        
        # Height: "Altura:\n1,67 m" or "Height:\n1,67 m"
        m = re.search(r'(?:Altura|Height)[^:]*:\s*([\d,\.]+)\s*m', text, re.IGNORECASE)
        if m:
            h = m.group(1).replace(',', '.')
            try:
                details['height_cm'] = int(float(h) * 100)
            except:
                pass
        
        # Position: "Posición:\nExtremo derecho" or "Position:\n..."
        m = re.search(r'(?:Posición|Position)[^:]*:\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if m:
            pos = m.group(1).strip()
            if pos and len(pos) < 40:
                details['position'] = pos
        
        # Nationality: "Nacionalidad:\nEspaña" or "Citizenship:\n..."
        m = re.search(r'(?:Nacionalidad|Nationality|Citizenship)[^:]*:\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if m:
            nat = m.group(1).strip()
            if nat and len(nat) < 40:
                details['nationality'] = nat
        
        # Birth place: "Lugar de nac.:\nSanta Cruz de ..." 
        m = re.search(r'(?:Lugar de nac|Place of birth)[^:]*:\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if m:
            bp = m.group(1).strip()
            if bp and len(bp) < 60:
                details['birth_place'] = bp
        
        return details
    except Exception as e:
        print(f"  [!] Error scraping {player_name}: {e}")
        return None


def run_transfermarkt_scraper():
    """Scrape Transfermarkt for all players with missing profile data."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    players_path = os.path.join(BASE_DIR, 'data', 'raw', 'core', 'players.csv')
    if not os.path.exists(players_path):
        print("[!] players.csv not found")
        return
    
    df = pd.read_csv(players_path, keep_default_na=False)
    
    # Find players with missing data
    needs_data = df[
        (df['nationality'].isin(['Unknown', '', 'NULL'])) | 
        (df['position'].isin(['Unknown', '', 'NULL'])) |
        (df['height_cm'].isin(['', 'NULL', 0, '0']))
    ]
    
    print(f"[*] Found {len(needs_data)} players with missing profile data")
    
    batch = needs_data.head(100)
    print(f"[*] Scraping batch of {len(batch)} players from Transfermarkt...")
    
    results = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = ctx.new_page()
        
        for i, (_, row) in enumerate(batch.iterrows()):
            name = row['player_name']
            if not name or name == 'Unknown':
                continue
            
            print(f"  [{i+1}/{len(batch)}] Searching: {name}")
            
            slug, tm_id = search_player(page, name)
            if not slug:
                print(f"    Not found on Transfermarkt")
                continue
            
            details = scrape_player_profile(page, slug, tm_id, name)
            if details and details.get('nationality') != 'NULL':
                results.append(details)
                print(f"    Found: {details.get('nationality', '?')}, {details.get('position', '?')}, {details.get('height_cm', '?')}cm")
            else:
                print(f"    Profile found but no data extracted")
            
            time.sleep(1.5)
        
        browser.close()
    
    dump_path = os.path.join(RESULTS_DIR, 'raw_dump.json')
    with open(dump_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"[V] Transfermarkt dump saved: {len(results)} player profiles")
    
    sample_path = os.path.join(RESULTS_DIR, 'sample_data.json')
    with open(sample_path, 'w', encoding='utf-8') as f:
        json.dump(results[:5], f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    run_transfermarkt_scraper()
