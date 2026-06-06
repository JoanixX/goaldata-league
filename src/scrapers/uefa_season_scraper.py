import os
import sys
import json
import time
import pandas as pd
from playwright.sync_api import sync_playwright

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from formatter import generate_player_id

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'uefa')

# Column IDs for each category on UEFA's ag-grid tables
CATEGORY_COLS = {
    'goalkeeping': ['saves', 'goals_conceded', 'clean_sheets', 'penalty_saves', 'punches'],
    'disciplinary': ['yellow_cards', 'red_cards'],
    'attacking': ['goals', 'assists'],
    'passing': ['passes_completed', 'passes_attempted'],
}


def scrape_uefa_season_category(page, season_year, category):
    """Scrapes a category for a season from UEFA's ag-grid stats pages."""
    url = f"https://es.uefa.com/uefachampionsleague/history/seasons/{season_year}/statistics/players/{category}/"
    print(f"  Scraping: {url}")
    
    try:
        page.goto(url, timeout=45000, wait_until='networkidle')
        
        # Wait for ag-grid to render
        try:
            page.wait_for_selector('.ag-row', timeout=10000)
        except:
            print(f"    No ag-grid rows found, trying scroll...")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            try:
                page.wait_for_selector('.ag-row', timeout=5000)
            except:
                print(f"    Still no data, skipping")
                return []
        
        # Scroll to load all rows
        for _ in range(5):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.8)
        
        # Extract data from ag-grid rows
        rows = page.locator('.ag-center-cols-container .ag-row').all()
        col_ids = CATEGORY_COLS.get(category, [])
        
        data = []
        for row in rows:
            try:
                # Player name cell
                name_cell = row.locator('.ag-cell[col-id="player_name"]')
                player_name = name_cell.inner_text().strip() if name_cell.count() > 0 else ''
                
                if not player_name:
                    continue
                
                # Extract stats from each column
                stats = {}
                for col_id in col_ids:
                    cell = row.locator(f'.ag-cell[col-id="{col_id}"]')
                    val = cell.inner_text().strip() if cell.count() > 0 else '0'
                    try:
                        stats[col_id] = int(val) if val.isdigit() else 0
                    except:
                        stats[col_id] = 0
                
                data.append({
                    'player_name': player_name,
                    'stats': stats
                })
            except Exception:
                continue
        
        return data
    except Exception as e:
        print(f"  [!] Error scraping {url}: {e}")
        return []


def populate_season_stats():
    """Scrapes UEFA aggregate stats and saves raw results as JSON dump."""
    print("[*] Starting UEFA Season Stats scrape...")
    os.makedirs(RESULTS_DIR, exist_ok=True)

    seasons = [str(y) for y in range(2011, 2026)]
    categories = list(CATEGORY_COLS.keys())
    
    all_results = {cat: [] for cat in categories}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = ctx.new_page()

        for year in seasons:
            season_str = f"{year}-{int(year)+1}"
            for cat in categories:
                rows = scrape_uefa_season_category(page, year, cat)
                for row in rows:
                    row['season'] = season_str
                    row['player_id'] = generate_player_id(row['player_name'])
                all_results[cat].extend(rows)

        browser.close()

    # Save raw dump
    dump_path = os.path.join(RESULTS_DIR, 'raw_dump.json')
    with open(dump_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    total = sum(len(v) for v in all_results.values())
    print(f"[V] UEFA dump saved: {total} records across {len(categories)} categories.")
    
    # Save sample
    sample = []
    for cat, rows in all_results.items():
        for r in rows[:3]:
            sample.append({**r, 'category': cat})
    with open(os.path.join(RESULTS_DIR, 'sample_data.json'), 'w', encoding='utf-8') as f:
        json.dump(sample, f, indent=2, ensure_ascii=False)

    return all_results


if __name__ == "__main__":
    populate_season_stats()
