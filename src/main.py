import pandas as pd
import os, sys, json, subprocess
from datetime import datetime

# Add current dir to path for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.uefa import UEFAScraper
from scrapers.espn import ESPNScraper
from scrapers.fbref import FBRefScraper
from scrapers.flashscore import FlashscoreScraper
from scrapers.worldfootball import WorldFootballScraper
from scrapers.utils import is_null
from config import expand_aliases, load_espn_overrides
from formatter import are_equivalent, format_possession

ENRICHABLE = [
    'hora_inicio', 'hora_fin',
    'arbitro_principal', 'arbitros_linea',
    'entrenador_local', 'entrenador_visitante',
    'planteles',
    'posesion_local', 'posesion_visitante',
    'tiros_totales', 'tiros_totales_local', 'tiros_totales_visitante',
    'tiros_puerta', 'tiros_puerta_local', 'tiros_puerta_visitante',
    'faltas_total', 'faltas_local', 'faltas_visitante',
    'corners_total', 'corners_local', 'corners_visitante',
    'goles', 'amarillas', 'rojas', 'cambios',
    'marcador', 'marcador_global'
]

def run_diagnostics():
    print("[*] Running pre-enrichment diagnostics...")
    script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                          'tests', 'api_diagnostics', 'run_all_tests.py')
    if os.path.exists(script):
        subprocess.run([sys.executable, script], check=False)

def log_mismatch(match_info, field, current_val, new_val):
    log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'mismatches.log')
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] Mismatch in {match_info} | Field: {field} | Current: {current_val} | New: {new_val}\n")

def fill_missing_data(csv_path: str, output_path: str):
    run_diagnostics()
    print(f"[*] Reading dataset: {csv_path}")
    df = pd.read_csv(csv_path, keep_default_na=False)

    # Initialize Scrapers
    uefa_idx_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                 'tests', 'api_diagnostics', 'results', 'uefa', 'match_index.json')
    u_scraper = UEFAScraper(index_path=uefa_idx_path)
    e_scraper = ESPNScraper()
    f_scraper = FBRefScraper()
    fs_scraper = FlashscoreScraper()
    wf_scraper = WorldFootballScraper()
    
    espn_overrides = load_espn_overrides()

    # To calculate global scores, we need to track first legs
    # We'll build a map of (season, local, away) -> score
    first_leg_scores = {}

    for idx, row in df.iterrows():
        season = str(row.get('season', ''))
        fase = str(row.get('fase', ''))
        instancia = str(row.get('instancia', ''))
        local, away, date = str(row.get('local', '')), str(row.get('visitante', '')), str(row.get('fecha', ''))
        match_info = f"{season} | {fase} | {local} vs {away} ({date})"

        # Check for NULLs
        needs_update = False
        for col in ENRICHABLE:
            if col in df.columns and is_null(row.get(col)):
                needs_update = True
                break
        
        if not needs_update: continue
        
        print(f"  [{idx}] Processing: {local} vs {away} ({date})")

        # 1. UEFA Extraction
        u_id = u_scraper.find_id(date, local, away, expand_aliases)
        u_officials, u_lineups = {}, {}
        if u_id:
            u_officials = u_scraper.get_officials(u_id)
            if is_null(row.get('planteles')):
                u_lineups = u_scraper.get_lineups(u_id, local, away)

        # 2. ESPN Extraction
        e_id = e_scraper.find_event(date, local, away, expand_aliases, overrides=espn_overrides)
        e_data = {}
        if e_id:
            e_data = e_scraper.get_data(e_id, local)

        # 3. Fallbacks
        # ... (Indices loading logic could be here or optimized outside)
        
        merged = {**e_data} # Start with ESPN
        
        # Format Score with Penalties: "X-Y P(A-B)"
        if '_espn_penalties' in merged:
            p_score = merged['_espn_penalties']
            m_score = merged.get('marcador', row.get('marcador'))
            if m_score and not is_null(m_score):
                merged['marcador'] = f"{m_score} P({p_score})"

        # 4. Merge & Verify
        for k in ('arbitro_principal', 'arbitros_linea', 'hora_inicio', 'hora_fin'):
            if k in u_officials: merged[k] = u_officials[k]
        
        for k in ('planteles', 'entrenador_local', 'entrenador_visitante'):
            if k in u_lineups and u_lineups[k]: merged[k] = u_lineups[k]

        if 'arbitro_principal' not in merged and '_espn_main_ref' in merged:
            merged['arbitro_principal'] = merged['_espn_main_ref']

        # Global Score Logic
        if fase != 'Final' and is_null(row.get('marcador_global')):
            # This is simplified; ideally we'd look for the other leg in the DF
            pass

        # Update strictly NULLs
        for col, val in merged.items():
            if col.startswith('_'): continue
            if col in df.columns:
                current_val = df.at[idx, col]
                
                # Format before update/check
                if col.startswith('posesion'):
                    val = format_possession(val)

                if is_null(current_val):
                    if val and not is_null(val):
                        df.at[idx, col] = val
                else:
                    # Verify mismatch using advanced logic
                    if val and not is_null(val) and not are_equivalent(current_val, val, col):
                        log_mismatch(match_info, col, current_val, val)

    # Cleanup
    for col in ENRICHABLE:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: 'NULL' if is_null(x) else x)

    print(f"[*] Saving enriched dataset: {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8')

if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw  = os.path.join(base, 'data', 'raw', 'cl_2010_2025.csv')
    out  = os.path.join(base, 'data', 'raw', 'cl_2010_2025_completed.csv')
    
    inp = out if os.path.exists(out) else raw
    if os.path.exists(inp):
        fill_missing_data(inp, out)
    else:
        print(f"[!] Error: File {raw} not found.")