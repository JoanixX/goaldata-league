"""
UCL Data Enrichment Pipeline (Clean Version)
Sources: UEFA API, ESPN API, FBref (Fallback)
"""
import pandas as pd
import os, sys, json

# Add current dir to path for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.uefa import UEFAScraper
from scrapers.espn import ESPNScraper
from scrapers.fbref import FBRefScraper
from scrapers.flashscore import FlashscoreScraper
from scrapers.worldfootball import WorldFootballScraper
from scrapers.utils import is_null
from config import expand_aliases, load_espn_overrides

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
]

def fill_missing_data(csv_path: str, output_path: str):
    print(f"[*] Reading dataset: {csv_path}")
    df = pd.read_csv(csv_path, keep_default_na=False)

    # Initialize Scrapers
    uefa_idx = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                            'tests', 'api_diagnostics', 'results', 'uefa', 'match_index.json')
    u_scraper = UEFAScraper(index_path=uefa_idx)
    e_scraper = ESPNScraper()
    f_scraper = FBRefScraper()
    fs_scraper = FlashscoreScraper()
    wf_scraper = WorldFootballScraper()
    
    espn_overrides = load_espn_overrides()

    for idx, row in df.iterrows():
        season    = str(row.get('season', ''))
        local, away, date = str(row.get('local', '')), str(row.get('visitante', '')), str(row.get('fecha', ''))
        
        has_stats = not is_null(row.get('tiros_totales'))
        has_refs = not is_null(row.get('arbitro_principal'))
        
        if has_stats and has_refs: continue
        
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

        # 3. External Fallbacks (Flashscore & Worldfootball)
        ext_data = {}
        
        # Load indices if needed (optimally load once outside loop)
        def _load_idx(name):
            p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                             'tests', 'api_diagnostics', 'results', name, 'match_index.json')
            if os.path.exists(p):
                with open(p, encoding='utf-8') as f: return json.load(f)
            return {}

        fs_idx = _load_idx('flashscore')
        wf_idx = _load_idx('worldfootball')
        fb_idx = _load_idx('fbref')

        match_key = f"{local} vs {away} | {date}"
        
        # Flashscore for Stats
        if (not has_stats or is_null(e_data.get('tiros_totales'))) and match_key in fs_idx:
            fs_mid = fs_idx[match_key]
            print(f"    -> Flashscore fallback (ID: {fs_mid})")
            fs_data = fs_scraper.get_match_data(fs_mid)
            ext_data.update(fs_data.get('stats', {}))

        # Worldfootball for Lineups
        if is_null(row.get('planteles')) and is_null(u_lineups.get('planteles')) and is_null(e_data.get('planteles')):
            if match_key in wf_idx:
                wf_url = wf_idx[match_key]
                print(f"    -> Worldfootball fallback (URL: {wf_url})")
                html = wf_scraper.fetch_page(wf_url)
                wf_res = wf_scraper.parse_match_report(html)
                if wf_res and wf_res.get('lineups'):
                    l, a = wf_res['lineups'].get('home', []), wf_res['lineups'].get('away', [])
                    ext_data['planteles'] = f"{local}: {'; '.join(l)} | {away}: {'; '.join(a)}"

        # 4. FBref Extraction (Lowest priority fallback)
        fb_data = {}
        if is_null(ext_data.get('tiros_totales')) and is_null(e_data.get('tiros_totales')) and not has_stats:
            if match_key in fb_idx:
                fb_url = fb_idx[match_key]
                print(f"    -> FBref fallback (URL: {fb_url})")
                html = f_scraper.fetch_page(fb_url)
                fb_data = f_scraper.parse_match_data(html)
                # Map FBref stats to schema if needed
                # (FBref provides granular player stats, would need aggregation here)

        # --- Merge Logic ---
        # Priority: UEFA (officials) > ESPN (stats) > Flashscore (stats) > Worldfootball (lineups) > FBref
        merged = {**ext_data, **e_data}
        
        # UEFA Overrides
        for k in ('arbitro_principal', 'arbitros_linea', 'hora_inicio', 'hora_fin'):
            if k in u_officials: merged[k] = u_officials[k]
        
        for k in ('planteles', 'entrenador_local', 'entrenador_visitante'):
            if k in u_lineups and u_lineups[k]: merged[k] = u_lineups[k]

        if 'arbitro_principal' not in merged and '_espn_main_ref' in merged:
            merged['arbitro_principal'] = merged['_espn_main_ref']

        # Update DataFrame
        for col, val in merged.items():
            if col.startswith('_'): continue
            if col in df.columns and is_null(df.at[idx, col]) and val and not is_null(val):
                df.at[idx, col] = val

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