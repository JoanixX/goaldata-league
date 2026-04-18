import pandas as pd
import numpy as np
import os
import sys
import os

# Ensure local imports work regardless of where the script is run from
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api_clients import EspnClient, UefaClient, RatingClient
from formatter import format_score, format_percentage, format_events, format_referees, format_lineups, format_ratings

def fill_missing_data(csv_path, output_path):
    print(f"Leyendo dataset original: {csv_path}")
    # Read the dataset. Maintain NA as exactly 'NULL' string where it applies or fillna with 'NULL'.
    df = pd.read_csv(csv_path, keep_default_na=False)
    
    # Initialize clients
    espn = EspnClient()
    uefa = UefaClient()
    rc = RatingClient()
    
    modified_df = df.copy()

    for idx, row in modified_df.iterrows():
        # Condition to fill: missing lineups, referees or stats
        missing_squads = row.get('planteles') in ['NULL', '', None]
        missing_referees = row.get('arbitro_principal') in ['NULL', '', None]
        missing_stats = row.get('tiros_totales') in ['NULL', '', None]
        missing_ratings = row.get('puntuaciones_jugadores') in ['NULL', '', None]
        
        if missing_squads or missing_referees or missing_stats or missing_ratings:
            print(f"Procesando fila {idx}: {row['local']} vs {row['visitante']} ({row['fecha']})")
            
            # 1. UEFA DATA (Referees, Lineups, Kickoff, Coaches)
            uefa_match_id = find_uefa_match_id(row['fecha'], row['local'], row['visitante'])
            
            if uefa_match_id:
                details = uefa.get_match_details(uefa_match_id)
                lineups = uefa.get_lineups(uefa_match_id)
                if details and lineups:
                    update_row_from_uefa(modified_df, idx, details, lineups)
            
            # 2. ESPN DATA (Stats fallback, Events)
            event_id = espn.find_match_id(row['fecha'], row['local'], row['visitante'])
            if event_id:
                summary = espn.get_match_summary(event_id)
                if summary:
                    update_row_from_summary(modified_df, idx, summary)
            
            # 3. RATINGS DATA (SofaScore/Flashscore)
            ratings = rc.get_ratings(row['fecha'], row['local'], row['visitante'])
            if ratings:
                modified_df.at[idx, 'puntuaciones_jugadores'] = format_ratings(ratings)
            
            # Final cleanup: ensure no empty strings or NaNs
            for col in modified_df.columns:
                val = modified_df.at[idx, col]
                if val == '' or pd.isna(val) or val is None:
                    modified_df.at[idx, col] = 'NULL'

    print(f"Guardando dataset completado en: {output_path}")
    modified_df.to_csv(output_path, index=False)

def find_uefa_match_id(date, local, visitante):
    """
    Search for UEFA match ID. This is a heuristic/search placeholder.
    """
    # Demonstration: for Valencia vs Schalke 2011, we know it's 2003755 (1st leg)
    if "Valencia" in local and "Schalke" in visitante and "15-02-2011" in date:
        return "2003755"
    return None

def update_row_from_uefa(df, idx, details, lineups):
    """
    Updates DF using UEFA API data.
    """
    try:
        # Referees
        referees = details.get('referees', [])
        df.at[idx, 'arbitro_principal'] = format_referees(referees)
        
        # Time
        kickoff = details.get('kickOffTime', {}).get('dateTime')
        if kickoff:
            # Example: 2011-02-15T20:45:00Z -> 20:45
            time_part = kickoff.split('T')[1][:5]
            df.at[idx, 'hora_inicio'] = time_part
            # Estimate end time (approx 2h later)
            h, m = map(int, time_part.split(':'))
            end_h = (h + 2) % 24
            df.at[idx, 'hora_fin'] = f"{end_h:02d}:{m:02d}"

        # Lineups
        home_name = df.at[idx, 'local']
        away_name = df.at[idx, 'visitante']
        df.at[idx, 'planteles'] = format_lineups(home_name, lineups.get('homeTeam'), away_name, lineups.get('awayTeam'))
        
        # Coaches
        home_coach_obj = lineups.get('homeTeam', {}).get('coach')
        away_coach_obj = lineups.get('awayTeam', {}).get('coach')
        
        home_coach = 'NULL'
        if home_coach_obj:
            home_coach = home_coach_obj.get('person', {}).get('translations', {}).get('name', {}).get('EN', 'NULL')
            
        away_coach = 'NULL'
        if away_coach_obj:
            away_coach = away_coach_obj.get('person', {}).get('translations', {}).get('name', {}).get('EN', 'NULL')
            
        df.at[idx, 'entrenador_local'] = home_coach
        df.at[idx, 'entrenador_visitante'] = away_coach
        
    except Exception as e:
        print(f"Error actualizando desde UEFA: {e}")

def update_row_from_summary(df, idx, summary):
    """
    Parses ESPN summary JSON and updates the DataFrame cell values in-place.
    """
    try:
        header = summary.get('header', {})
        comps = header.get('competitions', [])
        if not comps:
            return
        comp = comps[0]
        status_detail = comp.get('status', {}).get('type', {}).get('detail', '')
        
        competitors = comp.get('competitors', [])
        home_comp = None
        away_comp = None
        for c in competitors:
            if c.get('homeAway') == 'home':
                home_comp = c
            else:
                away_comp = c
                
        # Scores
        if home_comp and away_comp:
            h_score = home_comp.get('score')
            a_score = away_comp.get('score')
            df.at[idx, 'marcador'] = format_score(h_score, a_score, status_detail)
            
        # Try to extract stats from boxscore
        boxscore = summary.get('boxscore', {})
        teams = boxscore.get('teams', [])
        
        home_stats_dict = {}
        away_stats_dict = {}
        for t in teams:
            team_stats = {s.get('name'): s.get('displayValue') for s in t.get('statistics', [])}
            is_home = False
            # Check by team ID or simply by order
            if t.get('team', {}).get('id') == home_comp.get('id'):
                is_home = True
            
            if is_home:
                home_stats_dict = team_stats
            else:
                away_stats_dict = team_stats

        # Map ESPN stat names
        # possessionPct, totalShots, shotsOnTarget, wonCorners, foulsCommitted
        posesion_local = format_percentage(home_stats_dict.get('possessionPct'))
        posesion_visitante = format_percentage(away_stats_dict.get('possessionPct'))
        
        df.at[idx, 'posesion_local'] = posesion_local
        df.at[idx, 'posesion_visitante'] = posesion_visitante
        
        def safe_int(val):
            try: return int(val)
            except: return 0
            
        th = home_stats_dict.get('totalShots')
        ta = away_stats_dict.get('totalShots')
        if th and ta:
            df.at[idx, 'tiros_totales_local'] = th
            df.at[idx, 'tiros_totales_visitante'] = ta
            df.at[idx, 'tiros_totales'] = str(safe_int(th) + safe_int(ta))
            
        sth = home_stats_dict.get('shotsOnTarget')
        sta = away_stats_dict.get('shotsOnTarget')
        if sth and sta:
            df.at[idx, 'tiros_puerta_local'] = sth
            df.at[idx, 'tiros_puerta_visitante'] = sta
            df.at[idx, 'tiros_puerta'] = str(safe_int(sth) + safe_int(sta))
            
        fh = home_stats_dict.get('foulsCommitted')
        fa = away_stats_dict.get('foulsCommitted')
        if fh and fa:
            df.at[idx, 'faltas_local'] = fh
            df.at[idx, 'faltas_visitante'] = fa
            df.at[idx, 'faltas_total'] = str(safe_int(fh) + safe_int(fa))
            
        ch = home_stats_dict.get('wonCorners')
        ca = away_stats_dict.get('wonCorners')
        if ch and ca:
            df.at[idx, 'corners_local'] = ch
            df.at[idx, 'corners_visitante'] = ca
            df.at[idx, 'corners_total'] = str(safe_int(ch) + safe_int(ca))
            
        # Parse Events
        # Goals, Cards, Subs are sometimes in 'details' or 'keyEvents'
        events = summary.get('keyEvents', [])
        # Fallback to plays? Plays might have all events, keyEvents has goals and cards usually.
        # But we need substitutions too. But this is the best effort for now.
        df.at[idx, 'goles'] = format_events(events, 'Goal')
        df.at[idx, 'amarillas'] = format_events(events, 'Yellow')
        df.at[idx, 'rojas'] = format_events(events, 'Red')
        df.at[idx, 'cambios'] = format_events(events, 'Sub')
        
        # We explicitly set empty to 'NULL'
        for col in df.columns:
            if df.at[idx, col] == '':
                df.at[idx, col] = 'NULL'
                
        # Player Ratings handling
        if df.at[idx, 'puntuaciones_jugadores'] in ['', 'NULL']:
            df.at[idx, 'puntuaciones_jugadores'] = 'NULL'

    except Exception as e:
        print(f"  - Excepción procesando fila {idx}: {e}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # New organized paths
    input_csv = os.path.join(base_dir, 'data', 'raw', 'champions_league_2011_2025.csv')
    output_csv = os.path.join(base_dir, 'data', 'processed', 'champions_league_2011_2025_completed.csv')
    
    if os.path.exists(input_csv):
        fill_missing_data(input_csv, output_csv)
    else:
        print(f"Error: no se pudo encontrar el archivo original en {input_csv}")