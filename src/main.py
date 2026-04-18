import pandas as pd
import numpy as np
import os
import sys
import re
import unicodedata

# Ensure local imports work regardless of where the script is run from
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import requests

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

ESPN_SCOREBOARD = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard'
ESPN_SUMMARY   = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize(s: str) -> str:
    """Lower-case, strip accents, remove punctuation for fuzzy matching."""
    s = s.lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    return ' '.join(s.split())


def teams_match(csv_name: str, espn_name: str) -> bool:
    a = normalize(csv_name)
    b = normalize(espn_name)
    # exact or substring
    if a == b:
        return True
    # at least one word in common (to handle Schalke 04 ≈ Schalke)
    words_a = set(a.split())
    words_b = set(b.split())
    common = words_a & words_b - {'fc', 'cf', 'ac', 'sc', 'bv', 'de', 'sl', 'rb'}
    return len(common) >= 1 and (a in b or b in a or bool(common))


def dd_mm_yyyy_to_yyyymmdd(date_str: str) -> str:
    """Convert '15-02-2011' -> '20110215'."""
    try:
        parts = date_str.split('-')
        return f"{parts[2]}{parts[1]}{parts[0]}"
    except Exception:
        return ''


def safe_pct(val) -> str:
    if val is None:
        return 'NULL'
    try:
        v = float(val)
        return f"{round(v)}%"
    except Exception:
        return 'NULL'


def safe_int_sum(*vals):
    total = 0
    valid = False
    for v in vals:
        try:
            total += int(float(v))
            valid = True
        except Exception:
            pass
    return str(total) if valid else 'NULL'

# ---------------------------------------------------------------------------
# ESPN discovery
# ---------------------------------------------------------------------------

def find_espn_event(date_str: str, local: str, visitante: str) -> str | None:
    """
    date_str: 'DD-MM-YYYY'
    Returns ESPN event id or None.
    """
    date_api = dd_mm_yyyy_to_yyyymmdd(date_str)
    if not date_api:
        return None
    try:
        r = requests.get(ESPN_SCOREBOARD, params={'dates': date_api}, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None
        events = r.json().get('events', [])
        for evt in events:
            comps = evt.get('competitions', [])
            if not comps:
                continue
            comp = comps[0]
            competitors = comp.get('competitors', [])
            home_name = ''
            away_name = ''
            for c in competitors:
                name = c.get('team', {}).get('displayName', '')
                if c.get('homeAway') == 'home':
                    home_name = name
                else:
                    away_name = name
            # Match home = local (from CSV) and away = visitante
            if teams_match(local, home_name) and teams_match(visitante, away_name):
                return str(evt.get('id'))
            # Sometimes ESPN stores it reversed (neutral venues)
            if teams_match(visitante, home_name) and teams_match(local, away_name):
                return str(evt.get('id'))
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# ESPN data extraction
# ---------------------------------------------------------------------------

def extract_espn_summary(event_id: str) -> dict:
    """
    Returns a flat dict with all enrichable fields, or empty dict on failure.
    """
    try:
        r = requests.get(ESPN_SUMMARY, params={'event': event_id}, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
    except Exception:
        return {}

    result = {}

    # --- Officials (referee + linesman) ---
    officials = data.get('gameInfo', {}).get('officials', [])
    seen_officials = set()
    referees = []
    for off in officials:
        name = off.get('displayName', '').strip()
        if name and name not in seen_officials:
            seen_officials.add(name)
            referees.append(name)
    if referees:
        result['arbitro_principal'] = referees[0]
        if len(referees) > 1:
            result['arbitros_linea'] = '; '.join(referees[1:])

    # --- Rosters / Lineups ---
    rosters = data.get('rosters', [])
    lineup_parts = []
    for roster_data in rosters:
        team_name = roster_data.get('team', {}).get('displayName', 'Team')
        entries = roster_data.get('roster', [])
        starters = [e for e in entries if e.get('starter')]
        # Sort by position order: GK first, then defenders, midfielders, forwards
        pos_order = {'G': 0, 'GK': 0, 'CD': 1, 'D': 1, 'M': 2, 'F': 3, 'FW': 3}
        def pos_key(e):
            pos = e.get('position', {}).get('abbreviation', 'X')
            # strip '-L', '-R' suffixes
            pos_root = pos.split('-')[0]
            return pos_order.get(pos_root, 2)
        starters_sorted = sorted(starters, key=pos_key)
        names = []
        for e in starters_sorted:
            n = e.get('athlete', {}).get('displayName', '')
            if n:
                # fix encoding artifacts
                names.append(n)
        lineup_parts.append(f"{team_name}: {'; '.join(names)}")
    if lineup_parts:
        result['planteles'] = ' | '.join(lineup_parts)

    # --- Coaches ---
    for roster_data in rosters:
        is_home_in_espn = None
        comp = data.get('header', {}).get('competitions', [{}])[0]
        for c in comp.get('competitors', []):
            tid = c.get('team', {}).get('id')
            rtid = roster_data.get('team', {}).get('id')
            if tid == rtid:
                is_home_in_espn = (c.get('homeAway') == 'home')
                break
        coaches = roster_data.get('coaches', [])
        coach_name = coaches[0].get('displayName', '') if coaches else ''
        if coach_name and is_home_in_espn is not None:
            if is_home_in_espn:
                result['_coach_home_espn'] = coach_name
            else:
                result['_coach_away_espn'] = coach_name

    # --- Stats ---
    bs_teams = data.get('boxscore', {}).get('teams', [])
    # Map team id -> stats dict
    comp = data.get('header', {}).get('competitions', [{}])[0]
    home_id = None
    away_id = None
    for c in comp.get('competitors', []):
        if c.get('homeAway') == 'home':
            home_id = c.get('team', {}).get('id')
        else:
            away_id = c.get('team', {}).get('id')

    home_stats = {}
    away_stats = {}
    for t in bs_teams:
        tid = t.get('team', {}).get('id')
        stats = {s.get('name'): s.get('displayValue') for s in t.get('statistics', [])}
        if tid == home_id:
            home_stats = stats
        else:
            away_stats = stats

    def gs(d, key):
        v = d.get(key)
        return v if v is not None else None

    ph = gs(home_stats, 'possessionPct')
    pa = gs(away_stats, 'possessionPct')
    if ph is not None:
        result['posesion_local'] = safe_pct(ph)
    if pa is not None:
        result['posesion_visitante'] = safe_pct(pa)

    th = gs(home_stats, 'totalShots')
    ta = gs(away_stats, 'totalShots')
    if th is not None and ta is not None:
        result['tiros_totales_local']    = th
        result['tiros_totales_visitante'] = ta
        result['tiros_totales']          = safe_int_sum(th, ta)
    elif th is not None:
        result['tiros_totales_local'] = th
    elif ta is not None:
        result['tiros_totales_visitante'] = ta

    sth = gs(home_stats, 'shotsOnTarget')
    sta = gs(away_stats, 'shotsOnTarget')
    if sth is not None and sta is not None:
        result['tiros_puerta_local']    = sth
        result['tiros_puerta_visitante'] = sta
        result['tiros_puerta']          = safe_int_sum(sth, sta)

    fh = gs(home_stats, 'foulsCommitted')
    fa = gs(away_stats, 'foulsCommitted')
    if fh is not None and fa is not None:
        result['faltas_local']    = fh
        result['faltas_visitante'] = fa
        result['faltas_total']    = safe_int_sum(fh, fa)

    ch = gs(home_stats, 'wonCorners')
    ca = gs(away_stats, 'wonCorners')
    if ch is not None and ca is not None:
        result['corners_local']    = ch
        result['corners_visitante'] = ca
        result['corners_total']    = safe_int_sum(ch, ca)

    # --- Key Events (goals, cards, subs) ---
    key_events = data.get('keyEvents', [])
    goals = []
    yellows = []
    reds = []
    subs_in = []   # [time, player_in, player_out]

    for evt in key_events:
        etype = evt.get('type', {}).get('text', '').lower()
        clock = evt.get('clock', {}).get('displayValue', '')
        participants = evt.get('participants', [])
        names = [p.get('athlete', {}).get('displayName', '?') for p in participants]
        team_home = None
        for c in comp.get('competitors', []):
            if c.get('team', {}).get('id') == evt.get('team', {}).get('id'):
                team_home = (c.get('homeAway') == 'home')
                break

        if 'goal' in etype and names:
            goals.append(f"{names[0]} {clock}'")
        elif 'yellow' in etype and names:
            yellows.append(f"{names[0]} {clock}'")
        elif 'red' in etype and names:
            reds.append(f"{names[0]} {clock}'")
        elif 'substitution' in etype and len(names) >= 2:
            # ESPN reports: [player_in, player_out]
            subs_in.append(f"{clock}' {names[0]} x {names[1]}")

    if goals:
        result['goles'] = '; '.join(goals)
    if yellows:
        result['amarillas'] = '; '.join(yellows)
    if reds:
        result['rojas'] = '; '.join(reds)
    if subs_in:
        result['cambios'] = '; '.join(subs_in)

    return result


# ---------------------------------------------------------------------------
# UEFA API enrichment (time, lineups fallback)
# ---------------------------------------------------------------------------

UEFA_MATCHES = 'https://match.uefa.com/v5/matches'

KNOWN_UEFA_IDS = {
    # key: (date_dd_mm_yyyy, local_fragment, visitante_fragment) -> matchId
    ('15-02-2011', 'Valencia', 'Schalke'): '2003755',
}


def find_uefa_match_id(date: str, local: str, visitante: str) -> str | None:
    for (d, h, a), mid in KNOWN_UEFA_IDS.items():
        if d == date and h.lower() in local.lower() and a.lower() in visitante.lower():
            return mid
    return None


def get_uefa_details(match_id: str) -> dict:
    result = {}
    try:
        r = requests.get(UEFA_MATCHES, params={'matchId': match_id}, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            m = data[0] if isinstance(data, list) and data else {}
            kickoff = m.get('kickOffTime', {}).get('dateTime', '')
            if kickoff:
                time_part = kickoff.split('T')[1][:5]
                result['hora_inicio'] = time_part
                h, mn = map(int, time_part.split(':'))
                result['hora_fin'] = f"{(h + 2) % 24:02d}:{mn:02d}"
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

IS_NULL = ('NULL', '', None, np.nan)


def is_null(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and np.isnan(val):
        return True
    return str(val).strip() in ('NULL', '')


def fill_missing_data(csv_path: str, output_path: str):
    print(f"Reading dataset: {csv_path}")
    df = pd.read_csv(csv_path, keep_default_na=False)

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

    for idx, row in df.iterrows():
        local     = str(row.get('local', ''))
        visitante = str(row.get('visitante', ''))
        fecha     = str(row.get('fecha', ''))

        # Only process rows that are missing at least one enrichable field
        needs_enrichment = any(is_null(row.get(col)) for col in ENRICHABLE)
        if not needs_enrichment:
            continue

        print(f"  Enriching row {idx}: {local} vs {visitante} ({fecha})")

        # 1. ESPN auto-discovery
        event_id = find_espn_event(fecha, local, visitante)
        espn_data = {}
        if event_id:
            espn_data = extract_espn_summary(event_id)
        else:
            print(f"    [!] ESPN event not found")

        # 2. UEFA for time (if known)
        uefa_mid = find_uefa_match_id(fecha, local, visitante)
        uefa_data = {}
        if uefa_mid:
            uefa_data = get_uefa_details(uefa_mid)

        # 3. Apply data — never overwrite a good existing value
        merged = {**espn_data, **uefa_data}  # UEFA takes precedence for time fields

        for col, val in merged.items():
            if col.startswith('_'):
                continue
            if col in df.columns and is_null(df.at[idx, col]):
                df.at[idx, col] = val

        # Coaches: map ESPN home/away to CSV local/visitante
        # Need to figure out if ESPN home == CSV local
        if '_coach_home_espn' in merged or '_coach_away_espn' in merged:
            # Determine orientation from scoreboard
            # If ESPN home team matches CSV local → direct mapping
            coach_home = merged.get('_coach_home_espn', '')
            coach_away = merged.get('_coach_away_espn', '')
            if is_null(df.at[idx, 'entrenador_local']) and coach_home:
                df.at[idx, 'entrenador_local'] = coach_home
            if is_null(df.at[idx, 'entrenador_visitante']) and coach_away:
                df.at[idx, 'entrenador_visitante'] = coach_away

        # Final cleanup: empty strings → NULL
        for col in ENRICHABLE:
            if col in df.columns and str(df.at[idx, col]).strip() == '':
                df.at[idx, col] = 'NULL'

    # Global cleanup: any remaining empty → NULL
    for col in df.columns:
        df[col] = df[col].apply(lambda x: 'NULL' if str(x).strip() == '' else x)

    print(f"Saving enriched dataset: {output_path}")
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_csv  = os.path.join(base_dir, 'data', 'raw',       'champions_league_2011_2025.csv')
    output_csv = os.path.join(base_dir, 'data', 'processed', 'champions_league_2011_2025_completed.csv')

    if os.path.exists(input_csv):
        fill_missing_data(input_csv, output_csv)
    else:
        print(f"Error: raw CSV not found at {input_csv}")