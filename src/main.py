"""
UCL Data Enrichment Pipeline
Sources: UEFA API (officials, lineups, time), ESPN API (stats, events, rosters)
RULES: Never invent data. Only write verified data. NULL if unavailable.
"""
import pandas as pd
import os, sys, re, json, unicodedata
import requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ESPN_BOARD   = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard'
ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary'
UEFA_BASE    = 'https://match.uefa.com/v5/matches'

# ---------------------------------------------------------------------------
# UEFA match-ID index (built once from the pre-generated json file)
# ---------------------------------------------------------------------------
_UEFA_INDEX: dict = {}  # (yyyy-mm-dd, home_intl, away_intl) -> matchId


def _load_uefa_index():
    global _UEFA_INDEX
    idx_path = os.path.join(os.path.dirname(__file__), 'uefa_match_ids.json')
    if not os.path.exists(idx_path):
        return
    with open(idx_path, encoding='utf-8') as f:
        raw = json.load(f)
    for key_str, mid in raw.items():
        parts = key_str.split('|', 2)
        if len(parts) == 3:
            _UEFA_INDEX[(parts[0], parts[1], parts[2])] = mid


_load_uefa_index()

# Team name aliases: CSV name fragment -> UEFA internationalName fragment
_ALIASES: dict[str, list[str]] = {
    'psg': ['paris'],
    'paris saint germain': ['paris'],
    'inter milan': ['inter'],
    'atletico': ['atleti', 'atletico', 'atletico madrid'],
    'atletico madrid': ['atleti', 'atletico'],
    'bayer munich': ['baviera', 'munchen', 'munich'],
    'stade brestois': ['brest'],
    'as monaco': ['monaco'],
    'sl benfica': ['benfica'],
    'borussia dortmund': ['dortmund'],
    'rb leipzig': ['leipzig'],
    'losc lille': ['lille'],
}


def _expand_aliases(name: str) -> list[str]:
    n = _norm(name)
    result = {n}
    for alias_key, alias_vals in _ALIASES.items():
        if alias_key in n or n in alias_key:
            result.update(alias_vals)
    return list(result)


# ESPN manual ID overrides (for matches where auto-discovery fails)
_ESPN_OVERRIDES: dict[str, str] = {}


def _load_espn_overrides():
    global _ESPN_OVERRIDES
    path = os.path.join(os.path.dirname(__file__), 'espn_id_overrides.json')
    if not os.path.exists(path):
        return
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    _ESPN_OVERRIDES = data.get('overrides', {})


_load_espn_overrides()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    s = s.lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    return ' '.join(s.split())


def _teams_match(a: str, b: str) -> bool:
    na, nb = _norm(a), _norm(b)
    if na == nb or na in nb or nb in na:
        return True
    stop = {'fc', 'cf', 'ac', 'sc', 'rb', 'bv', 'sl', 'de', 'as', 'us', 'ss', 'sb', 'cl'}
    wa = set(na.split()) - stop
    wb = set(nb.split()) - stop
    return bool(wa & wb)


def _date_to_api(date_str: str) -> str:
    """'DD-MM-YYYY' -> 'YYYYMMDD'"""
    try:
        p = date_str.split('-')
        return f"{p[2]}{p[1]}{p[0]}"
    except Exception:
        return ''


def _date_to_iso(date_str: str) -> str:
    """'DD-MM-YYYY' -> 'YYYY-MM-DD'"""
    try:
        p = date_str.split('-')
        return f"{p[2]}-{p[1]}-{p[0]}"
    except Exception:
        return ''


def _is_null(val) -> bool:
    if val is None:
        return True
    try:
        import numpy as np
        if isinstance(val, float) and (pd.isna(val) or pd.isnull(val)):
            return True
    except Exception:
        pass
    return str(val).strip() in ('NULL', '', 'nan', 'NaN', 'none', 'None')


def _safe_pct(v) -> str:
    try:
        return f"{round(float(v))}%"
    except Exception:
        return 'NULL'


def _sum_int(*vals) -> str:
    t, ok = 0, False
    for v in vals:
        try:
            t += int(float(v)); ok = True
        except Exception:
            pass
    return str(t) if ok else 'NULL'

# ---------------------------------------------------------------------------
# UEFA match-ID lookup
# ---------------------------------------------------------------------------

def find_uefa_id(date_csv: str, local: str, away: str) -> str | None:
    iso = _date_to_iso(date_csv)
    if not iso:
        return None
    stop = {'fc', 'cf', 'ac', 'sc', 'rb', 'as', 'sl', 'ss', 'sb', 'bv', 'de'}
    local_forms = _expand_aliases(local)
    away_forms  = _expand_aliases(away)

    def _any_match(forms, uefa_name):
        wuefa = set(uefa_name.split()) - stop
        for f in forms:
            wf = set(f.split()) - stop
            if f in uefa_name or uefa_name in f or bool(wf & wuefa):
                return True
        return False

    best, best_score = None, 0
    for (dt, ht, at), mid in _UEFA_INDEX.items():
        if dt != iso:
            continue
        nht, nat = _norm(ht), _norm(at)
        # Try normal orientation (score=2) and reversed/neutral (score=1)
        if _any_match(local_forms, nht) and _any_match(away_forms, nat):
            if 2 > best_score:
                best_score = 2
                best = mid
        elif _any_match(local_forms, nat) and _any_match(away_forms, nht):
            if 1 > best_score:
                best_score = 1
                best = mid
    return best

# ---------------------------------------------------------------------------
# UEFA API calls
# ---------------------------------------------------------------------------

def get_uefa_officials(match_id: str) -> dict:
    """Returns {arbitro_principal, arbitros_linea, hora_inicio, hora_fin}"""
    result = {}
    try:
        r = requests.get(UEFA_BASE, params={'matchId': match_id}, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return result
        data = r.json()
        m = data[0] if isinstance(data, list) and data else {}

        # Kickoff time
        ko = m.get('kickOffTime', {}).get('dateTime', '')
        if ko:
            t = ko.split('T')[1][:5]
            result['hora_inicio'] = t
            h, mn = map(int, t.split(':'))
            result['hora_fin'] = f"{(h+2)%24:02d}:{mn:02d}"

        # Referees
        refs = m.get('referees', [])
        main_ref = []
        asst_refs = []
        for ref in refs:
            role = ref.get('role', '')
            name = ref.get('person', {}).get('translations', {}).get('name', {}).get('EN', '').strip()
            if not name:
                continue
            if role == 'REFEREE':
                main_ref.append(name)
            elif role in ('ASSISTANT_REFEREE_ONE', 'ASSISTANT_REFEREE_TWO'):
                asst_refs.append(name)

        if main_ref:
            result['arbitro_principal'] = main_ref[0]
        if asst_refs:
            result['arbitros_linea'] = '; '.join(asst_refs)

    except Exception as e:
        print(f"    [UEFA] Error: {e}")
    return result


def get_uefa_lineups(match_id: str, local: str, away: str) -> dict:
    """Returns {planteles, entrenador_local, entrenador_visitante}"""
    result = {}
    try:
        r = requests.get(f"{UEFA_BASE}/{match_id}/lineups", headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return result

        data = r.json()

        def _extract_team(team_key: str, team_name: str) -> tuple[str, str]:
            """Returns (lineup_str, coach_name)"""
            team = data.get(team_key, {})
            starting = team.get('players', [])
            # filter starters
            starters = [p for p in starting if p.get('status') == 'STARTING_LINEUP']
            if not starters:
                starters = starting[:11]
            names = []
            for p in starters:
                n = p.get('player', {}).get('translations', {}).get('shortName', {}).get('EN', '')
                if not n:
                    n = p.get('player', {}).get('translations', {}).get('name', {}).get('EN', '')
                if n:
                    names.append(n)
            lineup = f"{team_name}: {'; '.join(names)}" if names else ''

            coach = team.get('coach')
            coach_name = ''
            if coach:
                coach_name = coach.get('person', {}).get('translations', {}).get('name', {}).get('EN', '')
            return lineup, coach_name

        home_lineup, home_coach = _extract_team('homeTeam', local)
        away_lineup, away_coach = _extract_team('awayTeam', away)

        parts = [p for p in [home_lineup, away_lineup] if p]
        if parts:
            result['planteles'] = ' | '.join(parts)
        if home_coach:
            result['entrenador_local'] = home_coach
        if away_coach:
            result['entrenador_visitante'] = away_coach

    except Exception as e:
        print(f"    [UEFA lineups] Error: {e}")
    return result

# ---------------------------------------------------------------------------
# ESPN discovery + extraction
# ---------------------------------------------------------------------------

def find_espn_event(date_csv: str, local: str, away: str) -> str | None:
    # 1. Check manual overrides first
    override_key = f"{local}|{away}|{date_csv}"
    if override_key in _ESPN_OVERRIDES:
        return _ESPN_OVERRIDES[override_key]
    # Also try reversed (visitante might be listed first)
    override_key_rev = f"{away}|{local}|{date_csv}"
    if override_key_rev in _ESPN_OVERRIDES:
        return _ESPN_OVERRIDES[override_key_rev]

    date_api = _date_to_api(date_csv)
    if not date_api:
        return None

    stop = {'fc', 'cf', 'ac', 'sc', 'rb', 'as', 'sl', 'ss', 'sb', 'at', 'vs', 'bv', 'de'}
    local_forms = _expand_aliases(local)
    away_forms  = _expand_aliases(away)

    def _any_word_match(forms: list[str], espn_name: str) -> bool:
        nname = _norm(espn_name)
        wname = set(nname.split()) - stop
        for f in forms:
            wf = set(f.split()) - stop
            if f in nname or nname in f or bool(wf & wname):
                return True
        return False

    try:
        r = requests.get(ESPN_BOARD, params={'dates': date_api}, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None
        for evt in r.json().get('events', []):
            comps = evt.get('competitions', [])
            if not comps:
                continue
            comp = comps[0]
            home_name, away_name = '', ''
            for c in comp.get('competitors', []):
                n = c.get('team', {}).get('displayName', '')
                if c.get('homeAway') == 'home':
                    home_name = n
                else:
                    away_name = n
            # Bidirectional match: CSV local can be ESPN home OR away
            if (_any_word_match(local_forms, home_name) and _any_word_match(away_forms, away_name)) or \
               (_any_word_match(local_forms, away_name) and _any_word_match(away_forms, home_name)):
                return str(evt.get('id'))
    except Exception:
        pass
    return None


def get_espn_data(event_id: str, local: str) -> dict:
    """Full extraction from ESPN summary. Returns flat enrichment dict."""
    result = {}
    try:
        r = requests.get(ESPN_SUMMARY, params={'event': event_id}, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return result
        data = r.json()
    except Exception:
        return result

    comp = (data.get('header', {}).get('competitions') or [{}])[0]
    competitors = comp.get('competitors', [])

    # Determine home/away orientation vs CSV local
    home_comp, away_comp = None, None
    for c in competitors:
        if c.get('homeAway') == 'home':
            home_comp = c
        else:
            away_comp = c

    # Figure out if ESPN home == CSV local
    espn_home_is_local = True
    if home_comp:
        espn_home_name = home_comp.get('team', {}).get('displayName', '')
        if not _teams_match(local, espn_home_name):
            espn_home_is_local = False

    local_comp  = home_comp if espn_home_is_local else away_comp
    visitor_comp = away_comp if espn_home_is_local else home_comp

    # --- Officials (main ref only if UEFA didn't provide) ---
    officials = data.get('gameInfo', {}).get('officials', [])
    seen, refs = set(), []
    for off in officials:
        n = off.get('displayName', '').strip()
        if n and n not in seen:
            seen.add(n)
            refs.append(n)
    if refs:
        result['_espn_main_ref'] = refs[0]

    # --- Rosters / Lineups ---
    rosters = data.get('rosters', [])
    lineup_parts = []
    coaches = {}  # team_id -> coach_name

    for roster_data in rosters:
        tid = roster_data.get('team', {}).get('id')
        tname = roster_data.get('team', {}).get('displayName', 'Team')
        entries = roster_data.get('roster', [])
        starters = [e for e in entries if e.get('starter')]
        if not starters:
            continue

        POS_ORDER = {'G': 0, 'GK': 0, 'CD': 1, 'D': 1, 'M': 2, 'F': 3, 'FW': 3}

        def _pk(e):
            p = e.get('position', {}).get('abbreviation', 'X').split('-')[0]
            return POS_ORDER.get(p, 2)

        starters_sorted = sorted(starters, key=_pk)
        names = [e.get('athlete', {}).get('displayName', '') for e in starters_sorted]
        names = [n for n in names if n]
        if names:
            lineup_parts.append((tid, tname, names))

        coach_list = roster_data.get('coaches', [])
        if coach_list:
            coaches[tid] = coach_list[0].get('displayName', '')

    # Order lineup parts: local first
    local_tid = (local_comp or {}).get('team', {}).get('id')
    visitor_tid = (visitor_comp or {}).get('team', {}).get('id')

    ordered = []
    for tid, tname, names in lineup_parts:
        ordered.append((tid, tname, names))
    ordered.sort(key=lambda x: 0 if x[0] == local_tid else 1)

    if ordered:
        result['planteles'] = ' | '.join(f"{tname}: {'; '.join(names)}" for _, tname, names in ordered)

    if local_tid and local_tid in coaches:
        result['entrenador_local'] = coaches[local_tid]
    if visitor_tid and visitor_tid in coaches:
        result['entrenador_visitante'] = coaches[visitor_tid]

    # --- Stats ---
    bs_teams = data.get('boxscore', {}).get('teams', [])
    local_stats, visitor_stats = {}, {}
    for t in bs_teams:
        tid = t.get('team', {}).get('id')
        stats = {s.get('name'): s.get('displayValue') for s in t.get('statistics', [])}
        if tid == local_tid:
            local_stats = stats
        else:
            visitor_stats = stats

    def _gs(d, k):
        return d.get(k)

    ph, pa = _gs(local_stats, 'possessionPct'), _gs(visitor_stats, 'possessionPct')
    if ph is not None:
        result['posesion_local'] = _safe_pct(ph)
    if pa is not None:
        result['posesion_visitante'] = _safe_pct(pa)

    th, ta = _gs(local_stats, 'totalShots'), _gs(visitor_stats, 'totalShots')
    if th is not None and ta is not None:
        result['tiros_totales_local'] = th
        result['tiros_totales_visitante'] = ta
        result['tiros_totales'] = _sum_int(th, ta)

    sth, sta = _gs(local_stats, 'shotsOnTarget'), _gs(visitor_stats, 'shotsOnTarget')
    if sth is not None and sta is not None:
        result['tiros_puerta_local'] = sth
        result['tiros_puerta_visitante'] = sta
        result['tiros_puerta'] = _sum_int(sth, sta)

    fh, fa = _gs(local_stats, 'foulsCommitted'), _gs(visitor_stats, 'foulsCommitted')
    if fh is not None and fa is not None:
        result['faltas_local'] = fh
        result['faltas_visitante'] = fa
        result['faltas_total'] = _sum_int(fh, fa)

    ch, ca = _gs(local_stats, 'wonCorners'), _gs(visitor_stats, 'wonCorners')
    if ch is not None and ca is not None:
        result['corners_local'] = ch
        result['corners_visitante'] = ca
        result['corners_total'] = _sum_int(ch, ca)

    # --- Events ---
    key_events = data.get('keyEvents', [])
    goals, yellows, reds, subs = [], [], [], []

    for evt in key_events:
        etype = evt.get('type', {}).get('text', '').lower()
        clock = evt.get('clock', {}).get('displayValue', '')
        participants = evt.get('participants', [])
        names = [p.get('athlete', {}).get('displayName', '?') for p in participants]

        if 'goal' in etype and names:
            goals.append(f"{names[0]} {clock}'")
        elif 'yellow' in etype and names:
            yellows.append(f"{names[0]} {clock}'")
        elif 'red' in etype and names:
            reds.append(f"{names[0]} {clock}'")
        elif 'substitution' in etype and len(names) >= 2:
            subs.append(f"{clock}' {names[0]} x {names[1]}")

    if goals:
        result['goles'] = '; '.join(goals)
    if yellows:
        result['amarillas'] = '; '.join(yellows)
    if reds:
        result['rojas'] = '; '.join(reds)
    if subs:
        result['cambios'] = '; '.join(subs)

    # Details sub-events (cards detail sometimes here)
    details = comp.get('details', [])
    yellow_details, red_details = [], []
    for d in details:
        dtype = d.get('type', {}).get('text', '').lower()
        clock = d.get('clock', {}).get('displayValue', '')
        athletes = d.get('athletesInvolved', [])
        aname = athletes[0].get('displayName', '?') if athletes else '?'
        if 'yellow' in dtype:
            yellow_details.append(f"{aname} {clock}'")
        elif 'red' in dtype:
            red_details.append(f"{aname} {clock}'")

    # Use details version only if keyEvents gave nothing
    if not yellows and yellow_details:
        result['amarillas'] = '; '.join(yellow_details)
    if not reds and red_details:
        result['rojas'] = '; '.join(red_details)

    return result


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

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

# Stats fields that should ALWAYS be retried even if partially filled
STATS_FIELDS = {
    'tiros_totales', 'tiros_totales_local', 'tiros_totales_visitante',
    'tiros_puerta', 'tiros_puerta_local', 'tiros_puerta_visitante',
    'posesion_local', 'posesion_visitante',
    'faltas_total', 'faltas_local', 'faltas_visitante',
    'corners_total', 'corners_local', 'corners_visitante',
    'amarillas', 'goles',
}


def fill_missing_data(csv_path: str, output_path: str):
    print(f"Reading: {csv_path}")
    df = pd.read_csv(csv_path, keep_default_na=False)

    for idx, row in df.iterrows():
        local     = str(row.get('local', ''))
        visitante = str(row.get('visitante', ''))
        fecha     = str(row.get('fecha', ''))

        # Only re-process if we are missing critical fields
        has_stats = not _is_null(row.get('tiros_totales'))
        has_refs = not _is_null(row.get('arbitro_principal'))
        if has_stats and has_refs:
            continue

        print(f"  [{idx}] {local} vs {visitante} ({fecha})")

        # --- UEFA: officials + time ---
        uefa_id = find_uefa_id(fecha, local, visitante)
        uefa_officials = {}
        uefa_lineups   = {}
        if uefa_id:
            print(f"    UEFA id={uefa_id}")
            uefa_officials = get_uefa_officials(uefa_id)
            if _is_null(row.get('planteles')):
                uefa_lineups = get_uefa_lineups(uefa_id, local, visitante)
        else:
            print(f"    [!] UEFA id not found")

        # --- ESPN: stats + events + rosters ---
        espn_id = find_espn_event(fecha, local, visitante)
        espn_data = {}
        if espn_id:
            print(f"    ESPN id={espn_id}")
            espn_data = get_espn_data(espn_id, local)
        else:
            print(f"    [!] ESPN event not found")

        # --- Merge: UEFA takes precedence for officials/time, ESPN for stats/events/lineups ---
        # Order: espn_data first (lower priority), then uefa for time+officials
        merged = {**espn_data}

        # UEFA officials always override ESPN (more accurate)
        for k in ('arbitro_principal', 'arbitros_linea', 'hora_inicio', 'hora_fin'):
            if k in uefa_officials:
                merged[k] = uefa_officials[k]

        # UEFA lineups override ESPN lineups (more accurate names)
        for k in ('planteles', 'entrenador_local', 'entrenador_visitante'):
            if k in uefa_lineups and uefa_lineups[k]:
                merged[k] = uefa_lineups[k]

        # Main referee fallback: if UEFA had no main ref, try ESPN
        if 'arbitro_principal' not in merged and '_espn_main_ref' in merged:
            merged['arbitro_principal'] = merged['_espn_main_ref']

        # Apply: never overwrite existing good values
        for col, val in merged.items():
            if col.startswith('_'):
                continue
            if col in df.columns and _is_null(df.at[idx, col]) and val and not _is_null(val):
                df.at[idx, col] = val

        # Final: empty strings → NULL
        for col in ENRICHABLE:
            if col in df.columns and str(df.at[idx, col]).strip() == '':
                df.at[idx, col] = 'NULL'

    # Global sweep
    for col in df.columns:
        df[col] = df[col].apply(lambda x: 'NULL' if str(x).strip() in ('', 'nan') else x)

    print(f"Saving: {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8')


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw  = os.path.join(base, 'data', 'raw',       'champions_league_2011_2025.csv')
    out  = os.path.join(base, 'data', 'processed', 'champions_league_2011_2025_completed.csv')

    # Prefer already-processed file as input (incremental enrichment)
    # This means only rows still having NULL fields will be re-fetched.
    inp = out if os.path.exists(out) else raw

    if os.path.exists(inp):
        print(f"Input: {inp}")
        fill_missing_data(inp, out)
    else:
        print(f"Error: neither processed nor raw CSV found at expected paths")