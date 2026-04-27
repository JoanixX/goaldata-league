import re

def norm_val(val):
    """Basic normalization for comparison."""
    if val is None: return "NULL"
    s = str(val).strip()
    if not s or s.upper() == "NULL": return "NULL"
    return s

def format_possession(val):
    if not val or str(val).upper() == "NULL": return "NULL"
    s = str(val).replace("%", "").strip()
    try:
        return f"{int(float(s))}%"
    except:
        return "NULL"

def format_score(home, away, penalties=None):
    if home is None or away is None: return "NULL"
    base = f"{home}-{away}"
    if penalties:
        return f"{base} P({penalties})"
    return base

def format_list(items, sep="; "):
    if not items: return "NULL"
    # Filter empty and join
    clean = [str(i).strip() for i in items if i and str(i).strip().upper() != "NULL"]
    if not clean: return "NULL"
    return sep.join(clean)

def format_lineup(team_name, players):
    p_str = format_list(players, sep="; ")
    return f"{team_name}: {p_str}"

def format_full_lineups(home_name, home_players, away_name, away_players):
    h = format_lineup(home_name, home_players)
    a = format_lineup(away_name, away_players)
    return f"{h} | {a}"

def format_goal(player, minute, is_penalty=False, is_own_goal=False):
    m = str(minute).replace("'", "").strip()
    suffix = "'"
    if is_penalty: suffix = "' (P)"
    elif is_own_goal: suffix = "' (OG)"
    return f"{player} {m}{suffix}"

def format_sub(minute, p_in, p_out):
    m = str(minute).replace("'", "").strip()
    return f"{m}' {p_in} x {p_out}"

def soft_norm(s):
    """Deep normalization to ignore accents, minor spelling, and special chars."""
    if not s or s.upper() == "NULL": return ""
    import unicodedata
    # Remove accents
    s = "".join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    # Remove non-alpha
    s = re.sub(r'[^a-zA-Z0-9]', '', s)
    return s.lower()

def are_equivalent(v1, v2, field=None):
    n1, n2 = norm_val(v1), norm_val(v2)
    if n1 == n2: return True
    if n1 == "NULL" or n2 == "NULL": return False

    # List-based fields (referees, goals, subs, players)
    if field in ('arbitros_linea', 'goles', 'amarillas', 'rojas', 'cambios'):
        s1 = set(soft_norm(x) for x in n1.split(';'))
        s2 = set(soft_norm(x) for x in n2.split(';'))
        if s1 == s2: return True
        return False

    if field == 'planteles':
        parts1 = [p.strip() for p in n1.split('|')]
        parts2 = [p.strip() for p in n2.split('|')]
        if len(parts1) != len(parts2): return False
        
        # We need to match teams even if swapped
        def _get_team_map(parts):
            m = {}
            for p in parts:
                if ':' not in p: continue
                t, l = p.split(':', 1)
                m[soft_norm(t)] = set(soft_norm(x) for x in l.split(';'))
            return m

        m1, m2 = _get_team_map(parts1), _get_team_map(parts2)
        if len(m1) != len(m2): return False
        for t_norm, players_set in m1.items():
            if t_norm not in m2 or m2[t_norm] != players_set:
                return False
        return True

    if field and field.startswith('posesion'):
        return n1.replace('%','') == n2.replace('%','')
    
    if field in ('arbitro_principal', 'entrenador_local', 'entrenador_visitante'):
        return soft_norm(n1) == soft_norm(n2)

    return False