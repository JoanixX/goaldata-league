import re
import hashlib
import unicodedata
import pandas as pd

# --- UNIFICATION GLOSSARY ---
METRIC_MAPPING = {
    'PasAss': 'assists',
    'asistencias': 'assists',
    'Goals': 'goals',
    'goles': 'goals',
    'MP': 'matches_played',
    'match_played': 'matches_played',
    'Min': 'minutes_played',
    'minutos': 'minutes_played',
    'conceded': 'goals_conceded',
    'saved': 'saves',
    'CrdY': 'yellow_cards',
    'amarillas': 'yellow_cards',
    'CrdR': 'red_cards',
    'rojas': 'red_cards'
}

def unify_metric_name(name):
    """Normalize metric names according to the glossary."""
    return METRIC_MAPPING.get(name, name.lower())

# --- IDENTIFICATORS ---
def generate_player_id(player_name, team_id=None):
    """
    Generates a deterministic ID based on the player's canonical name ONLY.
    team_id is accepted for backward compatibility but NOT used in the hash.
    The player-team relationship is stored in player_match_stats.team_id.
    """
    if not player_name:
        return "NULL"
    clean_n = canonical_name(str(player_name))
    return hashlib.md5(clean_n.encode('utf-8')).hexdigest()[:12]

# --- NORMALIZATION OF VALUES AND UNITS ---
def norm_val(val):
    """Basic normalization for comparison and storage."""
    if val is None: return "NULL"
    s = str(val).strip()
    if not s or s.upper() == "NULL" or s.lower() == "nan": return "NULL"
    return s

def norm_unit(val, unit_type):
    """
    Normalizes units according to project rules:
    - accuracy: convert percentage or ratio to decimal (0-1)
    - distance: ensure km
    - height: cm
    - weight: kg
    """
    if val is None or str(val).upper() == "NULL": return "NULL"
    
    try:
        f_val = float(str(val).replace('%', '').strip())
        if unit_type == 'accuracy':
            # If > 1, assume it was a percentage (e.g. 85.5)
            return round(f_val / 100.0, 4) if f_val > 1 else round(f_val, 4)
        elif unit_type == 'distance':
            # Logic for distance (assume input is km)
            return round(f_val, 2)
        return f_val
    except:
        return "NULL"

def format_possession(val):
    """Converts possession string (e.g. '65%') to decimal (0.65)."""
    res = norm_unit(val, 'accuracy')
    return res if res != "NULL" else 0.0

# --- DERIVED METRICS ---
def calculate_accuracy(numerator, denominator):
    """Calculates accuracy as a decimal (0-1). Recalculates if necessary."""
    try:
        n = float(numerator)
        d = float(denominator)
        if d == 0: return 0.0
        return round(n / d, 4)
    except:
        return 0.0

# --- TEXT AND COMPARISON ---
def soft_norm(s):
    """Deep normalization to ignore accents, minor spelling, and special chars."""
    if not s or str(s).upper() == "NULL": return ""
    s = str(s)
    # Remove accents
    s = "".join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    # Remove non-alpha (keep spaces)
    s = re.sub(r'[^a-zA-Z0-9 ]', '', s)
    return ' '.join(s.lower().split())

def canonical_name(name):
    """
    Returns a canonical version of a player name: 'last_name first_initial'.
    This allows matching variations like 'C. Ronaldo' and 'Cristiano Ronaldo'.
    """
    s = soft_norm(name)
    parts = s.split()
    if len(parts) >= 2:
        # Canonical form: Last Name + First Initial
        return f"{parts[-1]} {parts[0][0]}"
    return s

# --- HELPER FORMATTERS FOR SCRAPERS ---
def format_list(items, sep="; "):
    if not items: return "NULL"
    clean = [str(i).strip() for i in items if i and str(i).strip().upper() != "NULL"]
    if not clean: return "NULL"
    return sep.join(clean)

def format_goal(player, minute, is_penalty=False, is_own_goal=False):
    m = str(minute).replace("'", "").strip()
    suffix = "'"
    if is_penalty: suffix = "' (P)"
    elif is_own_goal: suffix = "' (OG)"
    return f"{player} {m}{suffix}"

def format_sub(minute, p_in, p_out):
    m = str(minute).replace("'", "").strip()
    return f"{m}' {p_in} x {p_out}"

def are_equivalent(v1, v2, field=None):
    """
    Checks if two values are equivalent based on their field type.
    Handles decimal comparison for accuracy and possession, and canonical matching for names.
    """
    n1, n2 = norm_val(v1), norm_val(v2)
    if n1 == n2: return True
    if n1 == "NULL" or n2 == "NULL": return False

    # Numerical/Unit fields (normalize both to decimal before comparing)
    if field and ('possession' in field or 'accuracy' in field):
        try:
            u1 = norm_unit(n1, 'accuracy')
            u2 = norm_unit(n2, 'accuracy')
            return abs(float(u1) - float(u2)) < 0.001
        except:
            pass

    # Names (use canonical matching for players)
    if field in ('player_name',):
        return canonical_name(n1) == canonical_name(n2)

    return soft_norm(n1) == soft_norm(n2)