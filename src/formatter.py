import math

def format_percentage(value):
    """
    Rounds a decimal percentage value and adds the '%' symbol.
    Example: 64.6 -> '65%'
    """
    if value is None:
        return 'NULL'
    try:
        val = float(value)
        return f"{round(val)}%"
    except ValueError:
        return 'NULL'

def format_score(home_score, away_score, status_detail=None):
    """
    Format score. Add '(P)' for Penalties or '(AET)' for extra-time if given in status_detail.
    """
    if home_score is None or away_score is None:
        return 'NULL'
    
    score_str = f"{home_score}-{away_score}"
    
    if status_detail:
        detail_upper = status_detail.upper()
        if 'PEN' in detail_upper:
            score_str += " (P)"
        elif 'AET' in detail_upper or 'A.E.T' in detail_upper or 'EXTRA' in detail_upper:
            score_str += " (AET)"
            
    return score_str

def format_referees(referees_list):
    """
    Format referees from UEFA API.
    Format: "Principal, Assistant 1; Assistant 2"
    """
    if not referees_list:
        return 'NULL'
    
    main = 'NULL'
    assistants = []
    
    for ref in referees_list:
        role = ref.get('role')
        name = ref.get('person', {}).get('translations', {}).get('name', {}).get('EN', '')
        if role == 'REFEREE':
            main = name
        elif role in ['ASSISTANT_REFEREE_ONE', 'ASSISTANT_REFEREE_TWO']:
            assistants.append(name)
            
    if main == 'NULL' and not assistants:
        return 'NULL'
        
    return f"{main}, {'; '.join(assistants)}"

def format_lineups(home_name, home_lineup, away_name, away_lineup):
    """
    Format full lineups (starters + bench) from UEFA API.
    Format: "Home: P1, P2... | Away: P1, P2..."
    """
    def get_team_string(team_name, lineup_data):
        if not lineup_data:
            return f"{team_name}: NULL"
            
        # Group by position
        pos_map = {'GOALKEEPER': [], 'DEFENDER': [], 'MIDFIELDER': [], 'FORWARD': []}
        
        # Combine starters and bench? The example seems to show starters mostly.
        # "Valencia: Guaita; D. Navarro, Ricardo Costa, Mathieu, Miguel; Mehmet Topal, Banega, Domínguez; Aritz Aduriz, Soldado, Costa"
        # This is 11 players. So starters only for the 'planteles' field.
        starters = lineup_data.get('lineup', [])
        for p in starters:
            name = p.get('player', {}).get('translations', {}).get('name', {}).get('EN', '')
            pos = p.get('player', {}).get('fieldPosition', 'FORWARD')
            if pos in pos_map:
                pos_map[pos].append(name)
            else:
                pos_map['FORWARD'].append(name)
                
        # Build sections
        sections = []
        if pos_map['GOALKEEPER']: sections.append(", ".join(pos_map['GOALKEEPER']))
        if pos_map['DEFENDER']: sections.append(", ".join(pos_map['DEFENDER']))
        if pos_map['MIDFIELDER']: sections.append(", ".join(pos_map['MIDFIELDER']))
        if pos_map['FORWARD']: sections.append(", ".join(pos_map['FORWARD']))
        
        return f"{team_name}: {'; '.join(sections)}"

    home_str = get_team_string(home_name, home_lineup)
    away_str = get_team_string(away_name, away_lineup)
    
    return f"{home_str} | {away_str}"

def format_ratings(ratings_list):
    """
    Format ratings from Flashscore.
    Format: "Player 8.9; Player 7.2; ..."
    """
    if not ratings_list:
        return 'NULL'
    # ratings_list is expected to be a list of (name, rating) tuples or similar
    return "; ".join([f"{name} {rating}" for name, rating in ratings_list])

def format_events(events_list, event_type):
    """
    Given a list of events from ESPN API details, filter by event_type
    and return formatted string.
    Types from ESPN:
     - 'Goal'
     - 'Yellow Card'
     - 'Red Card'
     - 'Substitution'
    
    Returns standard string, e.g. "Soldado 17'; Raul 64'"
    If empty, returns "NULL"
    """
    if not events_list:
        return 'NULL'
        
    filtered = []
    for evt in events_list:
        # Check event type
        t_id = evt.get('type', {}).get('text', '')
        if event_type == 'Yellow' and t_id == 'Yellow Card':
            filtered.append(evt)
        elif event_type == 'Red' and t_id == 'Red Card':
            filtered.append(evt)
        elif event_type == 'Goal' and t_id == 'Goal':
            filtered.append(evt)
        elif event_type == 'Sub' and t_id == 'Substitution':
            filtered.append(evt)
            
    if not filtered:
        return 'NULL'
        
    formatted = []
    for evt in filtered:
        clock = evt.get('clock', {}).get('displayValue', '').replace("'", "")
        # Get athletes
        athletes = evt.get('athletesInvolved', [])
        
        if event_type == 'Sub' and len(athletes) >= 2:
            # Substitution usually list 2 players: sub_in, sub_out
            sub_in = athletes[0].get('shortName', athletes[0].get('displayName', ''))
            sub_out = athletes[1].get('shortName', athletes[1].get('displayName', ''))
            formatted.append(f"{clock}' {sub_in} x {sub_out}")
            
        elif athletes:
            # Cards and Goals
            player = athletes[0].get('shortName', athletes[0].get('displayName', ''))
            # Format: Name clock'
            formatted.append(f"{player} {clock}'")
            
    return "; ".join(formatted) if formatted else "NULL"