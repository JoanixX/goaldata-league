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