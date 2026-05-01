import requests
import os
import json
from scrapers.utils import HEADERS, norm_text, date_to_iso

UEFA_BASE = 'https://match.uefa.com/v5/matches'

class UEFAScraper:
    def __init__(self, index_path=None):
        self.index = {}
        if index_path and os.path.exists(index_path):
            self._load_index(index_path)

    def _load_index(self, path):
        with open(path, encoding='utf-8') as f:
            raw = json.load(f)
        for key_str, mid in raw.items():
            parts = key_str.split('|', 2)
            if len(parts) == 3:
                self.index[(parts[0], parts[1], parts[2])] = mid

    def find_id(self, date_csv, local, away, aliases_func):
        iso = date_to_iso(date_csv)
        if not iso: return None
        stop = {'fc', 'cf', 'ac', 'sc', 'rb', 'as', 'sl', 'ss', 'sb', 'bv', 'de'}
        local_forms = aliases_func(local)
        away_forms  = aliases_func(away)

        def _any_match(forms, uefa_name):
            wuefa = set(uefa_name.split()) - stop
            for f in forms:
                wf = set(f.split()) - stop
                if f in uefa_name or uefa_name in f or bool(wf & wuefa):
                    return True
            return False

        best, best_score = None, 0
        for (dt, ht, at), mid in self.index.items():
            if dt != iso: continue
            nht, nat = norm_text(ht), norm_text(at)
            if _any_match(local_forms, nht) and _any_match(away_forms, nat):
                if 2 > best_score: best_score = 2; best = mid
            elif _any_match(local_forms, nat) and _any_match(away_forms, nht):
                if 1 > best_score: best_score = 1; best = mid
        return best

    def get_match_info(self, match_id):
        result = {}
        try:
            r = requests.get(UEFA_BASE, params={'matchId': match_id}, headers=HEADERS, timeout=12)
            if r.status_code != 200: return result
            data = r.json()
            m = data[0] if isinstance(data, list) and data else {}
            
            # Basic info for matches.csv
            result['stadium'] = m.get('stadium', {}).get('translations', {}).get('name', {}).get('EN', 'Unknown')
            result['city'] = m.get('stadium', {}).get('translations', {}).get('city', {}).get('EN', 'Unknown')
            result['country'] = m.get('stadium', {}).get('countryCode', 'Unknown')
            
            ko = m.get('kickOffTime', {}).get('dateTime', '')
            if ko:
                t = ko.split('T')[1][:5]
                result['start_time'] = t # Not in matches.csv but useful for logs
            
            refs = m.get('referees', [])
            for ref in refs:
                if ref.get('role') == 'REFEREE':
                    trans = ref.get('person', {}).get('translations', {}).get('name', {})
                    name = trans.get('ES') or trans.get('EN') or (list(trans.values())[0] if trans else 'Unknown')
                    result['referee'] = name.strip()
                    break
        except Exception: pass
        return result

    def get_player_stats(self, match_id):
        """Returns a list of player stats records for the match."""
        players_data = []
        try:
            r = requests.get(f"{UEFA_BASE}/{match_id}/lineups", headers=HEADERS, timeout=12)
            if r.status_code != 200: return []
            data = r.json()
            
            for team_key in ['homeTeam', 'awayTeam']:
                team = data.get(team_key, {})
                for p in team.get('players', []):
                    trans = p.get('player', {}).get('translations', {})
                    name = trans.get('name', {}).get('EN') or (list(trans.get('name', {}).values())[0] if trans.get('name') else 'Unknown')
                    
                    # Basic presence record
                    p_record = {
                        'player_name': name,
                        'team_name': team.get('team', {}).get('translations', {}).get('name', {}).get('EN', 'Unknown'),
                        'minutes_played': 90 if p.get('status') == 'STARTING_LINEUP' else 0, # Rough estimate
                        'yellow_cards': 0, # Lineups don't usually have stats but we can initialize
                        'red_cards': 0,
                        'goals': 0,
                        'assists': 0
                    }
                    players_data.append(p_record)
        except Exception: pass
        return players_data
