import requests
import os
import json
from .utils import HEADERS, norm_text, date_to_iso

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

    def get_officials(self, match_id):
        result = {}
        try:
            r = requests.get(UEFA_BASE, params={'matchId': match_id}, headers=HEADERS, timeout=12)
            if r.status_code != 200: return result
            data = r.json()
            m = data[0] if isinstance(data, list) and data else {}
            ko = m.get('kickOffTime', {}).get('dateTime', '')
            if ko:
                t = ko.split('T')[1][:5]
                result['hora_inicio'] = t
                h, mn = map(int, t.split(':'))
                result['hora_fin'] = f"{(h+2)%24:02d}:{mn:02d}"
            refs = m.get('referees', [])
            main_ref, asst_refs = [], []
            for ref in refs:
                role = ref.get('role', '')
                name = ref.get('person', {}).get('translations', {}).get('name', {}).get('EN', '').strip()
                if not name: continue
                if role == 'REFEREE': main_ref.append(name)
                elif role in ('ASSISTANT_REFEREE_ONE', 'ASSISTANT_REFEREE_TWO'): asst_refs.append(name)
            if main_ref: result['arbitro_principal'] = main_ref[0]
            if asst_refs: result['arbitros_linea'] = '; '.join(asst_refs)
        except Exception: pass
        return result

    def get_lineups(self, match_id, local_name, away_name):
        result = {}
        try:
            r = requests.get(f"{UEFA_BASE}/{match_id}/lineups", headers=HEADERS, timeout=12)
            if r.status_code != 200: return result
            data = r.json()
            def _extract_team(team_key, display_name):
                team = data.get(team_key, {})
                starting = team.get('players', [])
                starters = [p for p in starting if p.get('status') == 'STARTING_LINEUP']
                if not starters: starters = starting[:11]
                names = []
                for p in starters:
                    n = p.get('player', {}).get('translations', {}).get('shortName', {}).get('EN', '') or \
                        p.get('player', {}).get('translations', {}).get('name', {}).get('EN', '')
                    if n: names.append(n)
                lineup = f"{display_name}: {'; '.join(names)}" if names else ''
                coach = team.get('coach', {}).get('person', {}).get('translations', {}).get('name', {}).get('EN', '')
                return lineup, coach
            home_lineup, home_coach = _extract_team('homeTeam', local_name)
            away_lineup, away_coach = _extract_team('awayTeam', away_name)
            parts = [p for p in [home_lineup, away_lineup] if p]
            if parts: result['planteles'] = ' | '.join(parts)
            if home_coach: result['entrenador_local'] = home_coach
            if away_coach: result['entrenador_visitante'] = away_coach
        except Exception: pass
        return result
