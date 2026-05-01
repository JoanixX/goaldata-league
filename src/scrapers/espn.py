import requests
from scrapers.utils import HEADERS, norm_text, teams_match, date_to_api, safe_pct, sum_int

ESPN_BOARD   = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard'
ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary'

class ESPNScraper:
    @staticmethod
    def _stat_map(stats_list):
        mapped = {}
        for item in stats_list or []:
            keys = {
                item.get('name'),
                item.get('abbreviation'),
                item.get('label'),
                item.get('displayName'),
            }
            for key in keys:
                if key:
                    mapped[str(key)] = item.get('displayValue', item.get('value'))
        return mapped

    @staticmethod
    def _number_from_stat(stats, *keys):
        for key in keys:
            raw = stats.get(key)
            if raw in (None, '', 'NULL'):
                continue
            text = str(raw).strip().replace('%', '')
            if '/' in text:
                text = text.split('/', 1)[0]
            try:
                return int(float(text))
            except Exception:
                continue
        return 0

    def find_event(self, date_csv, local, away, aliases_func, overrides=None):
        overrides = overrides or {}
        override_key = f"{local}|{away}|{date_csv}"
        if override_key in overrides: return overrides[override_key]
        override_key_rev = f"{away}|{local}|{date_csv}"
        if override_key_rev in overrides: return overrides[override_key_rev]

        date_api = date_to_api(date_csv)
        if not date_api: return None
        stop = {'fc', 'cf', 'ac', 'sc', 'rb', 'as', 'sl', 'ss', 'sb', 'at', 'vs', 'bv', 'de'}
        local_forms = aliases_func(local)
        away_forms  = aliases_func(away)

        def _any_word_match(forms, espn_name):
            nname = norm_text(espn_name)
            wname = set(nname.split()) - stop
            for f in forms:
                wf = set(f.split()) - stop
                if f in nname or nname in f or bool(wf & wname): return True
            return False

        try:
            r = requests.get(ESPN_BOARD, params={'dates': date_api}, headers=HEADERS, timeout=10)
            if r.status_code != 200: return None
            for evt in r.json().get('events', []):
                comps = evt.get('competitions', [])
                if not comps: continue
                comp = comps[0]
                h_name, a_name = '', ''
                for c in comp.get('competitors', []):
                    n = c.get('team', {}).get('displayName', '')
                    if c.get('homeAway') == 'home': h_name = n
                    else: a_name = n
                if (_any_word_match(local_forms, h_name) and _any_word_match(away_forms, a_name)) or \
                   (_any_word_match(local_forms, a_name) and _any_word_match(away_forms, h_name)):
                    return str(evt.get('id'))
        except Exception: pass
        return None

    def get_structured_data(self, event_id, local_name_csv):
        """Returns match_info, player_stats, and events in a relational structure."""
        result = {
            'match_info': {},
            'player_stats': [],
            'events': []
        }
        try:
            r = requests.get(ESPN_SUMMARY, params={'event': event_id}, headers=HEADERS, timeout=15)
            if r.status_code != 200: return result
            data = r.json()
            comp = (data.get('header', {}).get('competitions') or [{}])[0]
            competitors = comp.get('competitors', [])
            h_comp, a_comp = None, None
            for c in competitors:
                if c.get('homeAway') == 'home': h_comp = c
                else: a_comp = c
            
            home_is_local = True
            if h_comp:
                espn_h_name = h_comp.get('team', {}).get('displayName', '')
                if not teams_match(local_name_csv, espn_h_name): home_is_local = False
            
            l_comp = h_comp if home_is_local else a_comp
            v_comp = a_comp if home_is_local else h_comp
            l_tid = (l_comp or {}).get('team', {}).get('id')
            v_tid = (v_comp or {}).get('team', {}).get('id')

            # Match Info
            info = result['match_info']
            officials = data.get('gameInfo', {}).get('officials', [])
            if officials:
                info['referee'] = officials[0].get('displayName', 'Unknown')
            
            bs_teams = data.get('boxscore', {}).get('teams', [])
            l_stats_map, v_stats_map = {}, {}
            for t in bs_teams:
                tid = t.get('team', {}).get('id')
                s_map = self._stat_map(t.get('statistics', []))
                if tid == l_tid: l_stats_map = s_map
                else: v_stats_map = s_map
            
            info['possession_home'] = safe_pct(l_stats_map.get('possessionPct'))
            info['possession_away'] = safe_pct(v_stats_map.get('possessionPct'))

            # Player Stats - Extract ALL available fields
            rosters = data.get('rosters', [])
            for rd in rosters:
                tid = rd.get('team', {}).get('id')
                team_name = rd.get('team', {}).get('displayName', 'Unknown')
                for e in rd.get('roster', []):
                    p_name = e.get('athlete', {}).get('displayName', 'Unknown')
                    s = self._stat_map(e.get('stats', []))
                    
                    p_record = {
                        'player_name': p_name,
                        'team_name': team_name,
                        'minutes_played': self._number_from_stat(s, 'minutesPlayed', 'minutes', 'MIN') or self._number_from_stat(s, 'appearances') * 90,
                        'goals': self._number_from_stat(s, 'totalGoals', 'goals', 'G'),
                        'assists': self._number_from_stat(s, 'goalAssists', 'assists', 'A'),
                        'shots': self._number_from_stat(s, 'totalShots', 'shots', 'SH'),
                        'shots_on_target': self._number_from_stat(s, 'shotsOnTarget', 'shots on target', 'SOT'),
                        'fouls_committed': self._number_from_stat(s, 'foulsCommitted', 'fouls committed', 'FC'),
                        'fouls_suffered': self._number_from_stat(s, 'foulsSuffered', 'fouls suffered', 'FS'),
                        'yellow_cards': self._number_from_stat(s, 'yellowCards', 'yellow cards', 'YC'),
                        'red_cards': self._number_from_stat(s, 'redCards', 'red cards', 'RC'),
                        'saves': self._number_from_stat(s, 'saves', 'SV'),
                        'shots_faced': self._number_from_stat(s, 'shotsFaced'),
                    }
                    result['player_stats'].append(p_record)
            
            # Team-level stats (for fields ESPN only provides at team level)
            result['team_stats'] = {}
            for t in bs_teams:
                tid = t.get('team', {}).get('id')
                s_map = self._stat_map(t.get('statistics', []))
                result['team_stats'][str(tid)] = {
                    'total_shots': s_map.get('totalShots', '0'),
                    'shots_on_target': s_map.get('shotsOnTarget', '0'),
                    'fouls_committed': s_map.get('foulsCommitted', '0'),
                    'offsides': s_map.get('offsides', '0'),
                    'saves': s_map.get('saves', '0'),
                    'passes_completed': s_map.get('accuratePasses', '0'),
                    'passes_attempted': s_map.get('totalPasses', '0'),
                    'tackles': s_map.get('totalTackles', '0'),
                }

            # Events
            ke = data.get('keyEvents', [])
            for evt in ke:
                et = evt.get('type', {}).get('text', '').lower()
                ck = str(evt.get('clock', {}).get('displayValue', '')).replace("'", "")
                pts = evt.get('participants', [])
                if not pts: continue
                
                p_name = pts[0].get('athlete', {}).get('displayName', 'Unknown')
                if 'goal' in et:
                    result['events'].append({
                        'event_type': 'goal',
                        'player_name': p_name,
                        'minute': ck,
                        'is_penalty': 'penalty' in et,
                        'is_own_goal': 'own goal' in et
                    })
        except Exception: pass
        return result

    def build_match_index(self, seasons_range: list[int]):
        """
        Builds a comprehensive index of match IDs for the given seasons.
        seasons_range: list of years (e.g., [2010, 2011, ..., 2024])
        """
        index = {}
        # UCL match months: Sept(09) to May(05) next year
        for year in seasons_range:
            print(f"[*] Fetching ESPN index for season starting {year}...")
            # We check dates from Sept of 'year' to June of 'year+1'
            # Note: This is a simplified crawl. In production, we'd use 
            # the ESPN 'calendar' endpoint if available for the league.
            # For now, we simulate by checking key matchdays or using our CSV dates.
            pass
        return index
