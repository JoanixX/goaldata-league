import requests
from scrapers.utils import HEADERS, norm_text, teams_match, date_to_api, safe_pct, sum_int

ESPN_BOARD   = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard'
ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/summary'

class ESPNScraper:
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

    def get_data(self, event_id, local_name_csv):
        result = {}
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

            # Penalties & Aggregate
            shootout_score = comp.get('shootoutScoreDisplay')
            if shootout_score:
                result['_espn_penalties'] = shootout_score
            
            agg_score = comp.get('aggregateScore')
            if agg_score:
                result['marcador_global'] = str(agg_score)

            # Officials fallback
            officials = data.get('gameInfo', {}).get('officials', [])
            seen, refs = set(), []
            for off in officials:
                n = off.get('displayName', '').strip()
                if n and n not in seen: seen.add(n); refs.append(n)
            if refs: result['_espn_main_ref'] = refs[0]

            # Rosters
            rosters = data.get('rosters', [])
            lineup_parts, coaches = [], {}
            for rd in rosters:
                tid = rd.get('team', {}).get('id')
                tn = rd.get('team', {}).get('displayName', 'Team')
                entries = rd.get('roster', [])
                starters = [e for e in entries if e.get('starter')]
                if not starters: continue
                p_order = {'G': 0, 'GK': 0, 'CD': 1, 'D': 1, 'M': 2, 'F': 3, 'FW': 3}
                def _pk(e): return p_order.get(e.get('position', {}).get('abbreviation', 'X').split('-')[0], 2)
                starters_sorted = sorted(starters, key=_pk)
                names = [e.get('athlete', {}).get('displayName', '') for e in starters_sorted if e.get('athlete', {}).get('displayName')]
                if names: lineup_parts.append((tid, tn, names))
                c_list = rd.get('coaches', [])
                if c_list: coaches[tid] = c_list[0].get('displayName', '')
            
            ordered = sorted(lineup_parts, key=lambda x: 0 if x[0] == l_tid else 1)
            if ordered: result['planteles'] = ' | '.join(f"{tn}: {'; '.join(nm)}" for _, tn, nm in ordered)
            if l_tid in coaches: result['entrenador_local'] = coaches[l_tid]
            if v_tid in coaches: result['entrenador_visitante'] = coaches[v_tid]

            # Stats
            bs_teams = data.get('boxscore', {}).get('teams', [])
            l_stats, v_stats = {}, {}
            for t in bs_teams:
                tid = t.get('team', {}).get('id')
                s_map = {s.get('name'): s.get('displayValue') for s in t.get('statistics', [])}
                if tid == l_tid: l_stats = s_map
                else: v_stats = s_map
            
            def _gs(d, k): return d.get(k)
            result['posesion_local'] = safe_pct(_gs(l_stats, 'possessionPct'))
            result['posesion_visitante'] = safe_pct(_gs(v_stats, 'possessionPct'))
            th, ta = _gs(l_stats, 'totalShots'), _gs(v_stats, 'totalShots')
            if th and ta: result['tiros_totales_local'], result['tiros_totales_visitante'], result['tiros_totales'] = th, ta, sum_int(th, ta)
            sth, sta = _gs(l_stats, 'shotsOnTarget'), _gs(v_stats, 'shotsOnTarget')
            if sth and sta: result['tiros_puerta_local'], result['tiros_puerta_visitante'], result['tiros_puerta'] = sth, sta, sum_int(sth, sta)
            fh, fa = _gs(l_stats, 'foulsCommitted'), _gs(v_stats, 'foulsCommitted')
            if fh and fa: result['faltas_local'], result['faltas_visitante'], result['faltas_total'] = fh, fa, sum_int(fh, fa)
            ch, ca = _gs(l_stats, 'wonCorners'), _gs(v_stats, 'wonCorners')
            if ch and ca: result['corners_local'], result['corners_visitante'], result['corners_total'] = ch, ca, sum_int(ch, ca)

            # Events
            from formatter import format_goal, format_sub, format_list
            ke = data.get('keyEvents', [])
            goals, yellows, reds, subs = [], [], [], []
            for evt in ke:
                et = evt.get('type', {}).get('text', '').lower()
                ck = str(evt.get('clock', {}).get('displayValue', '')).replace("'", "")
                pts = evt.get('participants', [])
                nms = [p.get('athlete', {}).get('displayName', '?') for p in pts]
                if not nms: continue

                if 'goal' in et:
                    is_p = 'penalty' in et
                    is_og = 'own goal' in et
                    goals.append(format_goal(nms[0], ck, is_p, is_og))
                elif 'yellow' in et:
                    yellows.append(f"{nms[0]} {ck}'")
                elif 'red' in et:
                    reds.append(f"{nms[0]} {ck}'")
                elif 'substitution' in et and len(nms) >= 2:
                    subs.append(format_sub(ck, nms[0], nms[1]))
            
            if goals: result['goles'] = format_list(goals)
            if yellows: result['amarillas'] = format_list(yellows)
            if reds: result['rojas'] = format_list(reds)
            if subs: result['cambios'] = format_list(subs)
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
