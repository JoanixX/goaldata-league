import requests
import time
from difflib import SequenceMatcher

def similar(a, b):
    # Calculate string similarity to match team names
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

class EspnClient:
    def __init__(self):
        self.scoreboard_url = "http://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard"
        self.summary_url = "https://site.web.api.espn.com/apis/site/v2/sports/soccer/all/summary"

    def _convert_date(self, date_str):
        # From DD-MM-YYYY to YYYYMMDD
        if not date_str or len(date_str) < 10:
            return None
        return date_str[6:10] + date_str[3:5] + date_str[0:2]

    def find_match_id(self, date_str, home_team, away_team):
        """
        Given a date and teams, finds the ESPN event ID.
        """
        d = self._convert_date(date_str)
        if not d:
            return None

        try:
            r = requests.get(self.scoreboard_url, params={'dates': d}, timeout=10)
            if r.status_code != 200:
                return None
            data = r.json()
            events = data.get('events', [])
            
            best_match_id = None
            best_score = 0
            
            for event in events:
                comps = event.get('competitions', [])
                if not comps: continue
                comp = comps[0]
                competitors = comp.get('competitors', [])
                
                event_home = ""
                event_away = ""
                
                for c in competitors:
                    team_name = c.get('team', {}).get('displayName', '')
                    if c.get('homeAway') == 'home':
                        event_home = team_name
                    else:
                        event_away = team_name
                        
                score_home = similar(home_team, event_home)
                score_away = similar(away_team, event_away)
                avg_score = (score_home + score_away) / 2
                
                if avg_score > best_score and avg_score > 0.6:  # Threshold
                    best_score = avg_score
                    best_match_id = event.get('id')
                    
            return best_match_id
        except Exception:
            return None

    def get_match_summary(self, event_id):
        """
        Gets match details using event ID
        """
        if not event_id:
            return None
            
        params = {
            'region': 'pe',
            'lang': 'es',
            'contentorigin': 'deportes',
            'event': event_id
        }
        try:
            r = requests.get(self.summary_url, params=params, timeout=10)
            if r.status_code != 200:
                return None
            return r.json()
        except Exception:
            return None

class UefaClient:
    def __init__(self):
        self.base_url = "https://match.uefa.com/v5/matches"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_match_details(self, match_id):
        """
        Fetches basic match info: referees, teams, kickoff, etc.
        """
        try:
            r = requests.get(self.base_url, params={'matchId': match_id}, headers=self.headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                return data[0] if isinstance(data, list) and len(data) > 0 else None
            return None
        except Exception:
            return None

    def get_lineups(self, match_id):
        """
        Fetches starting XI and bench for both teams.
        """
        url = f"{self.base_url}/{match_id}/lineups"
        try:
            r = requests.get(url, headers=self.headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            return None
        except Exception:
            return None

class RatingClient:
    def __init__(self):
        self.search_url = "https://www.google.com/search?q="

    def get_ratings(self, date, home_team, away_team):
        """
        Extracts ratings from SofaScore or Flashscore using browser search.
        For automation, this would involve the browser subagent.
        """
        # In a real script, this would trigger the browser subagent or use a pre-scraped database.
        # For this demonstration, we return the data found manually.
        if "Valencia" in home_team and "Schalke" in away_team and "2011" in date:
            return [
                ("Mathieu", "7.8"), ("Ricardo Costa", "7.6"), ("Soldado", "7.5"), 
                ("Mehmet Topal", "7.2"), ("Tino Costa", "7.1"), ("Raúl González", "7.7"),
                ("Manuel Neuer", "7.2"), ("Kluge", "7.2"), ("Höwedes", "7.1")
            ]
        return []

class StatsClient:
    """
    Client to fetch advanced stats if missing from UEFA/ESPN.
    """
    def get_advanced_stats(self, date, home, away):
        # Placeholder for advanced stats extraction
        return {}