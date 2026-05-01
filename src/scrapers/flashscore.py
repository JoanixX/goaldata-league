import requests
import re
from scrapers.utils import HEADERS, safe_pct, sum_int

class FlashscoreScraper:
    """
    Scraper for Flashscore using their internal feed API.
    Note: Flashscore often changes their endpoints, so this is a robust implementation 
    that targets the 'd_su' (summary) and 'd_st' (statistics) feeds.
    """
    def __init__(self):
        self.headers = HEADERS.copy()
        self.headers.update({
            'X-Fscore-Proxy': 'true',
            'Referer': 'https://www.flashscore.com/'
        })
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_feed(self, match_id, feed_type='su'):
        """
        feed_type: 'su' (summary), 'st' (statistics), 'li' (lineups)
        """
        # Base URL for Flashscore feeds (this can vary by region, e.g., flashscore.pe)
        url = f"https://d.flashscore.pe/x/feed/df_{feed_type}_1_{match_id}"
        try:
            response = self.session.get(url, timeout=10, headers={'x-fsign': 'SW9D1eZo'})
            if response.status_code == 200:
                return response.text
        except Exception: pass
        return None

    def parse_stats(self, feed_text):
        if not feed_text: return {}
        # Flashscore feed is a custom text format: KEY~VALUE¬
        stats = {}
        # Example: ST~Posesión de balón¬S1~56%¬S2~44%¬
        sections = feed_text.split('¬')
        current_stat = None
        for section in sections:
            if section.startswith('ST~'):
                current_stat = section.replace('ST~', '')
            elif section.startswith('S1~') and current_stat:
                val = section.replace('S1~', '')
                stats[f"{current_stat}_local"] = val
            elif section.startswith('S2~') and current_stat:
                val = section.replace('S2~', '')
                stats[f"{current_stat}_visitor"] = val
        return stats

    def get_match_data(self, match_id):
        data = {"stats": {}, "metadata": {}}
        
        # 1. Get Stats
        stats_feed = self.fetch_feed(match_id, 'st')
        if stats_feed:
            raw_stats = self.parse_stats(stats_feed)
            # Map Flashscore names to our schema
            mapping = {
                "Posesión de balón": "posesion",
                "Remates": "tiros_totales",
                "Remates a puerta": "tiros_puerta",
                "Faltas": "faltas",
                "Saques de esquina": "corners"
            }
            for fs_name, our_name in mapping.items():
                local = raw_stats.get(f"{fs_name}_local")
                visitor = raw_stats.get(f"{fs_name}_visitor")
                if local and visitor:
                    if "posesion" in our_name:
                        data["stats"]["possession_home"] = local
                        data["stats"]["possession_away"] = visitor
                    else:
                        data["stats"][f"{our_name}_home"] = local
                        data["stats"][f"{our_name}_away"] = visitor
        
        return data
