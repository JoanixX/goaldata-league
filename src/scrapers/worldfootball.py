import requests
from bs4 import BeautifulSoup
import re
from .utils import HEADERS, norm_text

class WorldFootballScraper:
    def __init__(self):
        self.headers = HEADERS.copy()
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_page(self, url):
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                return response.text
        except Exception: pass
        return None

    def parse_match_report(self, html):
        if not html: return None
        soup = BeautifulSoup(html, 'html.parser')
        data = {"metadata": {}, "lineups": {}, "stats": {}}

        # 1. Metadata
        box = soup.find('div', class_='box')
        if box:
            # Venue, Referee, Attendance are usually in a table or list
            table = box.find('table', class_='std')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    text = row.text.strip()
                    if "Venue" in text: data["metadata"]["venue"] = row.find_all('td')[-1].text.strip()
                    if "Referee" in text: data["metadata"]["referee"] = row.find_all('td')[-1].text.strip()
                    if "Attendance" in text: data["metadata"]["attendance"] = row.find_all('td')[-1].text.strip()

        # 2. Lineups (Targeting the /lineup/ subpage if provided)
        # On Worldfootball, the /lineup/ page has clear tables for both teams
        tables = soup.find_all('table', class_='std')
        team_index = 0
        for table in tables:
            # Check if it's a lineup table (usually has 'Player' or similar)
            header = table.find('tr')
            if header and "Player" in header.text:
                team_name = "home" if team_index == 0 else "away"
                players = []
                rows = table.find_all('tr')[1:] # Skip header
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        name = cols[1].text.strip()
                        if name: players.append(name)
                data["lineups"][team_name] = players
                team_index += 1

        return data
