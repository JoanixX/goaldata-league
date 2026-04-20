import requests
from bs4 import BeautifulSoup
import re
import json
from scrapers.utils import HEADERS, norm_text

class FBRefScraper:
    def __init__(self):
        self.headers = HEADERS.copy()
        self.headers.update({'Referer': 'https://fbref.com/'})
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_page(self, url, use_archive=True):
        """Attempts to fetch the page directly, falls back to Wayback Machine if blocked."""
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code == 200 and "Cloudflare" not in response.text:
                return response.text
        except Exception: pass

        if use_archive:
            archive_url = f"https://archive.org/wayback/available?url={url}"
            try:
                res = requests.get(archive_url, timeout=10).json()
                if res.get("archived_snapshots", {}).get("closest", {}).get("available"):
                    snapshot_url = res["archived_snapshots"]["closest"]["url"]
                    return requests.get(snapshot_url, timeout=15).text
            except Exception: pass
        return None

    def parse_match_data(self, html):
        if not html: return None
        soup = BeautifulSoup(html, 'html.parser')
        data = {"metadata": {}, "stats": {}, "lineups": {}}

        scorebox = soup.find('div', class_='scorebox')
        if scorebox:
            teams = scorebox.find_all('div', itemprop='performer')
            if len(teams) >= 2:
                data["metadata"]["home_team"] = teams[0].find('a').text.strip()
                data["metadata"]["away_team"] = teams[1].find('a').text.strip()
            score_divs = scorebox.find_all('div', class_='score')
            if len(score_divs) >= 2:
                data["metadata"]["score"] = f"{score_divs[0].text.strip()}-{score_divs[1].text.strip()}"
            meta_div = scorebox.find('div', class_='scorebox_meta')
            if meta_div:
                data["metadata"]["date"] = meta_div.find('span', class_='venuetime').get('data-venue-date') if meta_div.find('span', class_='venuetime') else "unknown"
                venue_link = meta_div.find('small', string=re.compile("Venue"))
                if venue_link: data["metadata"]["venue"] = venue_link.next_sibling.strip() if venue_link.next_sibling else "unknown"

        tables = soup.find_all('table', id=re.compile("stats_.*_summary"))
        if not tables:
            import bs4
            comments = soup.find_all(string=lambda text: isinstance(text, bs4.Comment))
            for comment in comments:
                if 'id="stats_' in comment and '_summary"' in comment:
                    comment_soup = BeautifulSoup(comment, 'html.parser')
                    tables.extend(comment_soup.find_all('table', id=re.compile("stats_.*_summary")))

        for table in tables:
            team_id = table.get('id').split('_')[1]
            caption = table.find('caption')
            team_label = caption.text.split('Player Stats')[0].strip() if caption else team_id
            player_stats = []
            rows = table.find('tbody').find_all('tr')
            for row in rows:
                if row.get('class') and 'spacer' in row.get('class'): continue
                p_cell = row.find('th', {'data-stat': 'player'})
                if not p_cell: continue
                
                def _get(stat): return row.find('td', {'data-stat': stat}).text.strip() if row.find('td', {'data-stat': stat}) else "0"
                stats = {
                    "player": p_cell.text.strip(),
                    "number": _get('shirt_number'), "nation": _get('nationality'), "pos": _get('position'),
                    "age": _get('age'), "min": _get('minutes'), "gls": _get('goals'), "ast": _get('assists'),
                    "sh": _get('shots'), "sot": _get('shots_on_target'), "crdy": _get('cards_yellow'),
                    "crdr": _get('cards_red'), "fls": _get('fouls'), "fld": _get('fouled'),
                    "off": _get('offsides'), "crs": _get('crosses'), "tklw": _get('tackles_won'), "int": _get('interceptions'),
                }
                player_stats.append(stats)
            data["stats"][team_label] = player_stats
        return data