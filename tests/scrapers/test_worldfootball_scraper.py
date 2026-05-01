import unittest
from unittest.mock import patch, Mock
import sys
import os

# Setup paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from scrapers.worldfootball import WorldFootballScraper

class TestWorldFootballScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = WorldFootballScraper()

    def test_parse_match_report(self):
        # Mock HTML for WorldFootball
        html = """
        <div class="box">
            <table class="std">
                <tr><td>Venue</td><td>Allianz Arena</td></tr>
                <tr><td>Referee</td><td>Clément Turpin</td></tr>
            </table>
        </div>
        <table class="std">
            <tr><th>Player</th></tr>
            <tr><td>1</td><td>Manuel Neuer</td></tr>
        </table>
        """
        data = self.scraper.parse_match_report(html)
        
        self.assertEqual(data['metadata']['stadium'], 'Allianz Arena')
        self.assertEqual(data['metadata']['referee'], 'Clément Turpin')
        self.assertIn('Manuel Neuer', data['lineups']['home'])

if __name__ == '__main__':
    unittest.main()
