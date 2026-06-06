import unittest
from unittest.mock import patch, Mock
import sys
import os

# Setup paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from scrapers.fbref import FBRefScraper

class TestFBRefScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = FBRefScraper()

    def test_parse_match_data(self):
        # Mock HTML content for FBRef
        html = """
        <div class="scorebox">
            <div itemprop="performer"><a href="/teams/1/Real-Madrid">Real Madrid</a></div>
            <div itemprop="performer"><a href="/teams/2/Leipzig">Leipzig</a></div>
            <div class="score">1</div><div class="score">0</div>
        </div>
        <table id="stats_1_summary">
            <caption>Real Madrid Player Stats</caption>
            <tbody>
                <tr>
                    <th data-stat="player">Vinícius Júnior</th>
                    <td data-stat="minutes">90</td>
                    <td data-stat="goals">1</td>
                    <td data-stat="assists">0</td>
                </tr>
            </tbody>
        </table>
        """
        data = self.scraper.parse_match_data(html)
        
        self.assertEqual(data['metadata']['home_team'], 'Real Madrid')
        self.assertIn('Real Madrid', data['player_stats'])
        self.assertEqual(data['player_stats']['Real Madrid'][0]['player_name'], 'Vinícius Júnior')
        self.assertEqual(data['player_stats']['Real Madrid'][0]['goals'], 1)

if __name__ == '__main__':
    unittest.main()
