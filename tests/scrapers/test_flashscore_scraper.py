import unittest
from unittest.mock import patch, Mock
import sys
import os

# Setup paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from scrapers.flashscore import FlashscoreScraper

class TestFlashscoreScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = FlashscoreScraper()

    def test_parse_stats(self):
        # Mock Flashscore feed format
        feed = "ST~Posesión de balón¬S1~56%¬S2~44%¬ST~Remates¬S1~12¬S2~8¬"
        stats = self.scraper.parse_stats(feed)
        
        self.assertEqual(stats['Posesión de balón_local'], '56%')
        self.assertEqual(stats['Remates_visitor'], '8')

    @patch('scrapers.flashscore.FlashscoreScraper.fetch_feed')
    def test_get_match_data(self, mock_fetch):
        # Mock feed response
        mock_fetch.return_value = "ST~Posesión de balón¬S1~56%¬S2~44%¬"
        
        data = self.scraper.get_match_data('12345')
        self.assertEqual(data['stats']['possession_home'], '56%')
        self.assertEqual(data['stats']['possession_away'], '44%')

if __name__ == '__main__':
    unittest.main()
