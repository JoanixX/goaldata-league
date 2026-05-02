import unittest
from unittest.mock import patch, Mock
import sys
import os

# Setup paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from scrapers.espn import ESPNScraper

class TestESPNScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = ESPNScraper()

    @patch('requests.get')
    def test_get_structured_data(self, mock_get):
        # Mock response for summary
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'header': {
                'competitions': [{
                    'competitors': [
                        {'homeAway': 'home', 'team': {'displayName': 'Real Madrid', 'id': '1'}},
                        {'homeAway': 'away', 'team': {'displayName': 'Leipzig', 'id': '2'}}
                    ]
                }]
            },
            'gameInfo': {'officials': [{'displayName': 'Davide Massa'}]},
            'boxscore': {
                'teams': [
                    {'team': {'id': '1'}, 'statistics': [{'name': 'possessionPct', 'displayValue': '55'}]},
                    {'team': {'id': '2'}, 'statistics': [{'name': 'possessionPct', 'displayValue': '45'}]}
                ]
            },
            'rosters': [
                {
                    'team': {'id': '1', 'displayName': 'Real Madrid'},
                    'roster': [{'athlete': {'displayName': 'Vinicius Jr'}, 'stats': [{'name': 'minutes', 'displayValue': '90'}]}]
                }
            ],
            'keyEvents': [
                {
                    'type': {'text': 'Goal'},
                    'clock': {'displayValue': "65'"},
                    'participants': [{'athlete': {'displayName': 'Vinicius Jr'}}]
                }
            ]
        }
        mock_get.return_value = mock_response

        data = self.scraper.get_structured_data('123456', 'Real Madrid')
        
        # Check match_info
        self.assertEqual(data['match_info']['referee'], 'Davide Massa')
        self.assertEqual(data['match_info']['possession_home'], '55%')
        
        # Check player_stats
        self.assertEqual(len(data['player_stats']), 1)
        self.assertEqual(data['player_stats'][0]['player_name'], 'Vinicius Jr')
        self.assertEqual(data['player_stats'][0]['minutes_played'], 90)
        
        # Check events
        self.assertEqual(len(data['events']), 1)
        self.assertEqual(data['events'][0]['player_name'], 'Vinicius Jr')
        self.assertEqual(data['events'][0]['minute'], '65')

if __name__ == '__main__':
    unittest.main()
