import unittest
from unittest.mock import patch, Mock
import sys
import os

# Setup paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from scrapers.uefa import UEFAScraper

class TestUEFAScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = UEFAScraper()

    @patch('requests.get')
    def test_get_match_info(self, mock_get):
        # Mock response for get_match_info
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{
            'stadium': {
                'translations': {
                    'name': {'EN': 'Santiago Bernabéu'},
                    'city': {'EN': 'Madrid'}
                },
                'countryCode': 'ES'
            },
            'kickOffTime': {'dateTime': '2024-03-06T21:00:00Z'},
            'referees': [
                {
                    'role': 'REFEREE',
                    'person': {
                        'translations': {
                            'name': {'EN': 'Davide Massa'}
                        }
                    }
                }
            ]
        }]
        mock_get.return_value = mock_response

        info = self.scraper.get_match_info('123456')
        self.assertEqual(info['stadium'], 'Santiago Bernabéu')
        self.assertEqual(info['city'], 'Madrid')
        self.assertEqual(info['referee'], 'Davide Massa')

    @patch('requests.get')
    def test_get_player_stats(self, mock_get):
        # Mock response for lineups
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'homeTeam': {
                'team': {'translations': {'name': {'EN': 'Real Madrid'}}},
                'players': [
                    {
                        'player': {'translations': {'name': {'EN': 'Vinícius Júnior'}}},
                        'status': 'STARTING_LINEUP'
                    }
                ]
            },
            'awayTeam': {
                'team': {'translations': {'name': {'EN': 'RB Leipzig'}}},
                'players': []
            }
        }
        mock_get.return_value = mock_response

        stats = self.scraper.get_player_stats('123456')
        self.assertTrue(len(stats) > 0)
        self.assertEqual(stats[0]['player_name'], 'Vinícius Júnior')
        self.assertEqual(stats[0]['minutes_played'], 90)

if __name__ == '__main__':
    unittest.main()
