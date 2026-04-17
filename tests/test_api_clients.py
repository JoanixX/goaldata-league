import unittest
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from api_clients import EspnClient, similar

class TestApiClients(unittest.TestCase):

    def setUp(self):
        self.client = EspnClient()

    def test_similar(self):
        self.assertTrue(similar("Real Madrid", "Real Madrid CF") > 0.7)
        self.assertTrue(similar("Schalke 04", "Schalke") > 0.7)

    def test_convert_date(self):
        self.assertEqual(self.client._convert_date("15-02-2011"), "20110215")
        self.assertEqual(self.client._convert_date("01-12-2023"), "20231201")
        self.assertIsNone(self.client._convert_date(None))

    @patch('api_clients.requests.get')
    def test_find_match_id(self, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "events": [
                {
                    "id": "123456",
                    "competitions": [
                        {
                            "competitors": [
                                {"homeAway": "home", "team": {"displayName": "Valencia"}},
                                {"homeAway": "away", "team": {"displayName": "Schalke 04"}}
                            ]
                        }
                    ]
                }
            ]
        }
        mock_get.return_value = mock_response
        
        match_id = self.client.find_match_id("15-02-2011", "Valencia", "Schalke 04")
        self.assertEqual(match_id, "123456")

if __name__ == '__main__':
    unittest.main()