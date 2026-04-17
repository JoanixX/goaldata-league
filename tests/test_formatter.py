import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import formatter

class TestFormatter(unittest.TestCase):

    def test_format_percentage(self):
        self.assertEqual(formatter.format_percentage(64.6), "65%")
        self.assertEqual(formatter.format_percentage("35.4"), "35%")
        self.assertEqual(formatter.format_percentage(None), "NULL")
        self.assertEqual(formatter.format_percentage("invalid"), "NULL")

    def test_format_score(self):
        self.assertEqual(formatter.format_score(1, 1), "1-1")
        self.assertEqual(formatter.format_score(0, 0, "PEN"), "0-0 (P)")
        self.assertEqual(formatter.format_score(2, 1, "AET"), "2-1 (AET)")
        self.assertEqual(formatter.format_score(None, 1), "NULL")

    def test_format_events_goals(self):
        events = [
            {"type": {"text": "Goal"}, "clock": {"displayValue": "17'"}, "athletesInvolved": [{"shortName": "Soldado"}]},
            {"type": {"text": "Goal"}, "clock": {"displayValue": "64'"}, "athletesInvolved": [{"shortName": "Raul"}]}
        ]
        self.assertEqual(formatter.format_events(events, 'Goal'), "Soldado 17'; Raul 64'")

    def test_format_events_substitutions(self):
        events = [
            {"type": {"text": "Substitution"}, "clock": {"displayValue": "68'"}, "athletesInvolved": [{"shortName": "Joaquín"}, {"shortName": "Domínguez"}]},
            {"type": {"text": "Substitution"}, "clock": {"displayValue": "68'"}, "athletesInvolved": [{"shortName": "Vicente"}, {"shortName": "Banega"}]}
        ]
        self.assertEqual(formatter.format_events(events, 'Sub'), "68' Joaquín x Domínguez; 68' Vicente x Banega")

    def test_format_events_empty(self):
        self.assertEqual(formatter.format_events([], 'Goal'), "NULL")
        self.assertEqual(formatter.format_events([{"type": {"text": "Yellow Card"}}], 'Goal'), "NULL")

if __name__ == '__main__':
    unittest.main()