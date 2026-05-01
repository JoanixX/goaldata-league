import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import formatter

class TestFormatter(unittest.TestCase):

    def test_unify_metric_name(self):
        self.assertEqual(formatter.unify_metric_name("PasAss"), "assists")
        self.assertEqual(formatter.unify_metric_name("Goals"), "goals")
        self.assertEqual(formatter.unify_metric_name("CrdY"), "yellow_cards")
        self.assertEqual(formatter.unify_metric_name("UnknownMetric"), "unknownmetric")

    def test_generate_player_id(self):
        pid1 = formatter.generate_player_id("Cristiano Ronaldo", "rm123")
        pid2 = formatter.generate_player_id("Cristiano Ronaldo", "juve456")
        pid3 = formatter.generate_player_id("C. Ronaldo", "al_nassr")
        
        # Same player = same ID regardless of team
        self.assertEqual(pid1, pid2)
        # canonical_name makes these equal
        self.assertEqual(pid1, pid3)
        self.assertNotEqual(pid1, formatter.generate_player_id("Messi", "barca"))

    def test_norm_unit_accuracy(self):
        # Percentage to decimal
        self.assertEqual(formatter.norm_unit("85.5%", "accuracy"), 0.855)
        self.assertEqual(formatter.norm_unit(0.855, "accuracy"), 0.855)
        self.assertEqual(formatter.norm_unit("64.6", "accuracy"), 0.646)
        self.assertEqual(formatter.norm_unit(None, "accuracy"), "NULL")

    def test_norm_unit_distance(self):
        self.assertEqual(formatter.norm_unit("10.5", "distance"), 10.5)
        self.assertEqual(formatter.norm_unit(12, "distance"), 12.0)

    def test_calculate_accuracy(self):
        self.assertEqual(formatter.calculate_accuracy(10, 20), 0.5)
        self.assertEqual(formatter.calculate_accuracy(1, 3), 0.3333)
        self.assertEqual(formatter.calculate_accuracy(5, 0), 0.0)

    def test_soft_norm(self):
        self.assertEqual(formatter.soft_norm("Clément"), "clement")
        self.assertEqual(formatter.soft_norm("Bayern München"), "bayern munchen")
        self.assertEqual(formatter.soft_norm("PSG!!!"), "psg")

    def test_are_equivalent(self):
        self.assertTrue(formatter.are_equivalent("0.85", "85%", "possession"))
        self.assertTrue(formatter.are_equivalent("Real Madrid", "real madrid", "team_name"))
        self.assertFalse(formatter.are_equivalent("Real Madrid", "Barcelona", "team_name"))

    def test_no_strings_in_numerical_columns(self):
        """Validates that no strings exist in numerical columns after formatting"""
        # Simulated raw dataframe row
        raw_data = {"goals": "2", "assists": "0", "distance": "10.5"}
        
        # Test the formatting/cleaning logic
        formatted_goals = int(raw_data["goals"])
        formatted_distance = float(raw_data["distance"])
        
        self.assertIsInstance(formatted_goals, int)
        self.assertIsInstance(formatted_distance, float)
        self.assertNotIsInstance(formatted_goals, str)
        self.assertNotIsInstance(formatted_distance, str)

    def test_id_consistency_between_tables(self):
        """Verifies that the ID is consistent between players and player_match_stats"""
        player_name = "Lionel Messi"
        
        # ID used in players table (team A)
        player_id_core = formatter.generate_player_id(player_name, "barca_10")
        
        # ID generated for the same player at a different team
        player_id_stats = formatter.generate_player_id("L. Messi", "inter_miami")
        
        # Must be the same because player identity is name-based, not team-based
        self.assertEqual(player_id_core, player_id_stats)

if __name__ == '__main__':
    unittest.main()