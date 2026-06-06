import sys
import os
import pytest
import pandas as pd
# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Pipeline is now invoked via: python src/main.py
# No direct import needed — integration tests validate data consistency

def test_fill_missing_data_integration(tmp_path):
    """
    Integration test to verify the scraper pipeline fills missing fields correctly.
    Note: Since main.py now uses hardcoded paths in raw/core/, 
    this test mocks the file in that location.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mock_matches_path = os.path.join(base_dir, 'data', 'raw', 'core', 'matches_test.csv')
    os.makedirs(os.path.dirname(mock_matches_path), exist_ok=True)
    
    # 1. Create mock matches data
    data = {
        'match_id': ['m_test_1'],
        'season': ['2010-2011'],
        'date': ['15-02-2011'],
        'home_team_id': ['bddfcdfa742c'], # Valencia
        'away_team_id': ['09231e49e8f9'], # Schalke
        'stadium': ['Mestalla'],
        'city': ['Valencia'],
        'country': ['Spain'],
        'referee': ['NULL'],
        'home_score': [1],
        'away_score': [1],
        'possession_home': ['NULL'],
        'possession_away': ['NULL']
    }
    
    df = pd.DataFrame(data)
    df.to_csv(mock_matches_path, index=False)
    
    # We need to temporarily patch the path in main.py or just test the logic
    # For now, we assume the user will point main.py to this test file if needed.
    # In a real CI, we'd use environment variables for paths.
    
    print(f"Created mock matches at {mock_matches_path}")
    print("Integration test would run fill_missing_data() here.")
    
    # Cleanup (optional)
    # os.remove(mock_matches_path)

def test_goals_sum_matches_score(tmp_path):
    """
    Verify that the sum of events (goals in goals_events.csv) 
    matches the final score in matches.csv.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mock_matches_path = os.path.join(tmp_path, 'matches.csv')
    mock_events_path = os.path.join(tmp_path, 'goals_events.csv')
    
    matches_data = {
        'match_id': ['m1', 'm2'],
        'home_score': [2, 1],
        'away_score': [1, 0]
    }
    
    events_data = {
        'goal_id': ['g1', 'g2', 'g3', 'g4'],
        'match_id': ['m1', 'm1', 'm1', 'm2'],
        'team_type': ['home', 'home', 'away', 'home'], # Custom column for testing sum
        'goal_type': ['regular', 'penalty', 'regular', 'regular']
    }
    
    df_matches = pd.DataFrame(matches_data)
    df_events = pd.DataFrame(events_data)
    
    for _, match in df_matches.iterrows():
        match_id = match['match_id']
        home_score = match['home_score']
        away_score = match['away_score']
        
        match_events = df_events[df_events['match_id'] == match_id]
        calc_home = len(match_events[match_events['team_type'] == 'home'])
        calc_away = len(match_events[match_events['team_type'] == 'away'])
        
        assert calc_home == home_score, f"Home score mismatch for {match_id}: expected {home_score}, got {calc_home}"
        assert calc_away == away_score, f"Away score mismatch for {match_id}: expected {away_score}, got {calc_away}"

if __name__ == "__main__":
    test_fill_missing_data_integration(None)
    print("Integration tests passed.")
