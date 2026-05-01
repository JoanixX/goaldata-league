import pandas as pd
import os
import json

def convert_relational_to_json(raw_dir, output_dir):
    """
    Reads all relational CSVs and groups them into a nested JSON structure by season.
    The JSON will be hierarchical: Season -> Match -> Events & Stats.
    """
    print(f"[*] Starting relational to JSON conversion from {raw_dir}...")
    
    # 1. Load all core tables
    try:
        matches = pd.read_csv(os.path.join(raw_dir, 'core', 'matches.csv'), keep_default_na=False)
        teams = pd.read_csv(os.path.join(raw_dir, 'core', 'teams.csv'), keep_default_na=False)
        players = pd.read_csv(os.path.join(raw_dir, 'core', 'players.csv'), keep_default_na=False)
        
        # 2. Load stats and events
        events = pd.read_csv(os.path.join(raw_dir, 'events', 'goals_events.csv'), keep_default_na=False)
        player_stats = pd.read_csv(os.path.join(raw_dir, 'stats', 'player_match_stats.csv'), keep_default_na=False)
        gk_stats = pd.read_csv(os.path.join(raw_dir, 'stats', 'goalkeeper_stats.csv'), keep_default_na=False)
    except Exception as e:
        print(f"[!] Error loading CSV files: {e}")
        return

    # Helper maps for quick lookup
    team_map = teams.set_index('team_id').to_dict(orient='index')
    player_map = players.set_index('player_id').to_dict(orient='index')

    seasons = matches['season'].unique()
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for season in seasons:
        print(f"  - Processing season: {season}")
        season_matches = matches[matches['season'] == season]
        
        season_data = {
            "season": season,
            "metadata": {
                "match_count": len(season_matches),
                "export_date": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            "matches": []
        }
        
        for _, m in season_matches.iterrows():
            mid = m['match_id']
            
            # Find goals for this match
            match_events = events[events['match_id'] == mid].to_dict(orient='records')
            
            # Find player stats for this match
            # We enrich stats with player names for readability in JSON
            m_stats = player_stats[player_stats['match_id'] == mid].copy()
            m_stats['player_name'] = m_stats['player_id'].map(lambda x: player_map.get(x, {}).get('player_name', 'Unknown'))
            stats_list = m_stats.to_dict(orient='records')
            
            match_obj = {
                "match_id": mid,
                "date": m['date'],
                "teams": {
                    "home": {
                        "team_id": m['home_team_id'],
                        "name": team_map.get(m['home_team_id'], {}).get('team_name', 'Unknown'),
                        "score": int(m['home_score']),
                        "possession": float(m['possession_home'])
                    },
                    "away": {
                        "team_id": m['away_team_id'],
                        "name": team_map.get(m['away_team_id'], {}).get('team_name', 'Unknown'),
                        "score": int(m['away_score']),
                        "possession": float(m['possession_away'])
                    }
                },
                "info": {
                    "stadium": m['stadium'],
                    "city": m['city'],
                    "referee": m['referee']
                },
                "events": {
                    "goals": match_events
                },
                "statistics": stats_list
            }
            season_data["matches"].append(match_obj)

        # Save to JSON
        filename = f"{season.replace('-', '_')}.json"
        output_file = os.path.join(output_dir, filename)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(season_data, f, indent=4, ensure_ascii=False)
            
    print(f"[*] Conversion complete. JSON files saved in {output_dir}")

if __name__ == "__main__":
    # Base configuration
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base, 'data', 'raw')
    json_path = os.path.join(base, 'data', 'raw', 'json_seasons')
    
    convert_relational_to_json(raw_path, json_path)
