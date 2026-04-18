import pandas as pd
import os
import json

def convert_csv_to_seasonal_json(input_path, output_dir):
    """
    Reads the master processed CSV and saves one JSON file per season.
    """
    if not os.path.exists(input_path):
        print(f"Error: Processed CSV not found at {input_path}")
        return

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Reading processed data from {input_path}...")
    df = pd.read_csv(input_path)

    seasons = df['season'].unique()
    print(f"Found {len(seasons)} seasons. Exporting to JSON...")

    for season in seasons:
        season_df = df[df['season'] == season]
        # Clean season name for filename (e.g. 2010-2011 -> 2010_2011)
        filename = f"{season.replace('-', '_')}.json"
        output_file = os.path.join(output_dir, filename)
        
        # Convert to list of dicts
        records = season_df.to_dict(orient='records')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=4, ensure_ascii=False)
        
        print(f"  - Saved: {filename}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_csv = os.path.join(base_dir, 'data', 'processed', 'champions_league_2011_2025_completed.csv')
    output_json_dir = os.path.join(base_dir, 'data', 'processed', 'json_seasons')
    
    convert_csv_to_seasonal_json(input_csv, output_json_dir)
