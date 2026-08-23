import pandas as pd
import os

# CONFIGURACIÓN DE RUTAS
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

TABLES = [
    {"raw": "core/matches.csv", "processed": "core/matches_cleaned.csv"},
    {"raw": "core/players.csv", "processed": "core/players_cleaned.csv"},
    {"raw": "core/teams.csv", "processed": "core/teams_cleaned.csv"},
    {"raw": "stats/goalkeeper_stats.csv", "processed": "stats/goalkeeper_stats_cleaned.csv"},
    {"raw": "stats/player_match_stats.csv", "processed": "stats/player_match_stats_cleaned.csv"},
    {"raw": "stats/player_season_stats.csv", "processed": "stats/player_season_stats_cleaned.csv"},
    {"raw": "events/goals_events.csv", "processed": "events/goals_events_cleaned.csv"},
]

def clean_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Base hook for dataset cleaning.
    Currently a passthrough, ready for validation and cleaning rules once the
    scraping stage is finished.
    """
    # Example of the intended structure:
    # if "players" in table_name:
    #     df = df.drop_duplicates(subset=["player_id"])
    
    return df

def main():
    print("Starting the data cleaning pipeline...")
    
    for table in TABLES:
        raw_path = os.path.join(RAW_DIR, table["raw"])
        processed_path = os.path.join(PROCESSED_DIR, table["processed"])
        
        if not os.path.exists(raw_path):
            print(f"  [!] File not found: {raw_path}")
            continue
            
        print(f"Processing: {table['raw']}...")
        df = pd.read_csv(raw_path)
        
        # Apply cleaning
        df_clean = clean_table(df, table["raw"])
        
        # Make sure the destination directory exists
        os.makedirs(os.path.dirname(processed_path), exist_ok=True)
        
        # Write the cleaned file
        df_clean.to_csv(processed_path, index=False)
        print(f"  -> Written to {processed_path} with shape {df_clean.shape}")

    print("Pipeline finished successfully.")

if __name__ == "__main__":
    main()
