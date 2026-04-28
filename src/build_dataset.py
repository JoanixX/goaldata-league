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
    Función base para la futura limpieza de los datasets.
    Actualmente actúa como passthrough, lista para agregar validaciones y limpieza 
    cuando termine la etapa de scraping.
    """
    # Ejemplo de estructura futura:
    # if "players" in table_name:
    #     df = df.drop_duplicates(subset=["player_id"])
    
    return df

def main():
    print("Iniciando pipeline de limpieza de datos...")
    
    for table in TABLES:
        raw_path = os.path.join(RAW_DIR, table["raw"])
        processed_path = os.path.join(PROCESSED_DIR, table["processed"])
        
        if not os.path.exists(raw_path):
            print(f"  [!] Archivo no encontrado: {raw_path}")
            continue
            
        print(f"Procesando: {table['raw']}...")
        df = pd.read_csv(raw_path)
        
        # Aplicar limpieza
        df_clean = clean_table(df, table["raw"])
        
        # Asegurar que el directorio de destino exista
        os.makedirs(os.path.dirname(processed_path), exist_ok=True)
        
        # Guardar archivo limpio
        df_clean.to_csv(processed_path, index=False)
        print(f"  -> Guardado en {processed_path} con shape {df_clean.shape}")

    print("Pipeline finalizado exitosamente.")

if __name__ == "__main__":
    main()