import os
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# Localizar las mismas rutas que usa tu script original
BASE_DIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
CLUSTER_LABELS_PATH = ARTIFACTS_DIR / "player_season_cluster_labels.csv"

def load_data():
    if not CLUSTER_LABELS_PATH.exists():
        raise FileNotFoundError(f"No se encuentra el archivo {CLUSTER_LABELS_PATH}. Ejecuta primero el script de clustering.")
    return pd.read_csv(CLUSTER_LABELS_PATH)

def find_target_player(df, player_name, season=None):
    """
    Busca al jugador de forma flexible y diagnostica el formato exacto si falla.
    """
    # Limpieza básica para evitar fallas por espacios vacíos
    df['player_name_clean'] = df['player_name'].str.strip()
    
    # Intento 1: Buscar nombre exacto
    player_df = df[df['player_name_clean'] == player_name.strip()]
    
    if player_df.empty:
        # Intento 2: Búsqueda parcial por si acaso (ej. "Modric" sin tilde)
        player_df = df[df['player_name_clean'].str.contains(player_name.strip(), case=False, na=False)]
        if player_df.empty:
            raise ValueError(f"❌ El jugador '{player_name}' no se encuentra en el dataset.")
        print(f"⚠️ Nombre exacto no hallado. Usando coincidencia parcial: '{player_df['player_name'].iloc[0]}'")

    # Si se especificó temporada, intentamos filtrar por ella
    if season:
        season_df = player_df[player_df['season'].astype(str) == str(season)]
        if not season_df.empty:
            return season_df.iloc[0]
        
        # Si la temporada falló, alertamos e imprimimos los formatos que SÍ existen
        print(f"❌ La temporada '{season}' no existe para este jugador.")
        print(f"💡 Temporadas disponibles para {player_df['player_name'].iloc[0]}: {player_df['season'].unique().tolist()}")
        print(f"🔄 Usando la primera temporada disponible de manera automática.")
        
    return player_df.iloc[0]


def get_baseline_ranking(player_name, season=None, top_n=5):
    """
    1. BASELINE SYSTEM: Content-Based Simple por Distancia Euclidiana Directa
    """
    df = load_data()
    target = find_target_player(df, player_name, season)
    
    # Reasignar por si la función flexible cambió el target/season real
    actual_name = target['player_name']
    actual_season = target['season']
    
    # Pool de candidatos
    pool = df[~((df['player_name'] == actual_name) & (df['season'] == actual_season))].copy()
    
    # Distancia Euclidiana
    distances = np.sqrt((pool['PC1'] - target['PC1'])**2 + (pool['PC2'] - target['PC2'])**2)
    pool['distance'] = distances
    
    return pool.sort_values(by='distance', ascending=True)[['player_name', 'season', 'position_group', 'distance']].head(top_n)


def get_advanced_ranking(player_name, season=None, top_n=5):
    """
    2. STRONGER SYSTEM: Segmentation Feeding Ranking (Hybrid Framework)
    """
    df = load_data()
    target = find_target_player(df, player_name, season)
    
    actual_name = target['player_name']
    actual_season = target['season']
    target_cluster = target['kmeans_cluster']
    target_vector = np.array([[target['PC1'], target['PC2']]])
    
    # ALINEACIÓN DE DATOS: Mismo cluster, excluyendo al objetivo
    candidate_pool = df[(df['kmeans_cluster'] == target_cluster) & 
                        ~((df['player_name'] == actual_name) & (df['season'] == actual_season))].copy()
    
    if candidate_pool.empty:
        return pd.DataFrame(columns=['player_name', 'season', 'position_group', 'similarity'])
        
    # Calcular Similitud de Coseno
    candidate_vectors = candidate_pool[['PC1', 'PC2']].values
    candidate_pool['similarity'] = cosine_similarity(target_vector, candidate_vectors).flatten()
    
    return candidate_pool.sort_values(by='similarity', ascending=False)[['player_name', 'season', 'position_group', 'similarity']].head(top_n)

# --- Ejecución de Pruebas ---
if __name__ == "__main__":
    # Probamos mandando solo el nombre para que el asistente de diagnóstico haga su magia
    print("--- BASELINE SYSTEM (Euclidean Distance Global) ---")
    print(get_baseline_ranking("Modrić"))
    
    print("\n--- ADVANCED SYSTEM (Segmentation + Cosine Similarity) ---")
    print(get_advanced_ranking("Modrić"))