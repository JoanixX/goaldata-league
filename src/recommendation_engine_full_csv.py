import os
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# Configuración de rutas de tu proyecto
BASE_DIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
CLUSTER_LABELS_PATH = ARTIFACTS_DIR / "player_season_cluster_labels.csv"
OUTPUT_RECOMMENDATIONS_PATH = ARTIFACTS_DIR / "all_player_recommendations.csv"

def generate_all_recommendations():
    if not CLUSTER_LABELS_PATH.exists():
        raise FileNotFoundError(f"Falta el archivo base: {CLUSTER_LABELS_PATH}")
        
    df = pd.read_csv(CLUSTER_LABELS_PATH)
    print(f"📊 Cargados {len(df)} perfiles de jugadores. Iniciando motor de recomendación global...")
    
    all_recommendations = []
    
    # Agrupamos por cluster para optimizar la velocidad del cálculo funcional (Alineación de datos)
    for cluster_id, cluster_df in df.groupby('kmeans_cluster'):
        if len(cluster_df) <= 1:
            continue
            
        print(f"⚙️ Procesando Cluster {cluster_id} ({len(cluster_df)} jugadores)...")
        
        # Extraer las matrices de coordenadas del espacio latente
        vectors = cluster_df[['PC1', 'PC2']].values
        names = cluster_df['player_name'].values
        seasons = cluster_df['season'].values
        positions = cluster_df['position_group'].values
        
        # Calcular la matriz de similitud de coseno interna del cluster completo de golpe
        sim_matrix = cosine_similarity(vectors)
        
        # Para cada jugador en este cluster, encontrar sus mejores coincidencias
        for idx in range(len(cluster_df)):
            current_player = names[idx]
            current_season = seasons[idx]
            
            # Obtener el vector de similitudes de este jugador con los demás
            player_sims = sim_matrix[idx].copy()
            
            # Penalizar la autosimilitud (poner en -1 para que no se recomiende a sí mismo)
            player_sims[idx] = -1
            
            # Obtener los índices de los 5 valores más altos de similitud
            top_indices = np.argsort(player_sims)[::-1][:5]
            
            # Estructurar las recomendaciones en formato de columnas
            rec_row = {
                "target_player": current_player,
                "target_season": current_season,
                "target_cluster": cluster_id,
                "target_position": positions[idx]
            }
            
            # Añadir dinámicamente los campos al CSV final
            for rank, top_idx in enumerate(top_indices, 1):
                rec_row[f"rec_{rank}_name"] = names[top_idx]
                rec_row[f"rec_{rank}_season"] = seasons[top_idx]
                rec_row[f"rec_{rank}_position"] = positions[top_idx]
                rec_row[f"rec_{rank}_similarity"] = round(float(player_sims[top_idx]), 4)
                
            all_recommendations.append(rec_row)
            
    # Guardar la matriz completa en un nuevo artefacto CSV
    output_df = pd.DataFrame(all_recommendations)
    output_df.to_csv(OUTPUT_RECOMMENDATIONS_PATH, index=False, encoding="utf-8")
    print(f"✅ ¡Éxito! El sistema ha generado las recomendaciones para todos los jugadores.")
    print(f"📂 Archivo guardado en: {OUTPUT_RECOMMENDATIONS_PATH}")

if __name__ == "__main__":
    generate_all_recommendations()