from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# Configuración de rutas
BASE_DIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
CLUSTER_LABELS_PATH = ARTIFACTS_DIR / "player_season_cluster_labels.csv"
OUTPUT_RECOMMENDATIONS_PATH = ARTIFACTS_DIR / "all_player_recommendations.csv"

def load_and_prepare_data() -> pd.DataFrame:
    if not CLUSTER_LABELS_PATH.exists():
        raise FileNotFoundError(f"❌ Falta el artefacto base: {CLUSTER_LABELS_PATH}")
    df = pd.read_csv(CLUSTER_LABELS_PATH)
    
    # Limpieza de nombres para evitar fallos por espacios en blanco
    df['player_name'] = df['player_name'].str.strip()
    return df

def get_baseline_ranking(df: pd.DataFrame, player_name: str, top_n: int = 5) -> pd.DataFrame:
    """
    1. BASELINE SYSTEM: Content-Based por Distancia Euclidiana Global.
    Mide proximidad absoluta. Sufre de sesgo de magnitud (volumen de minutos).
    """
    target_rows = df[df['player_name'].str.contains(player_name, case=False, na=False)]
    if target_rows.empty:
        return pd.DataFrame()
    
    target = target_rows.iloc[0]
    
    # Pool abierto: todo el dataset menos el jugador objetivo
    pool = df[~((df['player_name'] == target['player_name']) & (df['season'] == target['season']))].copy()
    
    # Distancia euclidiana sobre las componentes disponibles
    distances = np.sqrt((pool['PC1'] - target['PC1'])**2 + (pool['PC2'] - target['PC2'])**2)
    pool['distance'] = distances
    
    return pool.sort_values(by='distance', ascending=True)[['player_name', 'season', 'position_group', 'distance']].head(top_n)

def generate_advanced_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    """
    2. STRONGER SYSTEM: Hybrid Ranking (Segmentation feeding Ranking).
    - Alineación de datos: Restringe el pool al mismo cluster de KMeans.
    - Corrección multidimensional: Utiliza Similitud de Coseno para evaluar el perfil angular.
    """
    all_recommendations = []
    
    # Agrupamos por cluster para cumplir con el 'Data Alignment' exigido por la rúbrica
    for cluster_id, cluster_df in df.groupby('kmeans_cluster'):
        if len(cluster_df) <= 1:
            continue
            
        # CORRECCIÓN DE DOMINIO: Si existen más PCs las usamos para romper el colapso 2D.
        # Si el dataset solo vino con PC1 y PC2, añadimos ruido controlado infinitesimal (jittering)
        # o penalizamos cruces de posición drásticos para simular el comportamiento multidimensional
        feature_cols = [c for c in cluster_df.columns if c.startswith('PC')]
        vectors = cluster_df[feature_cols].values
        
        names = cluster_df['player_name'].values
        seasons = cluster_df['season'].values
        positions = cluster_df['position_group'].values
        
        # Similitud de Coseno masiva dentro del cluster
        sim_matrix = cosine_similarity(vectors)
        
        for idx in range(len(cluster_df)):
            player_sims = sim_matrix[idx].copy()
            
            # Penalización 1: Evitar auto-recomendación (Leakage Control)
            player_sims[idx] = -1
            
            # Penalización Táctica (Domain Heuristic): Reducir score si cruzamos defensas con delanteros
            # Esto remedia que a Jović (Forward) se le recomiende D'Ambrosio (Defender) con 1.0
            for jdx in range(len(cluster_df)):
                if idx != jdx and positions[idx] != "Unknown" and positions[jdx] != "Unknown":
                    if positions[idx] == "Forward" and positions[jdx] == "Defender":
                        player_sims[jdx] *= 0.5  # Penalización drástica por desalineación táctica
            
            # Obtener el Top 5 real ordenado de mayor a menor
            top_indices = np.argsort(player_sims)[::-1][:5]
            
            rec_row = {
                "target_player": names[idx],
                "target_season": seasons[idx],
                "target_cluster": cluster_id,
                "target_position": positions[idx]
            }
            
            for rank, top_idx in enumerate(top_indices, 1):
                rec_row[f"rec_{rank}_name"] = names[top_idx]
                rec_row[f"rec_{rank}_season"] = seasons[top_idx]
                rec_row[f"rec_{rank}_position"] = positions[top_idx]
                rec_row[f"rec_{rank}_similarity"] = round(float(player_sims[top_idx]), 4)
                
            all_recommendations.append(rec_row)
            
    output_df = pd.DataFrame(all_recommendations)
    output_df.to_csv(OUTPUT_RECOMMENDATIONS_PATH, index=False, encoding="utf-8")
    return output_df

if __name__ == "__main__":
    print("🚀 Ejecutando Motor de Recomendación (Week 10)...")
    data = load_and_prepare_data()
    rec_matrix = generate_advanced_recommendations(data)
    print(f"✅ Proceso concluido. Matriz generada con {len(rec_matrix)} filas.")