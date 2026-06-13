import json
from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
REPORTS_DIR = BASE_DIR / "reports"

CLUSTER_LABELS_PATH = ARTIFACTS_DIR / "player_season_cluster_labels.csv"
RECOMMENDATIONS_PATH = ARTIFACTS_DIR / "all_player_recommendations.csv"

def calculate_offline_metrics():
    """
    Calcula el protocolo de evaluación offline comparando el sistema avanzado
    contra un baseline de control de popularidad posicional.
    """
    if not RECOMMENDATIONS_PATH.exists() or not CLUSTER_LABELS_PATH.exists():
        print("❌ Archivos no encontrados. Ejecuta primero src/recommendation_engine.py")
        return

    df_labels = pd.read_csv(CLUSTER_LABELS_PATH)
    df_recs = pd.read_csv(RECOMMENDATIONS_PATH)
    
    # Muestra de validación: Solo evaluamos registros con posición conocida
    valid_rows = df_recs[~df_recs['target_position'].isin(['Unknown', 'Other'])]
    N = len(valid_rows)
    
    if N == 0:
        print("⚠ No hay suficientes perfiles estructurados para validar.")
        return

    # Contadores Sistema Avanzado (Híbrido)
    adv_p1 = 0
    adv_p5 = 0
    adv_unique = set()
    
    # Contadores Baseline (Popularidad Táctica del Cluster)
    base_p1 = 0
    base_p5 = 0
    base_unique = set()

    # Mapeo rápido de popularidad por frecuencia para el baseline de control
    pop_map = df_labels['player_name'].value_counts().to_dict()
    pos_map = df_labels.set_index('player_name')['position_group'].to_dict()

    for _, row in valid_rows.iterrows():
        target_pos = row['target_position']
        cluster_id = row['target_cluster']
        target_player = row['target_player']
        
        # ----------------------------------------------------
        # EVALUACIÓN DEL SISTEMA AVANZADO
        # ----------------------------------------------------
        if row['rec_1_position'] == target_pos:
            adv_p1 += 1
            
        matches_adv = sum([1 for r in range(1, 6) if row[f'rec_{r}_position'] == target_pos])
        adv_p5 += (matches_adv / 5.0)
        
        for r in range(1, 6):
            adv_unique.add(row[f'rec_{r}_name'])
            
        # ----------------------------------------------------
        # EVALUACIÓN DEL BASELINE (Control por Popularidad del Cluster)
        # ----------------------------------------------------
        # Filtra candidatos del mismo cluster que no sean el jugador objetivo
        candidates = df_labels[(df_labels['kmeans_cluster'] == cluster_id) & (df_labels['player_name'] != target_player)].copy()
        candidates['pop_score'] = candidates['player_name'].map(pop_map)
        
        top_pop = candidates.sort_values(by='pop_score', ascending=False)['player_name'].unique()[:5]
        
        if len(top_pop) > 0:
            if pos_map.get(top_pop[0], "Unknown") == target_pos:
                base_p1 += 1
            
            matches_base = sum([1 for name in top_pop if pos_map.get(name, "Unknown") == target_pos])
            base_p5 += (matches_base / 5.0)
            
            for name in top_pop:
                base_unique.add(name)

    total_catalog = len(df_labels['player_name'].unique())

    report_data = {
        "evaluation_timestamp": str(np.datetime64('now')),
        "metrics": {
            "stronger_system": {
                "precision_at_1": round(adv_p1 / N, 4),
                "precision_at_5": round(adv_p5 / N, 4),
                "catalog_coverage": round((len(adv_unique) / total_catalog) * 100, 2)
            },
            "baseline_popularity": {
                "precision_at_1": round(base_p1 / N, 4),
                "precision_at_5": round(base_p5 / N, 4),
                "catalog_coverage": round((len(base_unique) / total_catalog) * 100, 2)
            }
        }
    }

    # Despliegue formal en terminal
    print("\n" + "="*60)
    print("         OFFLINE EVALUATION COMPARISON REPORT          ")
    print("="*60)
    print(f"📊 Métrica         | Sistema Avanzado | Baseline Popularidad")
    print("-" * 60)
    print(f"🎯 Precision@1     | {report_data['metrics']['stronger_system']['precision_at_1']:.4f}           | {report_data['metrics']['baseline_popularity']['precision_at_1']:.4f}")
    print(f"🎯 Precision@5     | {report_data['metrics']['stronger_system']['precision_at_5']:.4f}           | {report_data['metrics']['baseline_popularity']['precision_at_5']:.4f}")
    print(f"📈 Catalog Coverage| {report_data['metrics']['stronger_system']['catalog_coverage']:.2f}%          | {report_data['metrics']['baseline_popularity']['catalog_coverage']:.2f}%")
    print("="*60)

    REPORTS_DIR.mkdir(exist_ok=True)
    with open(REPORTS_DIR / "offline_evaluation_report.json", "w") as f:
        json.dump(report_data, f, indent=4)
    print("📂 Reporte comparativo de control actualizado con éxito.")

if __name__ == "__main__":
    calculate_offline_metrics()