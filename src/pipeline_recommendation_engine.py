import sys
from pathlib import Path

# Forzar la inclusión de src en el path del sistema
sys.path.append(str(Path(__file__).resolve().parent))

import recommendation_engine
import evaluation_metrics_recommendation_engine

def main():
    print("======================================================================")
    print("         INICIANDO PIPELINE PRODUCTIVO: GOALDATA LEAGUE              ")
    print("======================================================================")
    
    try:
        # Step 1: Cargar datos y ejecutar el motor híbrido (Segmentation + Ranking)
        data = recommendation_engine.load_and_prepare_data()
        print("\n[STEP 1/2] Calculando recomendaciones globales...")
        recommendation_engine.generate_advanced_recommendations(data)
        
        # Step 2: Correr el protocolo de métricas offline
        print("\n[STEP 2/2] Generando reporte de métricas y validación...")
        evaluation_metrics_recommendation_engine.calculate_offline_metrics()
        
        print("\n🚀 Pipeline completado con éxito. Rama 'mauricio_branch' lista para el merge.")
        
    except Exception as e:
        print(f"\n❌ Error crítico durante la ejecución del Pipeline: {str(e)}")

if __name__ == "__main__":
    main()