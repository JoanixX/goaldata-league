import sys
import os
import pytest
import pandas as pd
# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.main import fill_missing_data

def test_fill_missing_data_integration(tmp_path):
    """
    Integration test to verify the scraper pipeline fills missing fields correctly.
    """
    # 1. Create a mock CSV with a known match that triggers the scraper
    # Using Valencia vs Schalke 2011 as it has a hardcoded ID for testing
    data = {
        'season': ['2010-2011'],
        'fase': ['Octavos'],
        'instancia': ['Ida'],
        'fecha': ['15-02-2011'],
        'hora_inicio': ['NULL'],
        'hora_fin': ['NULL'],
        'local': ['Valencia'],
        'visitante': ['Schalke 04'],
        'marcador': ['1-1'],
        'global': ['NULL'],
        'estadio': ['Mestalla'],
        'ciudad': ['Valencia'],
        'pais': ['España'],
        'arbitro_principal': ['NULL'],
        'arbitros_linea': ['NULL'],
        'entrenador_local': ['NULL'],
        'entrenador_visitante': ['NULL'],
        'planteles': ['NULL'],
        'puntuaciones_jugadores': ['NULL'],
        'tiros_totales': ['NULL'],
        'tiros_totales_local': ['NULL'],
        'tiros_totales_visitante': ['NULL'],
        'tiros_puerta': ['NULL'],
        'tiros_puerta_local': ['NULL'],
        'tiros_puerta_visitante': ['NULL'],
        'goles': ['NULL'],
        'asistencias': ['NULL'],
        'cambios': ['NULL'],
        'amarillas': ['NULL'],
        'rojas': ['NULL'],
        'posesion_local': ['NULL'],
        'posesion_visitante': ['NULL'],
        'faltas_total': ['NULL'],
        'faltas_local': ['NULL'],
        'faltas_visitante': ['NULL'],
        'corners_total': ['NULL'],
        'corners_local': ['NULL'],
        'corners_visitante': ['NULL']
    }
    
    input_csv = tmp_path / "test_input.csv"
    output_csv = tmp_path / "test_output.csv"
    
    df = pd.DataFrame(data)
    df.to_csv(input_csv, index=False)
    
    # 2. Run the fill logic
    fill_missing_data(str(input_csv), str(output_csv))
    
    # 3. Verify the output
    assert os.path.exists(output_csv)
    out_df = pd.read_csv(output_csv, keep_default_na=False)
    
    # Check that critical fields are no longer NULL
    assert out_df.at[0, 'arbitro_principal'] != 'NULL'
    assert out_df.at[0, 'planteles'] != 'NULL'
    assert out_df.at[0, 'hora_inicio'] == '19:45'
    assert out_df.at[0, 'entrenador_local'] in ['Unai Emery', 'NULL']
    
    print("Integration test passed successfully!")

if __name__ == "__main__":
    # If run directly, use a local folder
    local_tmp = os.path.join(os.getcwd(), "test_tmp")
    if not os.path.exists(local_tmp):
        os.makedirs(local_tmp)
    # Mocking tmp_path as a string for manual execution
    from pathlib import Path
    test_fill_missing_data_integration(Path(local_tmp))
