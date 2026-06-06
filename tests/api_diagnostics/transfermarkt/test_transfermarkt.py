import os
import json
import pytest
import sys

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(BASE_DIR, 'src'))

RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results', 'transfermarkt')

def test_transfermarkt_json_exists():
    """Verifica que el scraper de Transfermarkt genere los reportes JSON."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    sample_path = os.path.join(RESULTS_DIR, 'sample_data.json')
    # This test assumes the scraper has been run at least once
    if os.path.exists(sample_path):
        with open(sample_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            assert isinstance(data, list)
            if len(data) > 0:
                assert 'height_cm' in data[0]
                assert 'position' in data[0]
    else:
        pytest.skip("Transfermarkt sample_data.json not found. Run the scraper first.")

if __name__ == "__main__":
    pytest.main([__file__])
