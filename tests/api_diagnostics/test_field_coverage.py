"""
DIAGNOSTIC 5: Full Field Coverage Check
Scans the processed CSV and reports what % of rows have each required field filled.
Run: python tests/api_diagnostics/test_field_coverage.py
"""
import sys, os, json
import pandas as pd

BASE = os.path.join(os.path.dirname(__file__), '..', '..')
CSV_PATH = os.path.join(BASE, 'data', 'processed', 'champions_league_2011_2025_completed.csv')

REQUIRED_FIELDS = [
    'arbitro_principal', 'arbitros_linea',
    'entrenador_local', 'entrenador_visitante',
    'planteles',
    'tiros_totales', 'tiros_totales_local', 'tiros_totales_visitante',
    'tiros_puerta', 'tiros_puerta_local', 'tiros_puerta_visitante',
    'goles', 'asistencias', 'amarillas',
    'posesion_local', 'posesion_visitante',
    'faltas_total', 'faltas_local', 'faltas_visitante',
    'corners_total', 'corners_local', 'corners_visitante',
]


def is_filled(val):
    if val is None:
        return False
    s = str(val).strip()
    return s not in ('NULL', '', 'nan', 'NaN', 'None')


df = pd.read_csv(CSV_PATH, keep_default_na=False)
total = len(df)
print(f"Total rows: {total}")
print(f"{'Field':<35} {'Filled':>8} {'Missing':>8} {'% Filled':>10}")
print('-' * 65)

coverage = {}
for field in REQUIRED_FIELDS:
    if field not in df.columns:
        print(f"{'[MISSING COLUMN] ' + field:<35} {'N/A':>8} {'N/A':>8} {'N/A':>10}")
        coverage[field] = {'filled': 0, 'missing': total, 'pct': 0, 'column_exists': False}
        continue
    filled = df[field].apply(is_filled).sum()
    missing = total - filled
    pct = 100 * filled / total
    flag = '' if pct >= 95 else (' ⚠ LOW' if pct >= 50 else ' ✗ CRITICAL')
    print(f"{field:<35} {filled:>8} {missing:>8} {pct:>9.1f}%{flag}")
    coverage[field] = {'filled': int(filled), 'missing': int(missing), 'pct': round(pct, 1)}

# Show worst missing rows for CRITICAL fields
critical = [f for f, v in coverage.items() if v.get('pct', 100) < 80 and v.get('column_exists', True)]
if critical:
    print(f"\n=== Sample rows missing critical fields ===")
    for field in critical[:3]:
        if field not in df.columns:
            continue
        missing_rows = df[~df[field].apply(is_filled)][['season', 'fase', 'local', 'visitante', 'fecha', field]].head(5)
        print(f"\n[{field}] - first 5 missing:")
        print(missing_rows.to_string(index=False))

os.makedirs('tests/api_diagnostics/results', exist_ok=True)
with open('tests/api_diagnostics/results/field_coverage.json', 'w') as f:
    json.dump({'total_rows': total, 'coverage': coverage}, f, indent=2)

print(f"\nResults saved to tests/api_diagnostics/results/field_coverage.json")
critical_count = sum(1 for v in coverage.values() if v.get('pct', 100) < 50)
print(f"\n✗ CRITICAL fields (<50% filled): {critical_count}")
print(f"⚠ LOW fields (50-95%): {sum(1 for v in coverage.values() if 50 <= v.get('pct', 100) < 95)}")
print(f"✓ OK fields (>=95%): {sum(1 for v in coverage.values() if v.get('pct', 100) >= 95)}")
