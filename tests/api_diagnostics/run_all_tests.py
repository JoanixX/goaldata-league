"""
MASTER TEST: Runs all diagnostics and generates a final coverage report.
Run: python tests/api_diagnostics/run_all_tests.py
"""
import subprocess, sys, os, json

BASE = os.path.join(os.path.dirname(__file__), '..', '..')
TESTS_DIR = os.path.dirname(__file__)
PYTHON = sys.executable

tests = [
    ('UEFA Officials',    os.path.join(TESTS_DIR, 'test_uefa_officials.py')),
    ('ESPN Stats',        os.path.join(TESTS_DIR, 'test_espn_stats.py')),
    ('ESPN Events',       os.path.join(TESTS_DIR, 'test_espn_events.py')),
    ('ESPN Rosters',      os.path.join(TESTS_DIR, 'test_espn_rosters.py')),
    ('ESPN Discovery',    os.path.join(TESTS_DIR, 'test_espn_discovery.py')),
    ('Field Coverage',    os.path.join(TESTS_DIR, 'test_field_coverage.py')),
]

print("=" * 65)
print("UCL DATA PIPELINE - API DIAGNOSTICS SUITE")
print("=" * 65)

results_summary = {}

for name, script in tests:
    print(f"\n>>> Running: {name}")
    print("-" * 45)
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    proc = subprocess.run(
        [PYTHON, script],
        capture_output=True, text=True, encoding='utf-8',
        errors='replace', env=env
    )
    print(proc.stdout.strip())
    if proc.stderr.strip():
        print("[STDERR]:", proc.stderr.strip()[:500])
    results_summary[name] = 'PASS' if proc.returncode == 0 else 'FAIL'

print("\n" + "=" * 65)
print("FINAL SUMMARY")
print("=" * 65)
for name, status in results_summary.items():
    icon = "✓" if status == 'PASS' else "✗"
    print(f"  {icon} {name}: {status}")

# Also load field coverage results if available
cov_path = os.path.join(TESTS_DIR, 'results', 'field_coverage.json')
if os.path.exists(cov_path):
    with open(cov_path) as f:
        cov = json.load(f)
    print(f"\n--- Field Coverage Summary ({cov.get('total_rows', '?')} rows) ---")
    for field, data in cov.get('coverage', {}).items():
        pct = data.get('pct', 0)
        bar = '█' * int(pct // 10) + '░' * (10 - int(pct // 10))
        flag = '✓' if pct >= 95 else ('⚠' if pct >= 70 else '✗')
        print(f"  {flag} {field:<35} {bar} {pct:5.1f}%")
