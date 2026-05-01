"""
MASTER TEST: Runs all diagnostic tests for every scraper and generates a summary.
Usage: python tests/api_diagnostics/run_all_tests.py
"""
import subprocess
import sys
import os
import json

TESTS_DIR = os.path.dirname(__file__)
PYTHON = sys.executable

# Define the new modular tests
tests = [
    ('UEFA Diagnostic',          os.path.join(TESTS_DIR, 'uefa', 'test_uefa.py')),
    ('ESPN Diagnostic',          os.path.join(TESTS_DIR, 'espn', 'test_espn.py')),
    ('FBRef Diagnostic',         os.path.join(TESTS_DIR, 'fbref', 'test_fbref.py')),
    ('Flashscore Diagnostic',    os.path.join(TESTS_DIR, 'flashscore', 'test_flashscore.py')),
    ('Worldfootball Diagnostic', os.path.join(TESTS_DIR, 'worldfootball', 'test_worldfootball.py')),
]

print("=" * 65)
print("UCL DATA PIPELINE - CONSOLIDATED DIAGNOSTIC SUITE")
print("=" * 65)

results_summary = {}

for name, script in tests:
    print(f"\n>>> Running: {name}")
    print("-" * 45)
    if not os.path.exists(script):
        print(f"[!] Error: Test script not found at {script}")
        results_summary[name] = 'MISSING'
        continue

    proc = subprocess.run(
        [PYTHON, script],
        capture_output=True, text=True, encoding='utf-8'
    )
    print(proc.stdout.strip())
    if proc.stderr.strip():
        print("[STDERR]:", proc.stderr.strip())
    results_summary[name] = 'PASS' if proc.returncode == 0 else 'FAIL'

print("\n" + "=" * 65)
print("CONSOLIDATED SUMMARY")
print("=" * 65)
for name, status in results_summary.items():
    icon = "V" if status == 'PASS' else "X"
    if status == 'MISSING': icon = "!"
    print(f"  {icon} {name:30}: {status}")

print("\n[*] All diagnostic JSONs have been updated in tests/api_diagnostics/results/[source]/")
