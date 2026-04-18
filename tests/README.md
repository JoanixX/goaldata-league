# Testing Suite (tests)
This directory contains the validation and diagnostic tools used to ensure the integrity of the UEFA Champions League dataset.

## Subdirectories
- **`api_diagnostics/`**: A specialized suite of scripts designed to test specific API endpoints and report on field coverage.
  - `run_all_tests.py`: Aggregates all diagnostic checks and generates a summary report.
  - `test_field_coverage.py`: Scans the processed CSV and calculates the percentage of missing values for every required field.
  - `results/`: Contains raw JSON responses and the final `field_coverage.json` report.

## Test Files
- **`test_integration.py`**: Verifies the end-to-end enrichment pipeline. It checks if a raw row can be successfully enriched with stats and saved to the output CSV.
- **`test_api_clients.py`**: Unit tests for the UEFA and ESPN client classes, ensuring they correctly handle successful responses and API errors.
- **`test_formatter.py`**: Validates the utility functions in `src/formatter.py`, such as team name matching and date parsing.

## Running Tests
To execute all tests, use `pytest` from the project root:
```bash
pytest tests/
```

To run the full diagnostic suite and generate the coverage report:
```bash
python tests/api_diagnostics/run_all_tests.py
```
