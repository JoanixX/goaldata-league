# Source Code (src)
This directory contains the core logic for the Champions League data pipeline.

## Core Components
- **`main.py`**: The main orchestration script. It loads the raw data, identifies missing fields, and triggers the enrichment process using the UEFA and ESPN APIs. It includes logic for incremental processing to avoid redundant network overhead.
- **`api_clients.py`**: A modular interface for external data sources. It handles HTTP requests, error management, and response parsing for both the official UEFA Match API and the ESPN Scoreboard/Summary APIs.
- **`formatter.py`**: Provides standardized data transformation utilities. This includes date normalization (e.g., converting "DD-MM-YYYY" to ISO format), team name normalization with alias support, and percentage formatting for possession metrics.
- **`build_dataset.py`**: Focuses on the structural assembly of the final processed CSV. It manages the merge logic between different data streams (Structural, Tactical, and Statistical).
- **`conversion_csv.py`**: A utility script used during the initial stages to standardize the raw CSV headers and basic structure.

## Data Mapping
The `src` logic relies on JSON mappings (like `uefa_match_ids.json` and `espn_id_overrides.json`) to resolve historical inconsistencies and match teams accurately across different platforms.
