#!/usr/bin/env python3
"""
validate_inputs.py
==================
Week 15 validation script for player-season data before graph construction.

Checks:
  - File existence and format (parquet/csv)
  - Required columns present
  - Data types correct
  - Nulls within acceptable bounds
  - Feature statistics (mean, std, min, max)
  - Row counts and player counts
  
Outputs:
  - logs/input_validation_report.json (detailed audit)
  - logs/input_validation_summary.txt (human-readable)
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


REQUIRED_COLUMNS = [
    'player_id', 'player_name', 'position_suitability_score', 
    'recent_form_index', 'player_age', 'height_cm',
    'goals_per90', 'assists_per90', 'tackles_per90', 'pass_completion_rate',
    'player_position_group', 'matches_played'
]

NUMERIC_COLUMNS = [
    'position_suitability_score', 'recent_form_index', 'player_age', 'height_cm',
    'goals_per90', 'assists_per90', 'tackles_per90', 'pass_completion_rate', 'matches_played'
]


def load_data(input_file: str) -> pd.DataFrame:
    """Load data from parquet or CSV."""
    logger.info(f"Loading data from {input_file}...")
    try:
        if input_file.endswith('.parquet'):
            df = pd.read_parquet(input_file)
        elif input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        else:
            raise ValueError(f"Unsupported file format: {input_file}")
        logger.info(f"✓ Loaded {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"✗ Failed to load {input_file}: {e}")
        sys.exit(1)


def check_columns(df: pd.DataFrame) -> Dict[str, Any]:
    """Verify required columns exist."""
    logger.info("Checking required columns...")
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    extra = [col for col in df.columns if col not in REQUIRED_COLUMNS]
    
    result = {
        "all_required_present": len(missing) == 0,
        "missing_columns": missing,
        "extra_columns": extra[:10],  # Show first 10
        "total_columns": len(df.columns)
    }
    
    if missing:
        logger.warning(f"✗ Missing columns: {missing}")
    else:
        logger.info(f"✓ All {len(REQUIRED_COLUMNS)} required columns present")
    
    return result


def check_dtypes(df: pd.DataFrame) -> Dict[str, Any]:
    """Verify numeric columns have correct types."""
    logger.info("Checking data types...")
    issues = []
    
    # String columns
    for col in ['player_id', 'player_name', 'player_position_group']:
        if col in df.columns and not df[col].dtype == 'object':
            issues.append(f"{col} should be object/string, got {df[col].dtype}")
    
    # Numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                issues.append(f"{col} should be numeric, got {df[col].dtype}")
    
    result = {
        "dtype_issues": issues,
        "dtypes_correct": len(issues) == 0
    }
    
    if issues:
        logger.warning(f"✗ Type issues found: {issues}")
    else:
        logger.info(f"✓ All numeric columns have correct dtypes")
    
    return result


def check_nulls(df: pd.DataFrame) -> Dict[str, Any]:
    """Check null percentages across columns."""
    logger.info("Checking null values...")
    null_pct = (df.isnull().sum() / len(df) * 100).to_dict()
    
    # Allow up to 20% nulls in 'recent_form_index' (newer players)
    # Allow up to 10% in physical attributes (height, age)
    # No nulls allowed in IDs/names
    max_allowed = {
        'player_id': 0, 'player_name': 0,
        'position_suitability_score': 5,
        'recent_form_index': 20,
        'player_age': 10, 'height_cm': 10,
        'player_position_group': 0,
        'matches_played': 0
    }
    
    critical_nulls = []
    for col, pct in null_pct.items():
        allowed = max_allowed.get(col, 15)
        if pct > allowed:
            critical_nulls.append({
                "column": col,
                "null_pct": round(pct, 2),
                "allowed_pct": allowed
            })
    
    result = {
        "nulls_by_column": {k: round(v, 2) for k, v in null_pct.items()},
        "critical_null_issues": critical_nulls,
        "nulls_acceptable": len(critical_nulls) == 0
    }
    
    if critical_nulls:
        logger.warning(f"✗ Critical null issues: {critical_nulls}")
    else:
        logger.info(f"✓ Null values within acceptable bounds")
    
    return result


def check_feature_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """Compute summary statistics for numeric features."""
    logger.info("Computing feature statistics...")
    stats = {}
    
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            col_data = df[col].dropna()
            stats[col] = {
                "count": int(len(col_data)),
                "mean": float(col_data.mean()) if len(col_data) > 0 else None,
                "std": float(col_data.std()) if len(col_data) > 0 else None,
                "min": float(col_data.min()) if len(col_data) > 0 else None,
                "25%": float(col_data.quantile(0.25)) if len(col_data) > 0 else None,
                "50%": float(col_data.quantile(0.50)) if len(col_data) > 0 else None,
                "75%": float(col_data.quantile(0.75)) if len(col_data) > 0 else None,
                "max": float(col_data.max()) if len(col_data) > 0 else None
            }
    
    logger.info(f"✓ Feature statistics computed for {len(stats)} features")
    return stats


def check_player_uniqueness(df: pd.DataFrame) -> Dict[str, Any]:
    """Check player_id uniqueness."""
    logger.info("Checking player uniqueness...")
    if 'player_id' not in df.columns:
        return {"error": "player_id column not found"}
    
    total_rows = len(df)
    unique_players = df['player_id'].nunique()
    duplicates = total_rows - unique_players
    
    result = {
        "total_rows": total_rows,
        "unique_players": unique_players,
        "duplicate_rows": duplicates,
        "all_unique": duplicates == 0
    }
    
    if duplicates > 0:
        logger.warning(f"✗ Found {duplicates} duplicate player_ids")
        # Show top 5 duplicates
        dup_counts = df['player_id'].value_counts()
        dup_counts = dup_counts[dup_counts > 1].head(5)
        result["top_duplicates"] = dup_counts.to_dict()
    else:
        logger.info(f"✓ All {unique_players} players are unique")
    
    return result


def check_positions(df: pd.DataFrame) -> Dict[str, Any]:
    """Check position values."""
    logger.info("Checking position distribution...")
    if 'player_position_group' not in df.columns:
        return {"error": "player_position_group column not found"}
    
    position_counts = df['player_position_group'].value_counts().to_dict()
    unique_positions = len(position_counts)
    
    result = {
        "unique_positions": unique_positions,
        "position_distribution": position_counts,
        "expected_positions": ['Goalkeeper', 'Defender', 'Midfielder', 'Forward']
    }
    
    logger.info(f"✓ Found {unique_positions} unique positions")
    return result


def generate_report(
    df: pd.DataFrame,
    columns_check: Dict,
    dtype_check: Dict,
    null_check: Dict,
    stats: Dict,
    uniqueness_check: Dict,
    position_check: Dict
) -> Dict[str, Any]:
    """Aggregate all validation checks into a single report."""
    
    # Overall pass/fail
    all_pass = (
        columns_check['all_required_present'] and
        dtype_check['dtypes_correct'] and
        null_check['nulls_acceptable'] and
        uniqueness_check['all_unique']
    )
    
    report = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "input_file": "data/processed/stats/player_season_stats_cleaned.parquet",
        "validation_status": "PASS" if all_pass else "FAIL",
        "summary": {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "all_checks_pass": all_pass
        },
        "checks": {
            "columns": columns_check,
            "dtypes": dtype_check,
            "nulls": null_check,
            "uniqueness": uniqueness_check,
            "positions": position_check,
            "feature_statistics": stats
        }
    }
    
    return report


def main():
    parser = argparse.ArgumentParser(
        description='Validate player-season data before graph construction.'
    )
    parser.add_argument(
        '--input',
        default='data/processed/stats/player_season_stats_cleaned.parquet',
        help='Input parquet or CSV file'
    )
    parser.add_argument(
        '--output-json',
        default='logs/input_validation_report.json',
        help='Output JSON report path'
    )
    parser.add_argument(
        '--output-txt',
        default='logs/input_validation_summary.txt',
        help='Output human-readable summary path'
    )
    args = parser.parse_args()
    
    # Create logs directory
    Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
    
    # Load and validate
    df = load_data(args.input)
    columns_check = check_columns(df)
    dtype_check = check_dtypes(df)
    null_check = check_nulls(df)
    stats = check_feature_stats(df)
    uniqueness_check = check_player_uniqueness(df)
    position_check = check_positions(df)
    
    # Generate report
    report = generate_report(df, columns_check, dtype_check, null_check, stats, uniqueness_check, position_check)
    
    # Save JSON report
    with open(args.output_json, 'w') as f:
        json.dump(report, f, indent=2)
    logger.info(f"✓ JSON report saved to {args.output_json}")
    
    # Save human-readable summary
    with open(args.output_txt, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("WEEK 15: INPUT VALIDATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Timestamp: {report['timestamp']}\n")
        f.write(f"Status: {report['validation_status']}\n\n")
        f.write(f"Total Rows: {report['summary']['total_rows']}\n")
        f.write(f"Total Columns: {report['summary']['total_columns']}\n")
        f.write(f"All Checks Pass: {report['summary']['all_checks_pass']}\n\n")
        
        f.write("COLUMN CHECK:\n")
        f.write(f"  - All required columns present: {columns_check['all_required_present']}\n")
        if columns_check['missing_columns']:
            f.write(f"  - Missing: {columns_check['missing_columns']}\n")
        f.write("\n")
        
        f.write("DATA TYPE CHECK:\n")
        f.write(f"  - All dtypes correct: {dtype_check['dtypes_correct']}\n")
        if dtype_check['dtype_issues']:
            for issue in dtype_check['dtype_issues']:
                f.write(f"  - {issue}\n")
        f.write("\n")
        
        f.write("NULL VALUE CHECK:\n")
        f.write(f"  - Nulls acceptable: {null_check['nulls_acceptable']}\n")
        if null_check['critical_null_issues']:
            for issue in null_check['critical_null_issues']:
                f.write(f"  - {issue['column']}: {issue['null_pct']}% (allowed: {issue['allowed_pct']}%)\n")
        f.write("\n")
        
        f.write("PLAYER UNIQUENESS:\n")
        f.write(f"  - Total rows: {uniqueness_check['total_rows']}\n")
        f.write(f"  - Unique players: {uniqueness_check['unique_players']}\n")
        f.write(f"  - All unique: {uniqueness_check['all_unique']}\n\n")
        
        f.write("POSITION DISTRIBUTION:\n")
        for pos, count in position_check.get('position_distribution', {}).items():
            f.write(f"  - {pos}: {count}\n")
        f.write("\n")
        
        f.write("FEATURE STATISTICS:\n")
        for col, col_stats in stats.items():
            f.write(f"  {col}:\n")
            f.write(f"    - Count: {col_stats['count']}\n")
            f.write(f"    - Mean: {col_stats['mean']:.4f}\n")
            f.write(f"    - Std: {col_stats['std']:.4f}\n")
            f.write(f"    - Range: [{col_stats['min']:.4f}, {col_stats['max']:.4f}]\n")
    
    logger.info(f"✓ Text summary saved to {args.output_txt}")
    
    # Exit with appropriate code
    sys.exit(0 if report['validation_status'] == 'PASS' else 1)


if __name__ == '__main__':
    main()
