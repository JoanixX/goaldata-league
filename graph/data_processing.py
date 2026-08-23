"""data_processing.py

Skeleton for turning raw data into processed data.
Expected functions:
 - process_lineups: normalizes lineups to a CSV with ['match_id','player_id','minutes_played']
 - process_events: normalizes events to a CSV with ['match_id','from_player','to_player','event_type','minute']

This file is a starting point; adapt the columns to the raw data at hand.
"""
import argparse
import pandas as pd


def process_lineups(input_path: str, output_path: str):
    """Read a raw CSV and write processed/lineups.csv with the minimum columns.
    The input is assumed to have at least: match_id, player_id, minutes_played
    Adapt to the actual data schema.
    """
    df = pd.read_csv(input_path)
    cols = ['match_id', 'player_id', 'minutes_played']
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {input_path}: {missing}")
    out = df[cols].copy()
    out.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(out)} rows)")


def process_events(input_path: str, output_path: str):
    """Normalize events to a CSV with columns: match_id, from_player, to_player, event_type, minute
    """
    df = pd.read_csv(input_path)
    cols = ['match_id', 'from_player', 'to_player', 'event_type', 'minute']
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {input_path}: {missing}")
    out = df[cols].copy()
    out.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(out)} rows)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simple raw -> processed processing')
    parser.add_argument('--lineups-in', help='Raw lineups CSV')
    parser.add_argument('--lineups-out', help='Processed lineups CSV')
    parser.add_argument('--events-in', help='Raw events CSV')
    parser.add_argument('--events-out', help='Processed events CSV')
    args = parser.parse_args()
    if args.lineups_in and args.lineups_out:
        process_lineups(args.lineups_in, args.lineups_out)
    if args.events_in and args.events_out:
        process_events(args.events_in, args.events_out)
