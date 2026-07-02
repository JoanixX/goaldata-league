"""data_processing.py

Esqueleto para procesar datos raw a processed.
Funciones esperadas:
 - process_lineups: normaliza lineups a CSV con ['match_id','player_id','minutes_played']
 - process_events: normaliza eventos a CSV con ['match_id','from_player','to_player','event_type','minute']

Este archivo es un punto de partida; adapta columnas según tu raw data.
"""
import argparse
import pandas as pd


def process_lineups(input_path: str, output_path: str):
    """Lee un CSV raw y genera processed/lineups.csv con columnas mínimas.
    Se asume que input tiene columnas al menos: match_id, player_id, minutes_played
    Adapta según tu esquema real de datos.
    """
    df = pd.read_csv(input_path)
    cols = ['match_id', 'player_id', 'minutes_played']
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en {input_path}: {missing}")
    out = df[cols].copy()
    out.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(out)} rows)")


def process_events(input_path: str, output_path: str):
    """Normaliza eventos a CSV con columnas: match_id, from_player, to_player, event_type, minute
    """
    df = pd.read_csv(input_path)
    cols = ['match_id', 'from_player', 'to_player', 'event_type', 'minute']
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en {input_path}: {missing}")
    out = df[cols].copy()
    out.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(out)} rows)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Procesamiento simple de raw -> processed')
    parser.add_argument('--lineups-in', help='CSV raw de lineups')
    parser.add_argument('--lineups-out', help='CSV processed de lineups')
    parser.add_argument('--events-in', help='CSV raw de eventos')
    parser.add_argument('--events-out', help='CSV processed de eventos')
    args = parser.parse_args()
    if args.lineups_in and args.lineups_out:
        process_lineups(args.lineups_in, args.lineups_out)
    if args.events_in and args.events_out:
        process_events(args.events_in, args.events_out)
