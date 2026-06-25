"""build_graph.py

Construye grafos de co-participación (no dirigido) y de interacción (dirigido) a partir de CSVs processed.
Escribe grafos en formato gpickle.
"""
import argparse
import pandas as pd
import networkx as nx


def build_co_participation_graph(df_lineups: pd.DataFrame, min_shared_minutes: int = 1) -> nx.Graph:
    """Construye un grafo no dirigido donde cada par de jugadores que participaron en un mismo partido
    acumula peso = minutos compartidos (aproximación con min(minutes_i, minutes_j)).
    df_lineups debe tener columnas: match_id, player_id, minutes_played
    """
    G = nx.Graph()
    players = pd.unique(df_lineups['player_id'])
    G.add_nodes_from(players)
    for match_id, grp in df_lineups.groupby('match_id'):
        arr = grp[['player_id', 'minutes_played']].to_numpy()
        n = len(arr)
        for i in range(n):
            pid_i, min_i = arr[i]
            for j in range(i+1, n):
                pid_j, min_j = arr[j]
                shared = min(min_i, min_j)
                if shared < min_shared_minutes:
                    continue
                if G.has_edge(pid_i, pid_j):
                    G[pid_i][pid_j]['weight'] += shared
                    G[pid_i][pid_j]['matches'] += 1
                else:
                    G.add_edge(pid_i, pid_j, weight=shared, matches=1)
    return G


def build_interaction_graph(df_events: pd.DataFrame, event_types=None) -> nx.DiGraph:
    """Construye un grafo dirigido a partir de eventos donde existe from_player -> to_player.
    df_events debe tener: match_id, from_player, to_player, event_type, minute
    event_types: lista de tipos de evento a incluir (ej: ['assist','key_pass']). Si None usa todos.
    """
    if event_types is not None:
        df_events = df_events[df_events['event_type'].isin(event_types)]
    G = nx.DiGraph()
    # añadir nodos encontrados
    nodes = pd.unique(df_events[['from_player', 'to_player']].values.ravel())
    nodes = [n for n in nodes if pd.notnull(n)]
    G.add_nodes_from(nodes)
    for _, row in df_events.iterrows():
        a, b = row['from_player'], row['to_player']
        if pd.isnull(a) or pd.isnull(b):
            continue
        if G.has_edge(a, b):
            G[a][b]['weight'] += 1
            G[a][b]['events'] += 1
        else:
            G.add_edge(a, b, weight=1, events=1)
    return G


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Construir grafos desde CSVs processed')
    parser.add_argument('--lineups', help='CSV processed con match_id,player_id,minutes_played')
    parser.add_argument('--events', help='CSV processed con match_id,from_player,to_player,event_type,minute')
    parser.add_argument('--out-co', help='Salida .gpickle para grafo co-participation')
    parser.add_argument('--out-inter', help='Salida .gpickle para grafo interaction (directed)')
    parser.add_argument('--min-shared-minutes', type=int, default=1)
    parser.add_argument('--event-types', nargs='*', help='Tipos de evento a incluir (opcional)')
    args = parser.parse_args()

    if args.lineups and args.out_co:
        df_lineups = pd.read_csv(args.lineups)
        Gco = build_co_participation_graph(df_lineups, min_shared_minutes=args.min_shared_minutes)
        nx.write_gpickle(Gco, args.out_co)
        print(f'Wrote co-participation graph to {args.out_co} (nodes={Gco.number_of_nodes()}, edges={Gco.number_of_edges()})')

    if args.events and args.out_inter:
        df_events = pd.read_csv(args.events)
        Gint = build_interaction_graph(df_events, event_types=args.event_types)
        nx.write_gpickle(Gint, args.out_inter)
        print(f'Wrote interaction graph to {args.out_inter} (nodes={Gint.number_of_nodes()}, edges={Gint.number_of_edges()})')
