import pandas as pd
from graph.build_graph import build_co_participation_graph


def test_small():
    df = pd.DataFrame([
        {"match_id":1,"player_id":"A","minutes_played":90},
        {"match_id":1,"player_id":"B","minutes_played":90},
        {"match_id":2,"player_id":"A","minutes_played":45},
        {"match_id":2,"player_id":"C","minutes_played":45},
    ])
    G = build_co_participation_graph(df, min_shared_minutes=1)
    assert G.number_of_nodes() == 3
    assert G.number_of_edges() == 2

if __name__ == '__main__':
    test_small()
    print('test_small passed')
