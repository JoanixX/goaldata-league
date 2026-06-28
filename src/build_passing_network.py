"""
Passing-network graph + real xG from StatsBomb open data (P2-2 / P2-1)
=====================================================================

The similarity graph answers "who plays like whom". A **passing network** answers
the tactical question "who organises the play" — the graph the literature uses
for football (Pena & Touchette 2012; Buldu et al. 2019). For a single real match
it builds, per team:

  * nodes  = players on the pitch
  * edges  = completed passes between team-mates (weight = number of passes)
  * directed, weighted

and reports degree / betweenness / eigenvector centrality (the playmakers) plus
the team's pass count. It also extracts **real Expected Goals (xG)** per player
from the same StatsBomb shot events (P2-1 capability: real xG, not a proxy).

Data: StatsBomb Open Data (free, citable), fetched over HTTPS (no package needed).

Run:  python -m src.build_passing_network            # auto-pick a UCL final
      python -m src.build_passing_network --match-id 22912
"""
from __future__ import annotations

import argparse
import json
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

import networkx as nx
import pandas as pd

BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
ARTIFACTS = Path(__file__).resolve().parents[1] / "artifacts"


def _get(url: str):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def pick_match(competition_id: int = 16) -> int:
    """Pick a match for the given competition (default Champions League): a Final."""
    comps = _get(f"{BASE}/competitions.json")
    seasons = [c for c in comps if c["competition_id"] == competition_id]
    for c in seasons:
        matches = _get(f"{BASE}/matches/{competition_id}/{c['season_id']}.json")
        finals = [m for m in matches if str(m.get("competition_stage", {}).get("name", "")).lower() == "final"]
        chosen = finals or matches
        if chosen:
            m = chosen[0]
            print(f"Match: {m['home_team']['home_team_name']} vs {m['away_team']['away_team_name']} "
                  f"({m['match_date']}, {c['season_name']}) — id {m['match_id']}")
            return m["match_id"]
    raise RuntimeError("no match found")


def build_team_networks(events: list[dict]) -> dict[str, nx.DiGraph]:
    passes: dict[str, Counter] = defaultdict(Counter)
    for ev in events:
        if ev.get("type", {}).get("name") != "Pass":
            continue
        p = ev["pass"]
        if p.get("outcome"):  # outcome present => incomplete pass
            continue
        recipient = p.get("recipient", {}).get("name")
        passer = ev.get("player", {}).get("name")
        team = ev.get("team", {}).get("name")
        if passer and recipient and team:
            passes[team][(passer, recipient)] += 1
    graphs = {}
    for team, counter in passes.items():
        g = nx.DiGraph()
        for (a, b), w in counter.items():
            g.add_edge(a, b, weight=w)
        graphs[team] = g
    return graphs


def player_xg(events: list[dict]) -> pd.DataFrame:
    rows = []
    for ev in events:
        if ev.get("type", {}).get("name") != "Shot":
            continue
        rows.append({
            "player": ev.get("player", {}).get("name"),
            "team": ev.get("team", {}).get("name"),
            "xg": ev.get("shot", {}).get("statsbomb_xg", 0.0),
            "goal": ev.get("shot", {}).get("outcome", {}).get("name") == "Goal",
        })
    if not rows:
        return pd.DataFrame(columns=["player", "team", "xg", "goals", "shots"])
    df = pd.DataFrame(rows)
    return (df.groupby(["team", "player"])
              .agg(xg=("xg", "sum"), goals=("goal", "sum"), shots=("xg", "size"))
              .reset_index().sort_values("xg", ascending=False))


def centralities(g: nx.DiGraph) -> pd.DataFrame:
    if g.number_of_edges() == 0:
        return pd.DataFrame()
    btw = nx.betweenness_centrality(g, weight="weight")
    try:
        eig = nx.eigenvector_centrality_numpy(g, weight="weight")
    except Exception:
        eig = nx.eigenvector_centrality(g, weight="weight", max_iter=2000)
    wdeg = dict(g.degree(weight="weight"))
    df = pd.DataFrame({
        "player": list(g.nodes()),
        "weighted_degree": [wdeg[n] for n in g.nodes()],
        "betweenness": [btw[n] for n in g.nodes()],
        "eigenvector": [eig[n] for n in g.nodes()],
    })
    return df.sort_values("betweenness", ascending=False).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--match-id", type=int, default=None)
    parser.add_argument("--competition-id", type=int, default=16)
    args = parser.parse_args()

    match_id = args.match_id or pick_match(args.competition_id)
    events = _get(f"{BASE}/events/{match_id}.json")
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

    graphs = build_team_networks(events)
    for team, g in graphs.items():
        cent = centralities(g)
        print(f"\n=== {team}: passing network ({g.number_of_nodes()} players, "
              f"{int(sum(d['weight'] for _, _, d in g.edges(data=True)))} completed passes) ===")
        print("Top playmakers (betweenness):")
        print(cent.head(5).to_string(index=False))
        cent.to_csv(ARTIFACTS / f"passing_network_{team.replace(' ', '_')}_{match_id}.csv", index=False)

    xg = player_xg(events)
    if not xg.empty:
        print("\n=== Real per-player Expected Goals (StatsBomb xG) ===")
        print(xg.head(8).to_string(index=False))
        xg.to_csv(ARTIFACTS / f"player_xg_{match_id}.csv", index=False)


if __name__ == "__main__":
    main()
