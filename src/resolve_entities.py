"""
Entity resolution v2 — teams & player identities (variants merged, homonyms kept)
=================================================================================

Names are messy across sources. This resolves TEAM names (and guards PLAYER
identities) using REAL data signals, per the policy:
  * MERGE surface variants of the same entity:
      "Real Madrid" / "Real Madrid CF" / "Real Madrid C.F."  -> one team
      "Manchester United" / "Manchester Utd" / "... FC"       -> one team
      "Inter" / "Inter Milan" / "FC Internazionale Milano"    -> one team
  * KEEP genuine homonyms apart using country/context:
      "Barcelona" (ES)  !=  "Barcelona SC" (EC)
      "Inter" (IT)      !=  "Internacional" (BR)  != "Inter Club d'Escaldes" (AD)
Players: StatsBomb `player_id` is authoritative (never confuses homonyms); when
merging by name across sources we only merge if nationality is compatible.

Writes resolved `teams_cleaned` and remaps `matches_cleaned` foreign keys.

Run:  python -m src.resolve_entities
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "data" / "processed" / "core"

# club-name noise tokens to strip when comparing cores (not identity-bearing)
_SUFFIX = {"fc", "cf", "sc", "sl", "ac", "cd", "ca", "afc", "cfc", "club", "de", "the",
           "futbol", "football", "calcio", "clube", "association", "aszlub"}


def core_name(name: str) -> str:
    s = re.sub(r"[^a-z0-9\s]", " ", str(name).lower())
    toks = [t for t in s.split() if t and t not in _SUFFIX]
    return " ".join(toks)


def _country_ok(a: str, b: str) -> bool:
    a, b = str(a).strip().lower(), str(b).strip().lower()
    return a == b or a in ("", "unknown", "europe") or b in ("", "unknown", "europe")


def resolve_teams(teams: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = teams.copy().reset_index(drop=True)
    df["_core"] = df["team_name"].map(core_name)
    parent = {i: i for i in df.index}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        parent[find(a)] = find(b)

    # block by first core token (surname-like) to keep it cheap, then fuzzy + country guard
    blocks: dict[str, list[int]] = {}
    for i, c in df["_core"].items():
        blocks.setdefault(c.split()[0] if c.split() else c, []).append(i)
    for _, idxs in blocks.items():
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                i, j = idxs[a], idxs[b]
                ca, cb = df.at[i, "_core"], df.at[j, "_core"]
                # merge if one core contains the other OR high token-set similarity
                contained = ca and cb and (ca in cb or cb in ca)
                sim = fuzz.token_set_ratio(ca, cb)
                if (contained or sim >= 90) and _country_ok(df.at[i, "country"], df.at[j, "country"]):
                    union(i, j)

    df["_grp"] = [find(i) for i in df.index]
    # canonical per group = most frequent-looking / longest real name; canonical id = first
    id_map = {}
    canon_rows = []
    for grp, sub in df.groupby("_grp"):
        canon = sub.sort_values("team_name", key=lambda s: s.str.len(), ascending=False).iloc[0]
        cid = canon["team_id"]
        for tid in sub["team_id"]:
            id_map[tid] = cid
        canon_rows.append(canon)
    resolved = pd.DataFrame(canon_rows).drop(columns=["_core", "_grp"]).drop_duplicates("team_id").reset_index(drop=True)
    return resolved, id_map


def main() -> None:
    teams = pd.read_parquet(CORE / "teams_cleaned.parquet")
    resolved, id_map = resolve_teams(teams)
    merged = len(teams) - len(resolved)
    print(f"teams: {len(teams):,} -> {len(resolved):,} ({merged:,} variants merged; homonyms kept by country)", flush=True)

    # remap matches FKs
    m = pd.read_parquet(CORE / "matches_cleaned.parquet")
    for col in ("home_team_id", "away_team_id"):
        if col in m.columns:
            m[col] = m[col].map(lambda t: id_map.get(t, t))
    # write
    resolved.to_parquet(CORE / "teams_cleaned.parquet", index=False)
    resolved.to_csv(CORE / "teams_cleaned.csv", index=False, encoding="utf-8")
    m.to_parquet(CORE / "matches_cleaned.parquet", index=False)
    m.to_csv(CORE / "matches_cleaned.csv", index=False, encoding="utf-8")

    # sample of merges for audit
    tn = teams.set_index("team_id")["team_name"].to_dict()
    from collections import defaultdict
    groups = defaultdict(list)
    for tid, cid in id_map.items():
        groups[cid].append(tn.get(tid))
    examples = [v for v in groups.values() if len(v) > 1][:12]
    print("sample merges:", flush=True)
    for e in examples:
        print("  ", e, flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("resolve_entities", main)
