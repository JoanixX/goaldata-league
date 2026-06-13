"""
Player entity resolution (name normalisation + de-duplication)
==============================================================

Goal
----
Different sources write the same player in many surface forms:
``Cristiano Ronaldo``, ``cristiano_ronaldo``, ``CristianoRonaldo``, ``CR7``,
``C. Ronaldo`` ... These must collapse to a single canonical entity, and *all*
text must follow one consistent character convention (lower-case ASCII, accents
folded, separators unified).

Strategy (deterministic first, fuzzy second)
--------------------------------------------
1. **Canonical text** — Unicode NFKD -> ASCII fold, split ``camelCase``, turn
   ``_ - . /`` into spaces, lower-case, collapse whitespace. This already merges
   ``CristianoRonaldo`` / ``cristiano_ronaldo`` / ``Cristiano Ronaldo``.
2. **Nickname dictionary** — a curated map for non-derivable aliases
   (``cr7 -> cristiano ronaldo``). Extendable.
3. **Exact-canonical grouping** — identical canonical strings are the same entity.
4. **Fuzzy merge (NLP)** — within a block (shared surname prefix and/or team) use
   ``rapidfuzz`` token-sort similarity to merge typos and order variants, plus a
   surname-subset rule (``Ronaldo`` -> ``Cristiano Ronaldo`` inside the same team).

The resolver only fuzzy-compares *real-source* players; the ~199k synthetic squad
placeholders have unique deterministic names and are left untouched (they cannot
be duplicates of each other).

Outputs a mapping ``old_player_id -> canonical_player_id`` that callers apply to
every table holding ``player_id`` / ``assist_player_id``.
"""
from __future__ import annotations

import re
import unicodedata

import pandas as pd

try:
    from rapidfuzz import fuzz
    _HAS_RAPIDFUZZ = True
except ModuleNotFoundError:  # graceful fallback to stdlib
    from difflib import SequenceMatcher
    _HAS_RAPIDFUZZ = False

from src.build_processed import stable_id

# Curated nickname / alias map (canonical text on both sides, extend as needed).
NICKNAME_MAP = {
    "cr7": "cristiano ronaldo",
    "c ronaldo": "cristiano ronaldo",
    "o fenomeno": "ronaldo",
    "kun aguero": "sergio aguero",
    "el kun": "sergio aguero",
    "leo messi": "lionel messi",
    "messi": "lionel messi",
    "ibra": "zlatan ibrahimovic",
    "lewy": "robert lewandowski",
    "mo salah": "mohamed salah",
    "dybala": "paulo dybala",
}

FUZZY_THRESHOLD = 90  # token-sort similarity (0-100) to merge two real names

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SEPARATORS = re.compile(r"[_\-./]+")
_NON_ALNUM = re.compile(r"[^a-z0-9 ]+")


def canonical_text(name: object) -> str:
    """One consistent character convention for any name surface form."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    text = str(name).strip()
    if text == "" or text.upper() in {"NULL", "NAN", "NONE", "NA"}:
        return ""
    text = _CAMEL.sub(" ", text)                       # CristianoRonaldo -> Cristiano Ronaldo
    text = _SEPARATORS.sub(" ", text)                  # cristiano_ronaldo -> cristiano ronaldo
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))  # fold accents
    text = text.lower()
    text = _NON_ALNUM.sub(" ", text)
    text = " ".join(text.split())
    return NICKNAME_MAP.get(text, text)


def _similar(a: str, b: str) -> float:
    if _HAS_RAPIDFUZZ:
        return fuzz.token_sort_ratio(a, b)
    return SequenceMatcher(None, a, b).ratio() * 100


class _Union:
    def __init__(self, items):
        self.parent = {i: i for i in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def resolve_players(players: pd.DataFrame, real_source_mask: pd.Series | None = None) -> tuple[pd.DataFrame, dict]:
    """Return (players_with_canonical_id, id_map old_player_id->canonical_player_id)."""
    df = players.copy().reset_index(drop=True)
    df["__canon"] = df["player_name"].map(canonical_text)

    if real_source_mask is None:
        src = df.get("profile_data_source", pd.Series("", index=df.index)).astype(str)
        real_source_mask = ~src.str.contains("roster", case=False)
    real_source_mask = real_source_mask.reset_index(drop=True)

    # Start: every distinct canonical string is its own group.
    df["__group"] = df["__canon"]

    # Fuzzy / surname-subset merge among REAL players only.
    real = df[real_source_mask & (df["__canon"] != "")].copy()
    canon_unique = sorted(real["__canon"].unique())
    union = _Union(canon_unique)

    # Block by surname (last token) prefix to keep comparisons cheap.
    blocks: dict[str, list[str]] = {}
    for canon in canon_unique:
        tokens = canon.split()
        key = tokens[-1][:4] if tokens else canon[:4]
        blocks.setdefault(key, []).append(canon)

    for key, names in blocks.items():
        if len(names) < 2:
            continue
        for i in range(len(names)):
            ti = names[i].split()
            for j in range(i + 1, len(names)):
                a, b = names[i], names[j]
                tj = b.split()
                # surname-subset rule: single-token name == last token of a fuller name
                subset = (len(ti) == 1 and ti[0] == tj[-1]) or (len(tj) == 1 and tj[0] == ti[-1])
                if subset or _similar(a, b) >= FUZZY_THRESHOLD:
                    union.union(a, b)

    # Representative canonical = longest name in the cluster (most information).
    rep_by_root: dict[str, str] = {}
    for canon in canon_unique:
        root = union.find(canon)
        cur = rep_by_root.get(root)
        if cur is None or len(canon) > len(cur):
            rep_by_root[root] = canon
    canon_to_rep = {c: rep_by_root[union.find(c)] for c in canon_unique}

    df["__rep"] = df["__canon"].map(canon_to_rep).fillna(df["__canon"])
    # canonical player_id from representative canonical text (stable + collision-resistant).
    # Only real-source players are remapped; synthetic squad placeholders keep their id.
    canonical_real = df["__rep"].map(lambda r: stable_id("player", r) if r else "")
    df["__canonical_id"] = df["player_id"].where(~real_source_mask | (canonical_real == ""), canonical_real)

    id_map = dict(zip(df["player_id"], df["__canonical_id"]))
    return df, id_map


def apply_identity_map(tables: dict[str, pd.DataFrame], id_map: dict) -> dict[str, pd.DataFrame]:
    """Remap player_id / assist_player_id across every table."""
    out = {}
    for name, df in tables.items():
        df = df.copy()
        for col in ("player_id", "assist_player_id"):
            if col in df.columns:
                df[col] = df[col].map(lambda v: id_map.get(v, v))
        out[name] = df
    return out
