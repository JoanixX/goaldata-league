"""
Deterministic player-position resolution (no RNG)
=================================================

Background
----------
``enrich_processed_features.py`` previously assigned a *random* position group
(``rng.choice(["GK","DEF","MID","FW"])``) whenever the raw position text could
not be parsed, and persisted that random value into the ``position`` column.
That mislabelled real players (e.g. Thibaut Courtois, a goalkeeper, became
"FW") and, because the goal-allocation model weights scoring by position, it
made goalkeepers "score" like forwards (Courtois: 3.26 goals/90 in 2019-2020).

This module replaces that with a deterministic, evidence-based resolver. Order
of authority (most reliable first):

1. **Goalkeeper evidence** — a player who appears in ``goalkeeper_stats`` with
   observed saves is a goalkeeper. This is the strongest signal available and
   fixes the catastrophic "keeper scores like a striker" case regardless of what
   the (possibly randomised) ``position`` text says.
2. **Position text parsing** — parse the raw position string with *first-token
   priority* so FBref compound codes are read correctly (``"FWMF"`` -> FW, not
   MID; ``"MFFW"`` -> MID). Single words and clean codes are handled too.
3. **Behavioural fallback** — for rows still unresolved (missing/garbage text and
   no GK evidence), classify from season aggregates: a high defensive rate
   (tackles+interceptions per 90) -> DEF; a high attacking rate
   (shots+goals per 90) -> FW; otherwise MID (the modal outfield role).

No random numbers are used anywhere, so the result is reproducible.
"""
from __future__ import annotations

import unicodedata
from pathlib import Path

import pandas as pd

CANONICAL_GROUPS = ("GK", "DEF", "MID", "FW")

_NULL_TOKENS = {"", "NULL", "NA", "NAN", "NONE", "UNKNOWN", "UNK"}

# 3-char position tokens -> canonical group (FBref words/codes and Opta codes).
_TOKEN3_MAP = {
    "DEF": "DEF", "MID": "MID", "FWD": "FW", "FOR": "FW", "GKP": "GK",
    "ATT": "FW", "STR": "FW", "WIN": "FW",
    "DMF": "MID", "CMF": "MID", "AMF": "MID", "LMF": "MID", "RMF": "MID",
}
# 2-char position tokens -> canonical group, in FBref/Opta notation.
_TOKEN_MAP = {
    "GK": "GK",
    "DF": "DEF", "CB": "DEF", "LB": "DEF", "RB": "DEF", "WB": "DEF",
    "MF": "MID", "DM": "MID", "CM": "MID", "AM": "MID", "LM": "MID", "RM": "MID",
    "FW": "FW", "ST": "FW", "CF": "FW", "LW": "FW", "RW": "FW", "SS": "FW",
}

# Whole-word fallbacks (when the text is spelled out rather than coded).
_WORD_RULES = (
    (("goalkeep", "keeper", "portero", "goalie"), "GK"),
    (("defen", "back", "centre-back", "center-back", "full-back", "fullback"), "DEF"),
    (("midfield", "mediocamp"), "MID"),
    (("forward", "striker", "winger", "attack", "delanter"), "FW"),
)


def canonical_from_text(value: object) -> str:
    """Parse a raw position string into a canonical group, first-token priority.

    Returns one of ``GK/DEF/MID/FW`` or ``"UNK"`` when nothing can be inferred.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "UNK"
    text = str(value).strip().upper()
    if text in _NULL_TOKENS:
        return "UNK"

    # 1) First-token priority (handles compound codes like FWMF). Scan left to
    #    right; at each position try a 3-char token then a 2-char token, so the
    #    primary (left-most) listed position wins.
    compact = "".join(ch for ch in text if ch.isalpha())
    for i in range(len(compact)):
        tri = compact[i:i + 3]
        if tri in _TOKEN3_MAP:
            return _TOKEN3_MAP[tri]
        duo = compact[i:i + 2]
        if duo in _TOKEN_MAP:
            return _TOKEN_MAP[duo]

    # 2) Spelled-out words.
    low = text.lower()
    for needles, group in _WORD_RULES:
        if any(n in low for n in needles):
            return group

    return "UNK"


def _canonical_name(value: object) -> str:
    """NFKD ASCII-fold + lower-case so 'Luka Modrić' == 'Luka Modric'."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().split())


def load_fbref_position_map(path: str | Path) -> dict[str, str]:
    """Authoritative canonical_name -> position_group map from the real FBref file.

    P1-2 (real where it exists): the raw FBref top-5-league file carries real
    positions (column ``Pos``), so it overrides the possibly-randomised stored
    ``position`` for the players it covers (e.g. Modric -> MID, Thiago Silva ->
    DEF). Players not present fall back to the deterministic logic below.
    """
    path = Path(path)
    if not path.exists():
        return {}
    df = pd.read_csv(path, sep=";", encoding="latin1", usecols=lambda c: c in {"Player", "Pos"})
    mapping: dict[str, str] = {}
    for name, pos in zip(df["Player"], df["Pos"]):
        key = _canonical_name(name)
        group = canonical_from_text(pos)
        if key and group != "UNK":
            # First occurrence wins; rare same-name collisions are left as-is.
            mapping.setdefault(key, group)
    return mapping


def _map_from_pairs(names, positions) -> dict[str, str]:
    """canonical_name -> most frequent canonical group across (name, pos) pairs."""
    from collections import Counter
    buckets: dict[str, Counter] = {}
    for nm, ps in zip(names, positions):
        key = _canonical_name(nm)
        grp = canonical_from_text(ps)
        if key and grp != "UNK":
            buckets.setdefault(key, Counter())[grp] += 1
    return {k: c.most_common(1)[0][0] for k, c in buckets.items()}


def load_multiseason_position_map(path: str | Path) -> dict[str, str]:
    """Position map from the multi-season FBref file produced by ingest_real_player_data."""
    path = Path(path)
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if "player_name" not in df.columns or "pos" not in df.columns:
        return {}
    return _map_from_pairs(df["player_name"], df["pos"])


def build_position_map(base_dir: str | Path) -> dict[str, str]:
    """Authoritative real-position map combining all available FBref sources.

    The 2021-2022 committed file is the base; the multi-season ingestion (if
    present) extends and takes priority, widening coverage to players missing
    from the single-season file (e.g. Modric).
    """
    base = Path(base_dir)
    combined = load_fbref_position_map(base / "data" / "raw" / "2021-2022 Football Player Stats.csv")
    combined.update(load_multiseason_position_map(base / "data" / "raw" / "fbref_big5_multiseason.csv"))
    return combined


def _behaviour_group(row: pd.Series) -> str:
    """Last-resort classifier from season aggregates (no text, no GK evidence)."""
    def _num(col: str) -> float:
        val = pd.to_numeric(row.get(col), errors="coerce")
        return 0.0 if pd.isna(val) else float(val)

    def per90(total_col: str) -> float:
        minutes = _num("minutes_played")
        return (_num(total_col) / minutes * 90.0) if minutes > 0 else 0.0

    defensive = per90("tackles") + per90("interceptions")
    attacking = per90("shots") + per90("goals")
    if attacking >= 1.0 and attacking >= defensive:
        return "FW"
    if defensive >= 1.5 and defensive > attacking:
        return "DEF"
    return "MID"


def resolve_position_groups(
    players: pd.DataFrame,
    goalkeeper_stats: pd.DataFrame | None = None,
    player_season: pd.DataFrame | None = None,
    name_position_map: dict[str, str] | None = None,
) -> pd.Series:
    """Return an authoritative ``position_group`` Series aligned to ``players``.

    Parameters
    ----------
    players : must contain ``player_id`` and a raw ``position`` column.
    goalkeeper_stats : optional; players appearing here (with saves > 0) are GK.
    player_season : optional; used for the behavioural fallback (aggregated to
        one row per ``player_id``).
    """
    result = players["position"].map(canonical_from_text)
    result.index = players.index

    # 0) Highest authority: real FBref position by canonical name (P1-2).
    matched_by_name = pd.Series(False, index=players.index)
    if name_position_map and "player_name" in players.columns:
        cn = players["player_name"].map(_canonical_name)
        mapped = cn.map(name_position_map)
        name_hit = mapped.notna()
        result = result.mask(name_hit.to_numpy(), mapped)
        matched_by_name = name_hit

    # 1) Goalkeeper evidence (for players not already fixed by the real FBref map).
    if goalkeeper_stats is not None and not goalkeeper_stats.empty and "player_id" in goalkeeper_stats.columns:
        gk = goalkeeper_stats
        if "saves" in gk.columns:
            gk = gk[pd.to_numeric(gk["saves"], errors="coerce").fillna(0) > 0]
        gk_ids = set(gk["player_id"].astype(str))
        is_gk = players["player_id"].astype(str).isin(gk_ids) & (~matched_by_name)
        result = result.mask(is_gk.to_numpy(), "GK")

    # 3) Behavioural fallback for whatever is still UNK.
    unresolved = result.eq("UNK")
    if unresolved.any() and player_season is not None and not player_season.empty:
        agg_cols = [c for c in ["minutes_played", "tackles", "interceptions", "shots", "goals"] if c in player_season.columns]
        if "player_id" in player_season.columns and agg_cols:
            agg = player_season.groupby("player_id")[agg_cols].sum(numeric_only=True)
            joined = players.loc[unresolved, ["player_id"]].join(agg, on="player_id")
            behaviour = joined.apply(_behaviour_group, axis=1)
            result.loc[unresolved] = behaviour.reindex(result.loc[unresolved].index).fillna("MID")
    # Anything still UNK (no text, no GK, no stats) -> modal outfield role.
    result = result.mask(result.eq("UNK"), "MID")
    return result
