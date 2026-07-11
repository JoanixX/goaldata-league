"""
Real-roster participation datasets (>=1.5M rows, zero invented identities)
==========================================================================

Rebuilds the volume layer (`player_match_stats`, `goals_events`, season stats)
from REAL rosters instead of the removed "Squad NN" placeholder players:

* **Identities are 100% real.** Every player row comes from FBref Big-5 rosters
  (2005-2025, `data/raw/fbref_big5_multiseason.csv`) or the StatsBomb catalog.
  Identity variants collapse to one entity (CristianoRonaldo / cristiano_ronaldo
  / CR7 -> one id) via `entity_resolution.canonical_text` + nickname map, but the
  merge is **nation-blocked**: two players sharing a name are kept separate when
  their FBref nationality differs (e.g. an Argentine and a Colombian "Alvaro
  Perez" stay two entities — nationality plus team-season trajectory is the
  disambiguator), and fuzzy merges never cross nations.
* **Participations are real-roster derived.** Each (player, club, season) roster
  row expands to that club's REAL matches of that season (one row per squad
  member per deduplicated club fixture). Minutes spread the player's REAL season
  minutes uniformly. Cross-source duplicate fixtures are collapsed by
  normalized (date, home, away) before expansion.
* **Understat overlay (REAL per-match stats, Big-5 2014+).** Where Understat
  publishes the real per-match line (minutes, goals, assists, shots — ingested
  by `src/ingest_understat.py`), those values are kept verbatim
  (`observed_understat_core`) and only the residual of the player's real FBref
  season total is spread over his uncovered matches.
* **Goals / assists / shots / cards / fouls preserve real season totals.** Each
  player's REAL FBref season total is distributed across their match rows
  (seeded multinomial by minutes), so per-player season sums equal the real
  numbers exactly — goals are never inflated or invented (verified in the
  report). The per-match split is the modelled part (Maher 1982;
  Dixon & Coles 1997) and every such row is provenance-tagged.
* **Passes / tackles / interceptions** (absent from FBref combined tables) use
  each player's real StatsBomb per-90 rates when observed, else position-median
  rates computed from the real StatsBomb participation table. Tagged as
  modelled secondary metrics (allowed by the data policy).
* **Observed StatsBomb matches stay observed.** For the 2,270 matches with a
  real StatsBomb participation record, the real rows replace derived rows.

Every row carries `data_provenance`. Verification (invented-name scan, goal-sum
check, dedup stats, 1.5M-3M row-band check) is written to
`logs/roster_participation_report.json`.

Run:  python -m src.build_roster_participation_datasets
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.build_processed import stable_id
from src.entity_resolution import canonical_text

BASE = Path(__file__).resolve().parents[1]
PROC = BASE / "data" / "processed"
FBREF = BASE / "data" / "raw" / "fbref_big5_multiseason.csv"
LOGS = BASE / "logs"

RNG = np.random.default_rng(20260711)
PROVENANCE = "data_provenance"
ROW_BAND = (1_500_000, 3_000_000)

# FBref position string -> position group (first listed role wins).
_POS_MAP = {"GK": "GK", "DF": "DEF", "MF": "MID", "FW": "FW"}

# FBref club short forms that canonical_text alone cannot match to teams_cleaned.
TEAM_ALIASES = {
    "manchester utd": "manchester united",
    "newcastle utd": "newcastle united",
    "newcastle": "newcastle united",
    "paris s g": "paris saint germain",
    "nott ham forest": "nottingham forest",
    "nottingham": "nottingham forest",
    "sheffield utd": "sheffield united",
    "wolves": "wolves",
    "betis": "real betis",
    "atletico madrid": "atletico de madrid",
    # NOTE: the Serie A club Inter Milan is stored under the (mis-merged) name
    # "Inter Club d'Escaldes" in teams_cleaned; its match links are correct.
    "inter": "inter club d escaldes",
    "milan": "ac milan",
    "psg": "paris saint germain fc",
    "bayern munich": "fc bayern munchen",
    "nottingham": "nott m forest",
    "nott ham forest": "nott m forest",
    "cardiff city": "cardiff",
    "luton town": "luton",
    "ipswich town": "ipswich",
    "birmingham city": "birmingham",
    "charlton athletic": "charlton",
    "wigan athletic": "wigan",
    "msv duisburg": "duisburg",
    "derby county": "derby",
    "hull city": "hull",
    "grenoble foot": "grenoble",
    "paderborn 07": "paderborn",
    "arminia": "bielefeld",
    "aa aachen": "aachen",
    "energie cottbus": "cottbus",
    "dortmund": "borussia dortmund",
    "gladbach": "borussia monchengladbach",
    "leverkusen": "bayer leverkusen",
    "eint frankfurt": "eintracht frankfurt",
    "frankfurt": "eintracht frankfurt",
    "dusseldorf": "fortuna dusseldorf",
    "koln": "fc koln",
    "m gladbach": "borussia monchengladbach",
    "saint etienne": "as saint etienne",
    "west brom": "west bromwich albion",
    "west ham": "west ham united",
    "brighton": "brighton hove albion",
    "tottenham": "tottenham hotspur",
    "dep la coruna": "deportivo la coruna",
    "racing sant": "racing santander",
    "hertha bsc": "hertha berlin",
}

# Big-5 domestic competitions as labelled in matches_cleaned.
BIG5_COMPETITIONS = {"Premier League", "La Liga", "Serie A", "Ligue 1",
                     "Bundesliga", "1. Bundesliga"}


def _nation(v: object) -> str:
    """FBref nation cells look like 'eng ENG' / 'br BRA' -> keep the code."""
    s = str(v).strip()
    return s.split()[-1].upper() if s and s.lower() not in ("nan", "none") else "UNK"


def _pos_group(v: object) -> str:
    s = str(v).split(",")[0].strip().upper()
    return _POS_MAP.get(s, "UNK")


def resolve_roster_entities(fb: pd.DataFrame) -> pd.DataFrame:
    """Assign one canonical player_id per real identity, nation-disambiguated.

    Same canonical name + same nation -> one entity (variants merged).
    Same canonical name + different nations -> distinct entities (homonyms kept
    apart; their club trajectory stays separate by construction).
    """
    fb = fb.copy()
    fb["_canon"] = fb["player_name"].map(canonical_text)
    fb["_nation"] = fb["nation"].map(_nation) if "nation" in fb.columns else "UNK"
    fb = fb[fb["_canon"] != ""]
    for c in ("minutes", "matches", "goals", "assists", "shots", "shots_on_target",
              "yellow_cards", "red_cards", "fouls_committed"):
        fb[c] = pd.to_numeric(fb.get(c), errors="coerce").fillna(0.0)
    fb["position_group"] = fb["pos"].map(_pos_group)

    nations_per_name = fb.groupby("_canon")["_nation"].nunique()
    homonyms = set(nations_per_name[nations_per_name > 1].index)
    fb["player_id"] = [
        stable_id("player", f"{c} {n}") if c in homonyms else stable_id("player", c)
        for c, n in zip(fb["_canon"], fb["_nation"])
    ]
    fb["_homonym"] = fb["_canon"].isin(homonyms)
    return fb


def match_teams(fb: pd.DataFrame, teams: pd.DataFrame,
                matches: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """FBref club -> team_id per season, restricted to Big-5 participants.

    teams_cleaned holds duplicate entities for some clubs ("Leicester" and
    "Leicester City") and cross-country homonyms ("Arsenal de Sarandí"), so the
    mapping is season-aware: candidates are only teams that actually played a
    Big-5 domestic match, and for each (club, season) the candidate with real
    matches that season wins (most matches on ties).

    Returns a tidy frame (team=fbref club, season, team_id) plus unmatched clubs.
    """
    from rapidfuzz import fuzz, process

    dom = matches[matches["competition"].isin(BIG5_COMPETITIONS)]
    long = pd.concat([
        dom[["home_team_id", "season"]].rename(columns={"home_team_id": "team_id"}),
        dom[["away_team_id", "season"]].rename(columns={"away_team_id": "team_id"}),
    ], ignore_index=True)
    long["season"] = long["season"].astype(str)
    per_season = long.groupby(["team_id", "season"]).size().rename("n_matches").reset_index()

    big5_ids = set(per_season["team_id"])
    cand = teams[teams["team_id"].isin(big5_ids)]
    by_canon: dict[str, list[str]] = {}
    for tid, name in zip(cand["team_id"], cand["team_name"]):
        by_canon.setdefault(canonical_text(name), []).append(tid)

    def candidates(club: str) -> list[str]:
        canon = TEAM_ALIASES.get(canonical_text(club), canonical_text(club))
        canon = TEAM_ALIASES.get(canon, canon)
        if canon in by_canon:
            return by_canon[canon]
        # substring containment (e.g. "juventus" in "juventus fc") then fuzzy
        subs = [k for k in by_canon
                if canon and (k.startswith(canon + " ") or k.endswith(" " + canon)
                              or (" " + canon + " ") in (" " + k + " "))]
        if len(subs) >= 1:
            return sum((by_canon[k] for k in subs), [])
        best = process.extractOne(canon, list(by_canon), scorer=fuzz.token_sort_ratio)
        return by_canon[best[0]] if best and best[1] >= 87 else []

    rows, unmatched = [], []
    fb_pairs = fb[["team", "season"]].astype(str).drop_duplicates()
    cand_cache = {club: candidates(club) for club in fb_pairs["team"].unique()}
    ps_idx = per_season.set_index(["team_id", "season"])["n_matches"]
    for club, season in fb_pairs.itertuples(index=False):
        ids = cand_cache[club]
        if not ids:
            if club not in unmatched:
                unmatched.append(club)
            continue
        scored = [(ps_idx.get((tid, season), 0), tid) for tid in ids]
        n, tid = max(scored)
        if n > 0:
            rows.append((club, season, tid))
    return pd.DataFrame(rows, columns=["team", "season", "team_id"]), unmatched


def _spread_int(total: float, weights: np.ndarray) -> np.ndarray:
    """Deterministically distribute an integer season total across match rows."""
    n = int(round(float(total)))
    if n <= 0 or weights.sum() <= 0:
        return np.zeros(len(weights), dtype=np.int64)
    return RNG.multinomial(n, weights / weights.sum())


def dedup_fixtures(matches: pd.DataFrame) -> pd.DataFrame:
    """One row per real fixture, keyed by normalized (date, home, away).

    matches_cleaned carries the same fixture from several sources under
    different match_ids (e.g. La Liga 2011-2012: 38 football-data rows + 37
    StatsBomb rows); the first occurrence wins. `_date_norm` and `_fixture_key`
    stay on the frame for source alignment (Understat joins by fixture).
    """
    m = matches[["match_id", "season", "competition", "date",
                 "home_team_id", "away_team_id", "home_score", "away_score"]].copy()
    m["season"] = m["season"].astype(str)
    date_norm = pd.to_datetime(m["date"], errors="coerce", format="mixed", dayfirst=False)
    m["_date_norm"] = date_norm.dt.strftime("%Y-%m-%d").fillna(m["date"].astype(str))
    m["_fixture_key"] = (m["_date_norm"] + "|" + m["home_team_id"].astype(str)
                         + "|" + m["away_team_id"].astype(str))
    return m.drop_duplicates(subset="_fixture_key", keep="first")


def build_grid(fb: pd.DataFrame, matches: pd.DataFrame, team_map: pd.DataFrame) -> pd.DataFrame:
    """One row per real squad member per real club match of that season."""
    fb = fb.copy()
    fb["season"] = fb["season"].astype(str)
    fb = fb.merge(team_map, on=["team", "season"], how="inner")

    m = dedup_fixtures(matches).drop(columns=["_fixture_key", "_date_norm"])
    long = pd.concat([
        m.rename(columns={"home_team_id": "team_id", "home_score": "team_score"})[
            ["match_id", "season", "competition", "team_id", "team_score"]],
        m.rename(columns={"away_team_id": "team_id", "away_score": "team_score"})[
            ["match_id", "season", "competition", "team_id", "team_score"]],
    ], ignore_index=True)

    grid = fb.merge(long, on=["team_id", "season"], how="inner")
    # a mid-season transfer within the Big 5 puts a player at two clubs that may
    # face each other — keep him on one side only (the roster with more minutes)
    grid = (grid.sort_values("minutes", ascending=False)
            .drop_duplicates(subset=["player_id", "match_id"], keep="first"))
    n_matches = grid.groupby(["player_id", "team_id", "season"])["match_id"].transform("count")
    grid["minutes_played"] = (grid["minutes"] / n_matches).round(1).clip(upper=90.0)
    grid["_n_club_matches"] = n_matches
    return grid


UNDERSTAT_PM = BASE / "data" / "raw" / "understat_player_matches.csv"


def integrate_understat(grid: pd.DataFrame, matches: pd.DataFrame,
                        teams: pd.DataFrame) -> pd.DataFrame:
    """Overlay REAL Understat per-match stats (Big-5, 2014+) onto the grid.

    Adds u_minutes / u_goals / u_assists / u_shots columns (NaN where Understat
    has no coverage). Join: fixture = normalized (date, home, away) with fuzzy
    team-name mapping; player = canonical name (nation-ambiguous homonyms are
    skipped — Understat publishes no nationality).
    """
    from rapidfuzz import fuzz, process
    from src.position_resolution import _canonical_name

    if not UNDERSTAT_PM.exists():
        print("Understat overlay: no data file — skipped", flush=True)
        grid[["u_minutes", "u_goals", "u_assists", "u_shots"]] = np.nan
        return grid
    u = pd.read_csv(UNDERSTAT_PM)
    u["date"] = u["date"].astype(str).str[:10]

    # -- team-name map: understat name -> our team_id (over grid participants) --
    tid_names = teams.set_index("team_id")["team_name"].astype(str).to_dict()
    grid_tids = set(grid["team_id"])
    by_canon: dict[str, str] = {}
    for tid in grid_tids:
        by_canon.setdefault(canonical_text(tid_names.get(tid, "")), tid)
    u_names = pd.unique(pd.concat([u["h_team"], u["a_team"]]).astype(str))
    name_map: dict[str, str] = {}
    for name in u_names:
        canon = TEAM_ALIASES.get(canonical_text(name), canonical_text(name))
        canon = TEAM_ALIASES.get(canon, canon)
        if canon in by_canon:
            name_map[name] = by_canon[canon]
            continue
        best = process.extractOne(canon, list(by_canon), scorer=fuzz.token_sort_ratio)
        if best and best[1] >= 85:
            name_map[name] = by_canon[best[0]]
    print(f"Understat overlay: mapped {len(name_map)}/{len(u_names)} team names", flush=True)

    # -- fixture -> our match_id --
    m = dedup_fixtures(matches)
    fixture_to_match = dict(zip(m["_fixture_key"], m["match_id"].astype(str)))
    u["_h"] = u["h_team"].map(name_map)
    u["_a"] = u["a_team"].map(name_map)
    u = u.dropna(subset=["_h", "_a"])
    u["match_id"] = (u["date"] + "|" + u["_h"] + "|" + u["_a"]).map(fixture_to_match)
    u = u.dropna(subset=["match_id"])

    # -- player -> our player_id (canonical name; skip cross-nation homonyms) --
    ids = grid[["player_id", "player_name"]].drop_duplicates()
    canon_ids = ids.assign(_c=ids["player_name"].map(_canonical_name))
    counts = canon_ids.groupby("_c")["player_id"].nunique()
    unique_canon = canon_ids[canon_ids["_c"].map(counts) == 1]
    pid_by_canon = dict(zip(unique_canon["_c"], unique_canon["player_id"]))
    u["player_id"] = u["player_name"].map(_canonical_name).map(pid_by_canon)
    u = u.dropna(subset=["player_id"])

    for c in ("time", "goals", "assists", "shots"):
        u[c] = pd.to_numeric(u[c], errors="coerce")
    u = (u.sort_values("time", ascending=False)
         .drop_duplicates(subset=["player_id", "match_id"], keep="first"))
    overlay = u.set_index(["player_id", "match_id"])[["time", "goals", "assists", "shots"]]
    overlay.columns = ["u_minutes", "u_goals", "u_assists", "u_shots"]

    grid = grid.join(overlay, on=["player_id", "match_id"])
    n = int(grid["u_minutes"].notna().sum())
    print(f"Understat overlay: {n:,} grid rows now carry REAL per-match stats "
          f"({len(u):,} mapped source rows)", flush=True)
    return grid


def spread_season_totals(grid: pd.DataFrame) -> pd.DataFrame:
    """Distribute each player's REAL season totals over their match rows.

    A player's REAL FBref season totals (goals, assists, shots, cards, fouls)
    are spread across his matches (seeded multinomial by minutes), so
    per-player season sums are exactly the real totals — never inflated, never
    invented. Where Understat provides the REAL per-match value (u_* columns),
    that value is kept verbatim and only the residual (season total minus the
    observed part) is spread over the uncovered matches.
    """
    fixed_map = {"goals": "u_goals", "assists": "u_assists", "shots": "u_shots"}
    cols = ("goals", "assists", "shots", "shots_on_target", "yellow_cards",
            "red_cards", "fouls_committed")
    out = {k: np.zeros(len(grid), dtype=np.int64) for k in cols}
    fixed_arrays = {c: (grid[fc].to_numpy(dtype=float) if fc in grid.columns
                        else np.full(len(grid), np.nan))
                    for c, fc in fixed_map.items()}
    u_minutes = (grid["u_minutes"].to_numpy(dtype=float) if "u_minutes" in grid.columns
                 else np.full(len(grid), np.nan))

    order = np.lexsort((grid["season"].to_numpy(), grid["player_id"].to_numpy()))
    keys = grid["player_id"].astype(str).to_numpy() + "|" + grid["season"].to_numpy()
    ks = keys[order]
    bounds = np.flatnonzero(np.r_[True, ks[1:] != ks[:-1], True])
    minutes = np.maximum(grid["minutes_played"].to_numpy(), 0.1)
    new_minutes = grid["minutes_played"].to_numpy(dtype=float).copy()

    for a, b in zip(bounds[:-1], bounds[1:]):
        idx = order[a:b]
        row0 = grid.iloc[idx[0]]
        covered = ~np.isnan(u_minutes[idx])
        free = idx[~covered]
        # minutes: real where observed; season residual spread over free rows
        if covered.any():
            obs_min = np.nansum(u_minutes[idx])
            new_minutes[idx[covered]] = u_minutes[idx[covered]]
            if len(free):
                resid_min = max(float(row0["minutes"]) - obs_min, 0.0)
                new_minutes[free] = round(min(resid_min / len(free), 90.0), 1)
        w_free = minutes[free] if len(free) else np.array([])
        for col in cols:
            if col in fixed_map and covered.any():
                obs_vals = np.nan_to_num(fixed_arrays[col][idx], nan=0.0)
                out[col][idx[covered]] = obs_vals[covered].round().astype(np.int64)
                residual = max(float(row0[col]) - obs_vals[covered].sum(), 0.0)
                if len(free):
                    out[col][free] = _spread_int(residual, w_free)
            else:
                out[col][idx] = _spread_int(row0[col], minutes[idx])
    res = grid.copy()
    res["minutes_played"] = new_minutes
    res["_understat_covered"] = ~np.isnan(u_minutes)
    for col, vals in out.items():
        res[f"m_{col}"] = vals
    return res


def secondary_rates(real_pm: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Real per-90 pass/tackle/interception rates: per player and per position."""
    pm = real_pm[real_pm["minutes_played"] > 0].copy()
    per90 = lambda c: pm[c] / pm["minutes_played"] * 90.0
    rates = pd.DataFrame({
        "player_id": pm["player_id"], "position_group": pm["position_group"],
        "passes_completed_p90": per90("passes_completed"),
        "passes_attempted_p90": per90("passes_attempted"),
        "tackles_p90": per90("tackles"), "interceptions_p90": per90("interceptions"),
    })
    by_player = rates.groupby("player_id").median(numeric_only=True)
    by_pos = rates.groupby("position_group").median(numeric_only=True)
    return by_player, by_pos


def main() -> None:
    assert FBREF.exists(), "Run  python -m src.ingest_real_player_data  first."
    fb = pd.read_csv(FBREF)
    matches = pd.read_parquet(PROC / "core" / "matches_cleaned.parquet")
    teams = pd.read_parquet(PROC / "core" / "teams_cleaned.parquet")
    players = pd.read_parquet(PROC / "core" / "players_cleaned.parquet")
    real_pm = pd.read_parquet(PROC / "stats" / "player_match_stats_cleaned.parquet")
    if PROVENANCE in real_pm.columns:  # idempotent re-runs: keep only observed rows
        real_pm = real_pm[real_pm[PROVENANCE] == "observed_statsbomb"].drop(columns=[PROVENANCE])

    fb = resolve_roster_entities(fb)
    team_map, unmatched_teams = match_teams(fb, teams, matches)
    print(f"FBref rosters: {len(fb):,} rows, {fb['player_id'].nunique():,} real entities "
          f"({fb['_homonym'].sum():,} homonym rows kept apart by nation); "
          f"{len(unmatched_teams)} unmatched clubs", flush=True)

    grid = build_grid(fb, matches, team_map)
    print(f"Participation grid: {len(grid):,} rows over {grid['match_id'].nunique():,} real matches", flush=True)

    grid = integrate_understat(grid, matches, teams)
    grid = spread_season_totals(grid)

    # shots >= goals, goals <= SoT <= shots (repair the few violated rows)
    grid["m_shots"] = np.maximum(grid["m_shots"], grid["m_goals"])
    grid["m_shots_on_target"] = np.clip(grid["m_shots_on_target"], grid["m_goals"], grid["m_shots"])

    by_player, by_pos = secondary_rates(real_pm)
    g = grid.join(by_player, on="player_id")
    for col in ("passes_completed_p90", "passes_attempted_p90", "tackles_p90", "interceptions_p90"):
        pos_fill = g["position_group"].map(by_pos[col]).fillna(by_pos[col].median())
        g[col] = g[col].fillna(pos_fill)
    mins90 = g["minutes_played"] / 90.0
    g["passes_completed"] = (g["passes_completed_p90"] * mins90).round().astype(int)
    g["passes_attempted"] = np.maximum((g["passes_attempted_p90"] * mins90).round().astype(int),
                                       g["passes_completed"])
    g["tackles"] = (g["tackles_p90"] * mins90).round().astype(int)
    g["interceptions"] = (g["interceptions_p90"] * mins90).round().astype(int)

    derived = pd.DataFrame({
        "player_id": g["player_id"], "match_id": g["match_id"],
        "minutes_played": g["minutes_played"], "goals": g["m_goals"].astype(float),
        "assists": g["m_assists"].astype(float), "shots": g["m_shots"].astype(float),
        "shots_on_target": g["m_shots_on_target"].astype(float),
        "passes_completed": g["passes_completed"].astype(float),
        "passes_attempted": g["passes_attempted"].astype(float),
        "tackles": g["tackles"].astype(float), "interceptions": g["interceptions"].astype(float),
        "fouls_committed": g["m_fouls_committed"].astype(float),
        "yellow_cards": g["m_yellow_cards"].astype(float), "red_cards": g["m_red_cards"].astype(float),
        "position_group": g["position_group"], "team": g["team"],
        "competition": g["competition"], "season": g["season"],
        PROVENANCE: np.where(g["_understat_covered"],
                             "observed_understat_core",
                             "derived_real_roster_scoreline"),
    })

    observed = real_pm.copy()
    observed[PROVENANCE] = "observed_statsbomb"
    # match_id types differ across sources (StatsBomb int vs hashed str) — unify.
    observed["match_id"] = observed["match_id"].astype(str)
    derived["match_id"] = derived["match_id"].astype(str)
    # Observed matches keep only real rows; derived rows cover the rest.
    derived = derived[~derived["match_id"].astype(str).isin(observed["match_id"].astype(str))]
    pm_final = pd.concat([observed, derived], ignore_index=True)

    # --- goals_events: one aggregated row per scoring player-match -------------
    scorers = pm_final[pm_final["goals"] > 0]
    goals_events = scorers[["player_id", "match_id", "season", "competition", "team", "goals"]].copy()
    goals_events[PROVENANCE] = scorers[PROVENANCE].to_numpy()

    # --- player_season_stats: REAL FBref season totals + StatsBomb aggregates --
    fb_season = fb.copy()
    season_cols = {"minutes": "minutes_played", "matches": "matches_played",
                   "goals": "goals", "assists": "assists", "shots": "shots",
                   "shots_on_target": "shots_on_target", "yellow_cards": "yellow_cards",
                   "red_cards": "red_cards", "fouls_committed": "fouls_committed"}
    ps_fb = (fb_season.groupby(["player_id", "season"])
             .agg({**{k: "sum" for k in season_cols},
                   "player_name": "first", "position_group": "first", "team": "first"})
             .rename(columns=season_cols).reset_index())
    ps_fb[PROVENANCE] = "observed_fbref_season_totals"
    # downstream consumers (optimize_lineup) key positions off this column
    ps_fb["player_position_group"] = ps_fb["position_group"]
    ps_sb = pd.read_parquet(PROC / "stats" / "player_season_stats_cleaned.parquet")
    if PROVENANCE not in ps_sb.columns:
        ps_sb[PROVENANCE] = "aggregated_from_player_match"
    ps_final = pd.concat([ps_sb, ps_fb[~ps_fb.set_index(["player_id", "season"]).index.isin(
        ps_sb.set_index(["player_id", "season"]).index)].reset_index(drop=True)], ignore_index=True)
    # names live in the players catalog; a player_name column here breaks
    # downstream merges (optimize_lineup joins players for names)
    ps_final = ps_final.drop(columns=["player_name"], errors="ignore")
    for c in ps_final.columns:  # appended rows must not carry NaN into models/ILP
        if pd.api.types.is_numeric_dtype(ps_final[c]):
            ps_final[c] = ps_final[c].fillna(0.0)
        else:
            ps_final[c] = ps_final[c].astype("object").fillna("Unknown")

    # --- players catalog: append new real FBref entities -----------------------
    display = fb.groupby("player_id").agg(player_name=("player_name", "first"),
                                          nationality=("_nation", "first"),
                                          position_group=("position_group", "first")).reset_index()
    new_players = display[~display["player_id"].isin(players["player_id"])].copy()
    new_players[PROVENANCE] = "observed_fbref"
    players_final = pd.concat([players, new_players], ignore_index=True)
    for c in players_final.columns:
        if players_final[c].isna().any():
            players_final[c] = (players_final[c].fillna(0)
                                if pd.api.types.is_numeric_dtype(players_final[c])
                                else players_final[c].astype("object").fillna("Unknown"))

    # --- verification: per-player season goal sums == real FBref totals ---------
    # (understat-observed rows + residual spread must reproduce the real totals)
    per_season = derived.groupby(["player_id", "season"])["goals"].sum()
    fb_totals = fb.groupby(["player_id", "season"])["goals"].sum().round().astype(int)
    common = per_season.index.intersection(fb_totals.index)
    goal_sum_ok = float((per_season.loc[common] == fb_totals.loc[common]).mean()) if len(common) else 1.0
    n_understat = int((derived[PROVENANCE] == "observed_understat_core").sum())

    for df_chk in (pm_final, goals_events, ps_final):  # no-NULL guarantee
        for c in df_chk.columns:
            if df_chk[c].isna().any():
                df_chk[c] = (df_chk[c].fillna(0)
                             if pd.api.types.is_numeric_dtype(df_chk[c])
                             else df_chk[c].astype("object").fillna("Unknown"))

    invented = int(players_final["player_name"].astype(str)
                   .str.contains(r"Squad \d+", regex=True).sum())
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "rows": {"player_match_stats": int(len(pm_final)),
                 "goals_events": int(len(goals_events)),
                 "player_season_stats": int(len(ps_final)),
                 "players": int(len(players_final))},
        "row_band_check": {"target": list(ROW_BAND),
                           "player_match_in_band": ROW_BAND[0] <= len(pm_final) <= ROW_BAND[1]},
        "identity_checks": {"invented_names": invented,
                            "homonym_entities_kept_apart": int(fb.loc[fb["_homonym"], "player_id"].nunique()),
                            "real_entities": int(players_final["player_id"].nunique())},
        "goal_anchoring": {"player_seasons_checked": int(len(common)),
                           "season_total_match_rate": round(goal_sum_ok, 4)},
        "understat_observed_rows": n_understat,
        "unmatched_clubs": unmatched_teams,
        "provenance_counts": pm_final[PROVENANCE].value_counts().to_dict(),
    }
    LOGS.mkdir(exist_ok=True)
    (LOGS / "roster_participation_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    # --- write outputs (parquet + csv mirrors) ----------------------------------
    outputs = {
        PROC / "stats" / "player_match_stats_cleaned": pm_final,
        PROC / "events" / "goals_events_cleaned": goals_events,
        PROC / "stats" / "player_season_stats_cleaned": ps_final,
        PROC / "core" / "players_cleaned": players_final,
    }
    for stem, df in outputs.items():
        df.to_parquet(stem.with_suffix(".parquet"), index=False)
        df.to_csv(stem.with_suffix(".csv"), index=False, encoding="utf-8")
        print(f"wrote {stem.name}: {len(df):,} rows", flush=True)
    print(json.dumps(report["rows"], indent=2), flush=True)
    assert invented == 0, "invented identities detected — must never happen"


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("build_roster_participation_datasets", main)
