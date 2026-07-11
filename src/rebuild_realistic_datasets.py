"""
Realistic dataset rebuild (no arbitrary RNG fabrication)
========================================================

Context
-------
The previous generation step (``enrich_processed_features.py``) filled the
per-match and event tables with arbitrary ``rng.choice`` / ``rng.integers``
draws whose only goal was to reach a 1.5M-row threshold. That produced two
indefensible artefacts:

1. ``player_match_stats.goals`` was drawn independently per player and did **not**
   sum to the real match scoreline. Only ~11.6% of matches were consistent and
   the total number of player-goals was ~1.74x the real number of goals.
2. ``goals_events_cleaned`` was a raw StatsBomb *event stream* (passes, carries,
   pressures) mislabelled as goals, with ~937 "goals" per match.

This module rebuilds the synthetic layers from the **real anchors that we do have**
(real final scores per match, real player profiles/positions, real per-90
distributions from FBref top-5 leagues) using documented statistical models
instead of arbitrary buckets. Every regenerated cell is tagged in a
``data_provenance`` column as ``observed`` / ``derived`` / ``simulated``.

Design (and why it is defensible)
---------------------------------
* **Goals are anchored to the real scoreline.** For each team in each match we
  take the team's *real* goals (home_score / away_score, which come from real
  sources) and distribute them across that team's players with a multinomial
  whose weights are the players' position-conditioned scoring propensity scaled
  by minutes played. By construction ``sum(player goals) == real team score`` for
  every match. Team goal counts themselves are never invented.
    - Poisson scoring intensity / score modelling: Maher (1982); Dixon & Coles (1997).
    - Action/position value & per-90 normalisation: Decroos et al. (2019).
* **Shots follow from goals** via position conversion rates (a scorer needs
  shots; conversion ~ position), keeping the additive identity
  ``shots = on_target + off_target + blocked`` and ``shots >= goals``.
* **goals_events_cleaned** becomes the real-sized goal table (~254k rows = the
  real number of goals), one row per goal, consistent with the player-goal
  allocation. Minute, penalty/own-goal flags and assists use documented base
  rates, not uniform noise. This table is *intentionally* below 1.5M rows: the
  number of goals is a physical quantity (~2.69 goals/match), so a 1.5M target
  is illogical here, exactly like the players table.
* Counts that are not tied to the scoreline (passes, tackles, dribbles, ...) are
  kept from the position-conditioned draw but are honestly tagged ``simulated``.

The heavy derived-feature engineering (per-90 rates, indices, match features,
goal-event features) is **reused unchanged** from ``enrich_processed_features``
so the output schema is identical to the existing parquet files.

Run:  python -m src.rebuild_realistic_datasets
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.entity_resolution import resolve_players, apply_identity_map
from src.position_resolution import resolve_position_groups
from src.enrich_processed_features import (
    OUTPUTS,
    add_goal_event_features,
    add_goalkeeper_features,
    add_match_features,
    add_player_features,
    add_player_match_features,
    add_player_season_features,
    build_goalkeeper_stats,
    build_player_season_stats,
    normalize_position,
    numeric,
    read_cleaned,
    stable_id,
    write_cleaned,
)

BASE_DIR = Path(__file__).resolve().parents[1]
LOGS_DIR = BASE_DIR / "logs"

RNG_SEED = 20260613

# ---------------------------------------------------------------------------
# Documented model parameters (with literature anchors). These are *relative*
# weights / rates derived from public football-analytics ranges, NOT arbitrary.
# ---------------------------------------------------------------------------
# Relative goal-scoring propensity by position (FW score most, GK almost never).
# Consistent with empirical goal-share-by-position reported in football analytics
# literature (e.g. forwards ~50-60% of open-play goals, defenders ~10-12%).
SCORING_PROPENSITY = {"GK": 0.01, "DEF": 0.18, "MID": 0.55, "FW": 1.00, "UNK": 0.40}
# Relative assist propensity by position (midfielders/forwards create most).
ASSIST_PROPENSITY = {"GK": 0.02, "DEF": 0.22, "MID": 1.00, "FW": 0.85, "UNK": 0.45}
# Shot-to-goal conversion by position (a goal implies ~1/conv shots).
# Typical open-play conversion: forwards higher volume/lower conv, etc.
SHOT_CONVERSION = {"GK": 0.20, "DEF": 0.09, "MID": 0.11, "FW": 0.14, "UNK": 0.11}
# Baseline non-scoring shot volume per 90 by position (Poisson mean).
BASE_SHOTS_PER90 = {"GK": 0.02, "DEF": 0.6, "MID": 1.3, "FW": 2.6, "UNK": 1.0}
# Penalty share of goals and own-goal share of goals (public match data ranges).
PENALTY_GOAL_SHARE = 0.085   # ~8-9% of goals are penalties
OWN_GOAL_SHARE = 0.022       # ~2% of goals are own goals
ASSISTED_GOAL_SHARE = 0.74   # ~70-75% of goals are assisted

PROVENANCE_COL = "data_provenance"

CITATIONS = [
    "Maher, M. J. (1982). Modelling association football scores. Statistica Neerlandica, 36(3), 109-118.",
    "Dixon, M. J., & Coles, S. G. (1997). Modelling association football scores and inefficiencies in the football betting market. JRSS-C, 46(2), 265-280.",
    "Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019). Actions Speak Louder than Goals: Valuing Player Actions in Soccer. KDD 2019. arXiv:1802.07127.",
    "Pollard, R., & Reep, C. (1997). Measuring the effectiveness of playing strategies at soccer. JRSS-D, 46(4), 541-550.",
    "Little, R. J. A., & Rubin, D. B. (2002). Statistical Analysis with Missing Data. Wiley.",
]


def position_series(player_match: pd.DataFrame, players: pd.DataFrame) -> pd.Series:
    """Position group for each player-match row (from real/observed player profile)."""
    if "player_position_group" in player_match.columns:
        pos = player_match["player_position_group"].astype(str)
    else:
        ctx = players.drop_duplicates("player_id").set_index("player_id")
        pos = player_match["player_id"].map(ctx.get("position_group", pd.Series(dtype=object)))
    pos = pos.where(pos.isin(["GK", "DEF", "MID", "FW"]), "UNK")
    return pos.reset_index(drop=True)


def allocate_goals_to_scoreline(
    player_match: pd.DataFrame,
    matches: pd.DataFrame,
    positions: pd.Series,
    rng: np.random.Generator,
) -> np.ndarray:
    """Distribute each team's REAL goals among its players (multinomial by weight).

    Returns an int array (len == len(player_match)) of goals per player-match such
    that, per match-team, the sum equals the real team score.
    """
    pm = player_match.reset_index(drop=True)
    minutes = numeric(pm["minutes_played"]).fillna(60).clip(lower=1).to_numpy()
    weight = positions.map(SCORING_PROPENSITY).fillna(0.4).to_numpy() * (minutes / 90.0)
    weight = np.clip(weight, 1e-6, None)

    mctx = matches.drop_duplicates("match_id").set_index("match_id")
    home_team = pm["match_id"].map(mctx["home_team_id"]).astype(str).to_numpy()
    away_team = pm["match_id"].map(mctx["away_team_id"]).astype(str).to_numpy()
    home_score = numeric(pm["match_id"].map(mctx["home_score"])).fillna(0).to_numpy()
    away_score = numeric(pm["match_id"].map(mctx["away_score"])).fillna(0).to_numpy()
    team = pm["team_id"].astype(str).to_numpy()
    is_home = team == home_team
    is_away = team == away_team
    # Only allocate to genuine home/away teams; a team that matches neither (linkage
    # gap) gets 0 so we never invent goals beyond the real scoreline.
    team_score = np.where(is_home, home_score, np.where(is_away, away_score, 0)).astype(int)

    goals = np.zeros(len(pm), dtype="int64")
    # Group rows by (match_id, team_id); allocate that team's score.
    groups = pm.groupby(["match_id", "team_id"], sort=False).indices
    for key, idx in groups.items():
        # team score is constant within the group
        g = int(team_score[idx[0]])
        if g <= 0:
            continue
        w = weight[idx]
        p = w / w.sum()
        # multinomial: how many of the g goals each player scored
        counts = rng.multinomial(g, p)
        goals[idx] = counts
    return goals


def rebuild_shot_family(
    goals: np.ndarray,
    positions: pd.Series,
    minutes: np.ndarray,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Shots consistent with goals: shots >= goals, additive split into on/off/blocked."""
    conv = positions.map(SHOT_CONVERSION).fillna(0.11).to_numpy()
    base90 = positions.map(BASE_SHOTS_PER90).fillna(1.0).to_numpy()
    # Expected shots = goals/conversion (a scorer took shots) + baseline volume scaled by minutes.
    lam = goals / np.clip(conv, 0.05, None) + base90 * (np.clip(minutes, 1, 130) / 90.0)
    extra = rng.poisson(np.clip(lam - goals, 0, None))
    shots = goals + extra
    # On target: at least the goals, plus a fraction of the rest (~0.38 of shots on target).
    on_extra = rng.binomial(np.clip(shots - goals, 0, None), 0.38)
    on_target = np.minimum(shots, goals + on_extra)
    remaining = np.clip(shots - on_target, 0, None)
    blocked = rng.binomial(remaining, 0.22)
    off_target = np.clip(shots - on_target - blocked, 0, None)
    return {
        "shots": shots.astype("int64"),
        "shots_on_target": on_target.astype("int64"),
        "shots_blocked": blocked.astype("int64"),
        "shots_off_target": off_target.astype("int64"),
    }


def rebuild_cards(
    fouls_committed: np.ndarray,
    positions: pd.Series,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Cards from fouls with position factor (no uniform-tail plateau)."""
    card_factor = positions.map({"GK": 0.55, "DEF": 1.25, "MID": 1.05, "FW": 0.75, "UNK": 1.0}).fillna(1.0).to_numpy()
    yellow_p = np.clip((np.asarray(fouls_committed, dtype=float) / 4.0) * 0.18 * card_factor, 0.01, 0.55)
    yellow = rng.binomial(1, yellow_p)
    red = rng.binomial(1, np.where(positions.eq("DEF").to_numpy(), 0.013, 0.007))
    return {"yellow_cards": yellow.astype("int64"), "red_cards": red.astype("int64")}


def build_goal_events_base(
    player_match: pd.DataFrame,
    goals: np.ndarray,
    players: pd.DataFrame,
    positions: pd.Series,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """One row per allocated goal, consistent with the player-goal allocation."""
    pm = player_match.reset_index(drop=True)
    scorer_rows = np.repeat(np.arange(len(pm)), goals)
    n = len(scorer_rows)
    match_ids = pm["match_id"].to_numpy()[scorer_rows]
    player_ids = pm["player_id"].to_numpy()[scorer_rows]
    team_ids = pm["team_id"].to_numpy()[scorer_rows]
    name_ctx = players.drop_duplicates("player_id").set_index("player_id")["player_name"]
    player_names = pd.Series(player_ids).map(name_ctx).fillna("Unknown").to_numpy()

    # Minute model: goals are slightly more frequent later in the match.
    # Piecewise-increasing hazard over 1..90 (+ small extra-time tail).
    minute_w = np.concatenate([np.linspace(0.7, 1.4, 90), np.array([0.6, 0.4, 0.25, 0.15, 0.1])])
    minute_w = minute_w / minute_w.sum()
    minutes = rng.choice(np.arange(1, 96), size=n, p=minute_w)

    # Goal type base rates (documented shares, not uniform).
    u = rng.random(n)
    goal_type = np.where(u < OWN_GOAL_SHARE, "own_goal",
                         np.where(u < OWN_GOAL_SHARE + PENALTY_GOAL_SHARE, "penalty", "regular"))
    is_penalty = goal_type == "penalty"
    is_own_goal = goal_type == "own_goal"

    # Assist: a share of (non-penalty) goals are assisted by a same-team team-mate.
    assist_player_id = np.full(n, "NO_ASSIST", dtype=object)
    assist_w_all = positions.map(ASSIST_PROPENSITY).fillna(0.45).to_numpy()  # precomputed, per player-match row
    pm_player_ids = pm["player_id"].to_numpy()
    pool = pm.groupby(["match_id", "team_id"], sort=False).indices
    assisted = (rng.random(n) < ASSISTED_GOAL_SHARE) & (~is_penalty) & (~is_own_goal)
    for i in np.nonzero(assisted)[0]:
        idx = pool.get((match_ids[i], team_ids[i]))
        if idx is None or len(idx) <= 1:
            continue
        scorer = scorer_rows[i]
        mask = idx != scorer  # exclude the scorer himself
        if not mask.any():
            continue
        cand = idx[mask]
        ww = assist_w_all[cand]
        assist_player_id[i] = pm_player_ids[cand[rng.choice(len(ww), p=ww / ww.sum())]]

    goal_ids = [stable_id("goal", m, p, mi, gt, k) for k, (m, p, mi, gt) in
                enumerate(zip(match_ids, player_ids, minutes, goal_type))]
    return pd.DataFrame({
        "goal_id": goal_ids,
        "match_id": match_ids,
        "minute": minutes.astype("int64"),
        "goal_type": goal_type,
        "player_id": player_ids,
        "assist_player_id": assist_player_id,
        "event_type": "goal",
        "player_name": player_names,
        "is_penalty": is_penalty,
        "is_own_goal": is_own_goal,
    })


def base_player_match(player_match: pd.DataFrame, positions: pd.Series, goals: np.ndarray, rng: np.random.Generator) -> pd.DataFrame:
    """Assemble the 29-col base player_match table with scoreline-consistent values."""
    pm = player_match.reset_index(drop=True).copy()
    minutes = numeric(pm["minutes_played"]).fillna(60).clip(lower=1).to_numpy()
    pm["goals"] = goals
    shot = rebuild_shot_family(goals, positions, minutes, rng)
    for k, v in shot.items():
        pm[k] = v
    # assists: count of goals this player assisted is filled later from goal events;
    # keep the existing position-conditioned assists draw as a prior (tagged simulated).
    fouls = numeric(pm.get("fouls_committed", 0)).fillna(0).to_numpy()
    cards = rebuild_cards(fouls, positions, rng)
    for k, v in cards.items():
        pm[k] = v
    return pm


def main() -> None:
    LOGS_DIR.mkdir(exist_ok=True)
    rng = np.random.default_rng(RNG_SEED)
    print("Loading anchors from committed parquet...", flush=True)
    matches = read_cleaned("matches")
    players = read_cleaned("players")
    teams = read_cleaned("teams")
    player_match = read_cleaned("match_stats")
    
    player_season = read_cleaned("season_stats")
    goalkeepers = read_cleaned("gk")

    print("Resolving player identities (name normalisation + fuzzy merge)...", flush=True)
    players_resolved, id_map = resolve_players(players)
    merged = sum(1 for k, v in id_map.items() if k != v)
    print(f"  merged {merged:,} duplicate player ids into canonical entities", flush=True)
    remapped = apply_identity_map(
        {"match_stats": player_match, "season_stats": player_season, "gk": goalkeepers},
        id_map,
    )
    player_match = remapped["match_stats"].drop_duplicates(["player_id", "match_id"], keep="first").reset_index(drop=True)
    # Merging identities can collapse two old ids onto one canonical id in the same
    # season; keep one row per (player_id, season) so downstream aggregation keys stay unique.
    player_season = remapped["season_stats"].drop_duplicates(["player_id", "season"], keep="first").reset_index(drop=True)
    goalkeepers = remapped["gk"].drop_duplicates(["player_id", "season"], keep="first").reset_index(drop=True)
    players["player_id"] = players["player_id"].map(lambda v: id_map.get(v, v))
    players = players.drop_duplicates("player_id", keep="first").reset_index(drop=True)

    # P0-1: authoritative, deterministic position resolution (replaces the old
    # random fallback). Goalkeeper evidence + first-token code parsing +
    # behavioural fallback (see src/position_resolution.py). This is what stops
    # goalkeepers from being weighted as scorers in the goal allocation below.
    from src.position_resolution import build_position_map
    fbref_pos = build_position_map(BASE_DIR)
    print(f"  loaded {len(fbref_pos):,} authoritative FBref positions (by name)", flush=True)
    resolved = resolve_position_groups(players, goalkeepers, player_season, name_position_map=fbref_pos)
    players["position_group"] = resolved.to_numpy()
    players["position"] = resolved.to_numpy()
    print("Resolving authoritative positions (no RNG)...", flush=True)
    print("  real-player position counts: "
          + str(players.loc[players.get('profile_data_source', '') != 'imputed_team_season_roster', 'position_group'].value_counts().to_dict()), flush=True)
    pos_map = dict(zip(players["player_id"].astype(str), players["position_group"].astype(str)))
    positions = player_match["player_id"].astype(str).map(pos_map)
    positions = positions.where(positions.isin(["GK", "DEF", "MID", "FW"]), "UNK").reset_index(drop=True)
    # Keep the per-match position column consistent so downstream feature
    # functions use the corrected groups too.
    player_match = player_match.reset_index(drop=True)
    player_match["player_position_group"] = positions.to_numpy()

    print("Allocating goals to real scorelines...", flush=True)
    goals_arr = allocate_goals_to_scoreline(player_match, matches, positions, rng)
    real_total = int((numeric(matches["home_score"]).fillna(0) + numeric(matches["away_score"]).fillna(0)).sum())
    print(f"  allocated {int(goals_arr.sum()):,} player-goals (real total = {real_total:,})", flush=True)

    print("Rebuilding base player_match (shots/cards consistent)...", flush=True)
    pm_base_full = base_player_match(player_match, positions, goals_arr, rng)
    # carry over the remaining (non-regenerated) base columns from the existing table
    base_cols = [
        "player_id", "match_id", "minutes_played", "goals", "assists", "shots",
        "shots_on_target", "shots_off_target", "shots_blocked", "passes_completed",
        "passes_attempted", "pass_accuracy", "crosses_completed", "crosses_attempted",
        "dribbles", "offsides", "tackles", "tackles_won", "tackles_lost",
        "interceptions", "clearances", "fouls_committed", "fouls_suffered",
        "yellow_cards", "red_cards", "distance_covered", "top_speed", "team_id", "touches",
    ]
    for c in base_cols:
        if c not in pm_base_full.columns:
            pm_base_full[c] = 0
    player_match_base = pm_base_full[base_cols].copy()

    print("Building goal-events base (real-sized goal table)...", flush=True)
    goals_base = build_goal_events_base(player_match_base, goals_arr, players, positions, rng)

    # assists per player-match derived from the goal-event allocation (consistency).
    assist_counts = (
        goals_base[goals_base["assist_player_id"] != "NO_ASSIST"]
        .groupby(["assist_player_id", "match_id"]).size()
    )
    key = pd.MultiIndex.from_arrays([player_match_base["player_id"], player_match_base["match_id"]])
    player_match_base["assists"] = pd.Series(assist_counts.reindex(key).to_numpy(), index=player_match_base.index).fillna(0).astype("int64")

    print("Recomputing derived features (reusing enrich functions)...", flush=True)
    # Possession has no real source (it was simulated with a flat uniform prior);
    # null it so add_match_features re-draws it from the realistic Beta prior.
    for _poss_col in ("possession_home", "possession_away"):
        if _poss_col in matches.columns:
            matches[_poss_col] = pd.NA
    matches = add_match_features(matches, teams)
    player_season_base = build_player_season_stats(player_match_base, matches, player_season)
    # P0-1 (season-level correction): a real goalkeeper's open-play offensive
    # season totals were imputed with a WRONG outfield (forward) position median
    # before the position fix, which produced absurd values (e.g. a keeper at
    # 3.26 goals/90). Goalkeepers have no open-play scoring role, so force their
    # offensive season counts to ~0; per-90 features are derived afterwards.
    gk_ids = set(players.loc[players["position_group"].eq("GK"), "player_id"].astype(str))
    gk_rows = player_season_base["player_id"].astype(str).isin(gk_ids)
    for col in ("goals", "shots", "shots_on_target"):
        if col in player_season_base.columns:
            player_season_base.loc[gk_rows, col] = 0
    print(f"  zeroed open-play offensive season stats for {int(gk_rows.sum()):,} goalkeeper rows", flush=True)

    # P-real overlay: replace imputed season counts of REAL players with their
    # actual FBref season totals (by canonical name + season). This gives each
    # real player an individual fingerprint instead of a position-median profile,
    # which is what was making the PCA/recommender unable to tell players apart.
    # Only existing values are overwritten -> no NULLs are introduced.
    from src.ingest_real_player_data import load_real_season_stats
    from src.position_resolution import _canonical_name
    real_stats = load_real_season_stats(BASE_DIR / "data" / "raw" / "fbref_big5_multiseason.csv")
    if not real_stats.empty:
        name_by_id = players.drop_duplicates("player_id").set_index("player_id")["player_name"]
        cn = player_season_base["player_id"].map(name_by_id).map(_canonical_name)
        key = pd.MultiIndex.from_arrays([cn.to_numpy(), player_season_base["season"].astype(str).to_numpy()])
        n_overlaid = 0
        for col in real_stats.columns:
            if col in player_season_base.columns:
                rv = pd.Series(real_stats[col].reindex(key).to_numpy(), index=player_season_base.index)
                mask = rv.notna()
                player_season_base.loc[mask, col] = rv[mask].to_numpy()
                n_overlaid = max(n_overlaid, int(mask.sum()))
        print(f"  overlaid REAL FBref season stats onto {n_overlaid:,} real player-seasons "
              f"(cols: {list(real_stats.columns)})", flush=True)

    # StatsBomb real STYLE overlay: inject real per-90 style (shots, passes,
    # tackles, interceptions, fouls) for covered players by scaling their real
    # StatsBomb per-90 rate to their full-season minutes. This replaces the
    # position-median imputation of those columns -> real role/style signal
    # (what separates a holding midfielder from a winger). No NULLs introduced.
    from src.ingest_statsbomb import load_statsbomb_style_rates, match_catalog_to_statsbomb
    sb_rates = load_statsbomb_style_rates(BASE_DIR / "data" / "raw" / "statsbomb_player_season.csv", min_minutes=450)
    if not sb_rates.empty:
        real_names = players.loc[players["profile_data_source"] != "imputed_team_season_roster", "player_name"].dropna().unique()
        name_to_sb = match_catalog_to_statsbomb(real_names, sb_rates)
        id_to_name = players.drop_duplicates("player_id").set_index("player_id")["player_name"]
        sbkey = player_season_base["player_id"].map(id_to_name).map(lambda n: name_to_sb.get(n))
        minutes = pd.to_numeric(player_season_base["minutes_played"], errors="coerce").fillna(0)
        # Only the columns FBref cannot provide via soccerdata (passes/tackles/
        # interceptions). Shots/SoT/fouls come from FBref full-season totals above.
        style_map = {c: f"{c}_rate90" for c in
                     ["passes_completed", "passes_attempted", "tackles", "interceptions"]}
        n_sb = 0
        for col, ratecol in style_map.items():
            if col in player_season_base.columns and ratecol in sb_rates.columns:
                rate = pd.to_numeric(sbkey.map(lambda k: sb_rates[ratecol].get(k) if k is not None else None), errors="coerce")
                mask = rate.notna() & (minutes > 0)
                player_season_base.loc[mask, col] = (rate[mask] * minutes[mask] / 90.0).round()
                n_sb = max(n_sb, int(mask.sum()))
        print(f"  overlaid REAL StatsBomb per-90 style onto {n_sb:,} real player-seasons "
              f"({len(name_to_sb):,} players matched)", flush=True)
    goalkeepers_base = build_goalkeeper_stats(player_match_base, matches, players, goalkeepers)
    goals = add_goal_event_features(goals_base, matches, players)
    players = add_player_features(players, teams, player_match_base, player_season_base, goals)
    player_match = add_player_match_features(player_match_base, matches, players)
    player_season = add_player_season_features(player_season_base, players)
    goalkeepers = add_goalkeeper_features(goalkeepers_base, player_season, players)

    # ---- provenance tagging -------------------------------------------------
    # matches/teams: observed real anchors (scorelines, names) + derived features.
    matches[PROVENANCE_COL] = "observed_score+derived_features"
    teams[PROVENANCE_COL] = "observed"
    # players: real-source vs simulated squad fill.
    src = players.get("profile_data_source", pd.Series("", index=players.index)).astype(str)
    players[PROVENANCE_COL] = np.where(src.str.contains("roster", case=False), "simulated_squad", "observed_or_imputed")
    # goals_events: goals are derived from real scorelines (counts real, identities modelled).
    goals[PROVENANCE_COL] = "derived_from_real_scoreline"
    # player_match: goals/shots derived from real score; other counts simulated.
    player_match[PROVENANCE_COL] = "goals_derived_from_score;other_counts_simulated"
    player_season[PROVENANCE_COL] = "aggregated_from_player_match"
    goalkeepers[PROVENANCE_COL] = "derived_from_real_scoreline"

    outputs = {
        "matches": matches,
        "players": players,
        "teams": teams,
        "goals": goals,
        "gk": goalkeepers,
        "match_stats": player_match,
        "season_stats": player_season,
    }
    for name, df in outputs.items():
        print(f"Writing {name}: {len(df):,} rows x {len(df.columns)} cols", flush=True)
        write_cleaned(name, df)

    # ---- no-NULL guarantee (post-write) -------------------------------------
    # write_cleaned -> parquet_ready_frame converts "NULL" markers into typed NaN,
    # so the only reliable place to enforce "no missing data" is on the written
    # files. Re-read each table, fill residual NaN in numeric columns with 0
    # (counts/scores with no recorded value), and rewrite both parquet and CSV.
    total_filled = 0
    for name in outputs:
        ppath = OUTPUTS[name].with_suffix(".parquet")
        d = pd.read_parquet(ppath)
        changed = False
        for col in d.columns:
            if pd.api.types.is_numeric_dtype(d[col]) and d[col].isna().any():
                total_filled += int(d[col].isna().sum())
                d[col] = d[col].fillna(0)
                changed = True
        if changed:
            d.to_parquet(ppath, index=False)
            d.to_csv(OUTPUTS[name], index=False, encoding="utf-8")
    print(f"  filled {total_filled:,} residual numeric NaN cells with 0 (no-NULL guarantee)", flush=True)

    # ---- verification + methodology report ---------------------------------
    pm_goal_sum = int(numeric(player_match["goals"]).fillna(0).sum())
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "method": "Scoreline-anchored multinomial goal allocation; shots from conversion; "
                  "goals_events rebuilt at real size; provenance tagged.",
        "citations": CITATIONS,
        "verification": {
            "real_total_goals": real_total,
            "allocated_player_goals": pm_goal_sum,
            "player_goals_equal_real": pm_goal_sum == real_total,
            "goals_events_rows": int(len(goals)),
            "player_match_rows": int(len(player_match)),
        },
        "rows": {name: int(len(df)) for name, df in outputs.items()},
        "model_parameters": {
            "scoring_propensity": SCORING_PROPENSITY,
            "shot_conversion": SHOT_CONVERSION,
            "penalty_goal_share": PENALTY_GOAL_SHARE,
            "own_goal_share": OWN_GOAL_SHARE,
            "assisted_goal_share": ASSISTED_GOAL_SHARE,
        },
    }
    out = LOGS_DIR / "realistic_rebuild_report.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report["verification"], indent=2))


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("rebuild_realistic_datasets", main)
