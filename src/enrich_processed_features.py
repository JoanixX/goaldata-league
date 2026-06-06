from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from src.build_processed import OUTPUTS, parquet_ready_frame, stable_id
    from src.data_quality import write_quality_report
except ModuleNotFoundError:  # Allows `python src/enrich_processed_features.py`.
    import sys

    BASE_DIR_FOR_IMPORT = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(BASE_DIR_FOR_IMPORT))
    from src.build_processed import OUTPUTS, parquet_ready_frame, stable_id
    from src.data_quality import write_quality_report


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

RNG_SEED = 20260502
ROSTER_SIZE = 26
MATCH_PLAYERS_PER_TEAM = 12
MIN_PLAYER_MATCH_ROWS = 1_500_000

MISSING_STRINGS = {
    "",
    "NULL",
    "NAN",
    "NONE",
    "NA",
    "<NA>",
    "NOT_AVAILABLE_IN_SOURCE",
    "NO_ASSIST_OR_NOT_RECORDED",
}

COUNTRIES = [
    "Argentina",
    "Brazil",
    "Chile",
    "Colombia",
    "England",
    "France",
    "Germany",
    "Italy",
    "Netherlands",
    "Peru",
    "Portugal",
    "Spain",
    "Turkey",
    "Uruguay",
]

POSITION_BUCKETS = {
    "GK": {"age": (29, 4), "height": (190, 5), "weight": (84, 6), "speed": 30.5, "meters_per_min": 70},
    "DEF": {"age": (27, 4), "height": (183, 6), "weight": (78, 7), "speed": 32.0, "meters_per_min": 105},
    "MID": {"age": (26, 4), "height": (178, 6), "weight": (73, 7), "speed": 33.0, "meters_per_min": 120},
    "FW": {"age": (25, 4), "height": (181, 6), "weight": (76, 7), "speed": 34.5, "meters_per_min": 110},
}

ROSTER_POSITIONS = ["GK", "GK"] + ["DEF"] * 8 + ["MID"] * 8 + ["FW"] * 8
MATCH_POSITION_PLAN = ["GK"] + ["DEF"] * 4 + ["MID"] * 4 + ["FW"] * 3

BASE_PLAYER_MATCH_COLUMNS = [
    "player_id",
    "match_id",
    "minutes_played",
    "goals",
    "assists",
    "shots",
    "shots_on_target",
    "shots_off_target",
    "shots_blocked",
    "passes_completed",
    "passes_attempted",
    "pass_accuracy",
    "crosses_completed",
    "crosses_attempted",
    "dribbles",
    "offsides",
    "tackles",
    "tackles_won",
    "tackles_lost",
    "interceptions",
    "clearances",
    "fouls_committed",
    "fouls_suffered",
    "yellow_cards",
    "red_cards",
    "distance_covered",
    "top_speed",
    "team_id",
    "touches",
]

BASE_PLAYER_SEASON_COLUMNS = [
    "player_id",
    "season",
    "matches_played",
    "minutes_played",
    "goals",
    "assists",
    "shots",
    "shots_on_target",
    "passes_completed",
    "passes_attempted",
    "tackles",
    "interceptions",
    "fouls_committed",
    "yellow_cards",
    "red_cards",
]

BASE_GOALKEEPER_COLUMNS = [
    "player_id",
    "season",
    "saves",
    "goals_conceded",
    "clean_sheets",
    "penalty_saves",
    "punches",
]


def deterministic_int(*parts: object, modulo: int = 2**32) -> int:
    raw = "|".join(str(part) for part in parts)
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % modulo


def is_missing_value(value: object) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip().upper() in MISSING_STRINGS


def missing_mask(series: pd.Series) -> pd.Series:
    return series.map(is_missing_value)


def numeric(series: pd.Series | object, index: pd.Index | None = None) -> pd.Series:
    if isinstance(series, pd.Series):
        source = series
    else:
        source = pd.Series(series, index=index)
    return pd.to_numeric(source.mask(missing_mask(source), pd.NA), errors="coerce")


def safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    den = den.mask(den == 0)
    return num / den


def round_series(series: pd.Series, decimals: int = 6) -> pd.Series:
    out = series.replace([np.inf, -np.inf], np.nan).round(decimals).astype("object")
    out[pd.isna(out)] = "NULL"
    return out


def clip_round(values: np.ndarray | pd.Series, low: float, high: float, as_int: bool = True) -> np.ndarray:
    clipped = np.clip(np.asarray(values, dtype="float64"), low, high)
    if as_int:
        return np.rint(clipped).astype("int64")
    return np.round(clipped, 4)


def normalize_position(value: object) -> str:
    if is_missing_value(value):
        return "UNK"
    text = str(value).upper()
    if "GK" in text or "GOAL" in text or "KEEP" in text:
        return "GK"
    if any(token in text for token in ["DF", "DEF", "BACK", "CB", "LB", "RB"]):
        return "DEF"
    if any(token in text for token in ["MF", "MID", "CM", "DM", "AM", "LM", "RM"]):
        return "MID"
    if any(token in text for token in ["FW", "FOR", "ST", "CF", "LW", "RW", "ATT", "WING"]):
        return "FW"
    return "UNK"


def position_for_slot(slot: int) -> str:
    return ROSTER_POSITIONS[(slot - 1) % len(ROSTER_POSITIONS)]


def season_start_year(value: object) -> int:
    text = str(value)
    match = re.search(r"(19|20)\d{2}", text)
    return int(match.group(0)) if match else 2021


def slug(value: object) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")
    return text or "unknown"


def read_cleaned(name: str) -> pd.DataFrame:
    csv_path = OUTPUTS[name]
    parquet_path = csv_path.with_suffix(".parquet")
    if csv_path.exists():
        return pd.read_csv(csv_path, keep_default_na=False, low_memory=False)
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    return pd.DataFrame()


def write_cleaned(name: str, df: pd.DataFrame) -> None:
    csv_path = OUTPUTS[name]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    parquet_ready_frame(name, df).to_parquet(csv_path.with_suffix(".parquet"), index=False)


def fill_missing_column(df: pd.DataFrame, column: str, values: object) -> None:
    if column not in df.columns:
        df[column] = "NULL"
    mask = missing_mask(df[column])
    if not mask.any():
        return
    if isinstance(values, pd.Series):
        aligned = values.reindex(df.index)
    elif np.ndim(values) == 0:
        aligned = pd.Series([values] * len(df), index=df.index)
    else:
        aligned = pd.Series(values, index=df.index)
    df.loc[mask, column] = aligned.loc[mask].to_numpy()


def fill_existing_missing_profiles(players: pd.DataFrame, teams: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED + 1)
    players = players.copy()
    for column in ["player_id", "player_name", "nationality", "age", "height_cm", "weight_kg", "position", "team_id"]:
        if column not in players.columns:
            players[column] = "NULL"

    team_country = teams.set_index("team_id")["country"].to_dict() if {"team_id", "country"}.issubset(teams.columns) else {}
    team_name = teams.set_index("team_id")["team_name"].to_dict() if {"team_id", "team_name"}.issubset(teams.columns) else {}
    position_group = players["position"].map(normalize_position)
    unknown = position_group == "UNK"
    fallback_positions = np.array(["GK", "DEF", "MID", "FW"])
    fallback_probs = np.array([0.08, 0.32, 0.35, 0.25])
    generated_positions = rng.choice(fallback_positions, size=len(players), p=fallback_probs)
    position_group = position_group.mask(unknown, generated_positions)

    fill_missing_column(players, "position", position_group)
    country_values = players["team_id"].map(team_country).fillna(pd.Series(rng.choice(COUNTRIES, len(players)), index=players.index))
    fill_missing_column(players, "nationality", country_values)

    ages = np.zeros(len(players))
    heights = np.zeros(len(players))
    weights = np.zeros(len(players))
    for pos, priors in POSITION_BUCKETS.items():
        idx = position_group == pos
        ages[idx] = rng.normal(priors["age"][0], priors["age"][1], idx.sum())
        heights[idx] = rng.normal(priors["height"][0], priors["height"][1], idx.sum())
        weights[idx] = rng.normal(priors["weight"][0], priors["weight"][1], idx.sum())
    fill_missing_column(players, "age", clip_round(ages, 16, 42))
    fill_missing_column(players, "height_cm", clip_round(heights, 160, 205))
    fill_missing_column(players, "weight_kg", clip_round(weights, 58, 102))

    name_missing = missing_mask(players["player_name"])
    if name_missing.any():
        generated_names = [
            f"{team_name.get(team_id, 'Team')} Player {idx:05d}"
            for idx, team_id in zip(players.index, players["team_id"], strict=False)
        ]
        players.loc[name_missing, "player_name"] = np.asarray(generated_names, dtype=object)[name_missing.to_numpy()]

    players["position_group"] = players["position"].map(normalize_position)
    players.loc[players["position_group"] == "UNK", "position_group"] = position_group[players["position_group"] == "UNK"]
    players["profile_data_source"] = players.get("profile_data_source", "processed_or_imputed")
    return players.drop_duplicates("player_id", keep="first").reset_index(drop=True)


def complete_teams(teams: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    teams = teams.copy()
    for column in ["team_id", "team_name", "country", "logo"]:
        if column not in teams.columns:
            teams[column] = "NULL"

    team_country_rows = []
    if not matches.empty:
        home = matches[["home_team_id", "country"]].rename(columns={"home_team_id": "team_id"})
        away = matches[["away_team_id", "country"]].rename(columns={"away_team_id": "team_id"})
        team_country_rows = pd.concat([home, away], ignore_index=True)
    country_lookup = {}
    if isinstance(team_country_rows, pd.DataFrame) and not team_country_rows.empty:
        valid = team_country_rows[~missing_mask(team_country_rows["country"])]
        country_lookup = valid.drop_duplicates("team_id").set_index("team_id")["country"].to_dict()

    fill_missing_column(teams, "country", teams["team_id"].map(country_lookup).fillna("Unknown"))
    generated_logos = "generated://team-logo/" + teams["team_id"].astype(str)
    fill_missing_column(teams, "logo", generated_logos)
    teams["team_slug"] = teams["team_name"].map(slug)
    teams["team_region"] = teams["country"].map(country_region)
    return teams.drop_duplicates("team_id", keep="first").reset_index(drop=True)


def country_region(country: object) -> str:
    text = str(country).casefold()
    if text in {"argentina", "brazil", "brasil", "chile", "colombia", "ecuador", "paraguay", "peru", "uruguay", "venezuela", "bolivia"}:
        return "South America"
    if text in {"england", "spain", "france", "germany", "italy", "portugal", "netherlands", "belgium", "scotland", "turkey", "greece", "europe"}:
        return "Europe"
    if text in {"japan", "china", "south korea", "qatar", "saudi arabia"}:
        return "Asia"
    return "Other"


def build_team_season_pairs(matches: pd.DataFrame) -> pd.DataFrame:
    home = matches[["season", "home_team_id"]].rename(columns={"home_team_id": "team_id"})
    away = matches[["season", "away_team_id"]].rename(columns={"away_team_id": "team_id"})
    pairs = pd.concat([home, away], ignore_index=True)
    pairs = pairs[~missing_mask(pairs["team_id"]) & ~missing_mask(pairs["season"])]
    return pairs.drop_duplicates(["team_id", "season"]).reset_index(drop=True)


def add_team_season_rosters(
    players: pd.DataFrame,
    teams: pd.DataFrame,
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    team_names = teams.set_index("team_id")["team_name"].to_dict() if {"team_id", "team_name"}.issubset(teams.columns) else {}
    team_countries = teams.set_index("team_id")["country"].to_dict() if {"team_id", "country"}.issubset(teams.columns) else {}
    existing_ids = set(players["player_id"].astype(str))
    rows = []
    roster_rows = []
    pairs = build_team_season_pairs(matches)

    for pair in pairs.itertuples(index=False):
        team_id = str(pair.team_id)
        season = str(pair.season)
        team_name = team_names.get(team_id, team_id)
        country = team_countries.get(team_id, "Unknown")
        for slot in range(1, ROSTER_SIZE + 1):
            position = position_for_slot(slot)
            player_id = stable_id("player", team_id, season, slot)
            player_name = f"{team_name} {season} Squad {slot:02d}"
            roster_rows.append({"team_id": team_id, "season": season, "slot": slot, "player_id": player_id, "position_group": position})
            if player_id in existing_ids:
                continue
            priors = POSITION_BUCKETS[position]
            seed = deterministic_int(player_id)
            local_rng = np.random.default_rng(seed)
            rows.append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "nationality": country if not is_missing_value(country) else COUNTRIES[seed % len(COUNTRIES)],
                    "age": int(np.clip(local_rng.normal(priors["age"][0], priors["age"][1]), 16, 42)),
                    "height_cm": int(np.clip(local_rng.normal(priors["height"][0], priors["height"][1]), 160, 205)),
                    "weight_kg": int(np.clip(local_rng.normal(priors["weight"][0], priors["weight"][1]), 58, 102)),
                    "position": position,
                    "team_id": team_id,
                    "position_group": position,
                    "profile_data_source": "imputed_team_season_roster",
                    "roster_season": season,
                }
            )
            existing_ids.add(player_id)

    if rows:
        players = pd.concat([players, pd.DataFrame(rows)], ignore_index=True)
    roster = pd.DataFrame(roster_rows)
    return players.drop_duplicates("player_id", keep="first").reset_index(drop=True), roster


def build_roster_lookup(roster: pd.DataFrame) -> tuple[dict[tuple[str, str, str], list[str]], dict[tuple[str, str], list[str]]]:
    by_position: dict[tuple[str, str, str], list[str]] = {}
    all_players: dict[tuple[str, str], list[str]] = {}
    if roster.empty:
        return by_position, all_players
    work = roster[["team_id", "season", "position_group", "player_id"]].astype(str)
    for key, group in work.groupby(["team_id", "season", "position_group"], sort=False):
        by_position[key] = group["player_id"].tolist()
    for key, group in work.groupby(["team_id", "season"], sort=False):
        all_players[key] = group["player_id"].tolist()
    return by_position, all_players


def select_match_players(
    roster_by_position: dict[tuple[str, str, str], list[str]],
    roster_all: dict[tuple[str, str], list[str]],
    team_id: object,
    season: object,
    match_id: object,
) -> list[str]:
    team_key = (str(team_id), str(season))
    if team_key not in roster_all:
        return []
    selected: list[str] = []
    for pos in MATCH_POSITION_PLAN:
        pool = roster_by_position.get((team_key[0], team_key[1], pos), [])
        if not pool:
            pool = roster_all[team_key]
        offset = deterministic_int(match_id, team_id, pos, len(selected), modulo=len(pool))
        selected.append(pool[offset])
    return selected[:MATCH_PLAYERS_PER_TEAM]


def expand_player_match_stats(
    player_match: pd.DataFrame,
    matches: pd.DataFrame,
    roster: pd.DataFrame,
) -> pd.DataFrame:
    for column in BASE_PLAYER_MATCH_COLUMNS:
        if column not in player_match.columns:
            player_match[column] = "NULL"
    player_match = player_match[BASE_PLAYER_MATCH_COLUMNS + [c for c in player_match.columns if c not in BASE_PLAYER_MATCH_COLUMNS]]
    existing = set(zip(player_match["player_id"].astype(str), player_match["match_id"].astype(str), strict=False))

    roster_by_position, roster_all = build_roster_lookup(roster)
    player_ids: list[str] = []
    match_ids: list[str] = []
    team_ids: list[str] = []
    for row in matches[["match_id", "season", "home_team_id", "away_team_id"]].itertuples(index=False):
        for team_id in [row.home_team_id, row.away_team_id]:
            for player_id in select_match_players(roster_by_position, roster_all, team_id, row.season, row.match_id):
                key = (str(player_id), str(row.match_id))
                if key in existing:
                    continue
                player_ids.append(player_id)
                match_ids.append(str(row.match_id))
                team_ids.append(str(team_id))
                existing.add(key)

    if player_ids:
        generated = pd.DataFrame({"player_id": player_ids, "match_id": match_ids, "team_id": team_ids})
        for column in BASE_PLAYER_MATCH_COLUMNS:
            if column not in generated.columns:
                generated[column] = "NULL"
        generated = generated[BASE_PLAYER_MATCH_COLUMNS]
        player_match = pd.concat([player_match, generated], ignore_index=True)
    return player_match.drop_duplicates(["player_id", "match_id"], keep="first").reset_index(drop=True)


def draw_count_by_position(
    rng: np.random.Generator,
    positions: pd.Series,
    probabilities: dict[str, list[float]],
    values: list[int],
) -> np.ndarray:
    out = np.zeros(len(positions), dtype="int64")
    for pos in ["GK", "DEF", "MID", "FW", "UNK"]:
        mask = (positions == pos).to_numpy()
        if not mask.any():
            continue
        probs = probabilities.get(pos, probabilities["UNK"])
        out[mask] = rng.choice(values, size=int(mask.sum()), p=probs)
    extra = out == values[-1]
    if extra.any() and values[-1] >= 3:
        out[extra] = rng.integers(values[-1], values[-1] + 3, size=int(extra.sum()))
    return out


def fill_player_match_values(
    player_match: pd.DataFrame,
    players: pd.DataFrame,
    matches: pd.DataFrame,
    goals_events: pd.DataFrame,
) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED + 2)
    df = player_match.copy()
    player_context = players.drop_duplicates("player_id").set_index("player_id")
    positions = df["player_id"].map(player_context.get("position_group", pd.Series(dtype=object))).fillna("UNK")
    positions = positions.where(positions.isin(["GK", "DEF", "MID", "FW"]), "UNK")
    ages = numeric(df["player_id"].map(player_context.get("age", pd.Series(dtype=object))), df.index).fillna(26)
    weights = numeric(df["player_id"].map(player_context.get("weight_kg", pd.Series(dtype=object))), df.index).fillna(75)

    if not goals_events.empty and {"player_id", "match_id", "event_type"}.issubset(goals_events.columns):
        goal_mask = goals_events["event_type"].astype(str).str.casefold().eq("goal")
        observed_goals = goals_events[goal_mask].groupby(["player_id", "match_id"]).size().rename("observed_goals")
        assist_mask = ~missing_mask(goals_events.get("assist_player_id", pd.Series(index=goals_events.index, dtype=object)))
        observed_assists = (
            goals_events[goal_mask & assist_mask]
            .groupby(["assist_player_id", "match_id"])
            .size()
            .rename("observed_assists")
        )
        key_index = pd.MultiIndex.from_frame(df[["player_id", "match_id"]])
        actual_goals = pd.Series(observed_goals.reindex(key_index).to_numpy(), index=df.index)
        assist_index = pd.MultiIndex.from_arrays([df["player_id"], df["match_id"]])
        actual_assists = pd.Series(observed_assists.reindex(assist_index).to_numpy(), index=df.index)
    else:
        actual_goals = pd.Series(np.nan, index=df.index)
        actual_assists = pd.Series(np.nan, index=df.index)

    minute_draw = rng.choice([0, 1, 2, 3], size=len(df), p=[0.05, 0.10, 0.75, 0.10])
    minutes = np.select(
        [minute_draw == 0, minute_draw == 1, minute_draw == 2, minute_draw == 3],
        [
            rng.integers(1, 16, len(df)),
            rng.integers(15, 46, len(df)),
            rng.integers(45, 91, len(df)),
            rng.integers(90, 121, len(df)),
        ],
    )
    gk_def = positions.isin(["GK", "DEF"]).to_numpy()
    minutes[gk_def] = np.maximum(minutes[gk_def], rng.integers(70, 96, int(gk_def.sum())))
    fill_missing_column(df, "minutes_played", minutes)
    minutes_now = numeric(df["minutes_played"]).fillna(pd.Series(minutes, index=df.index)).clip(lower=1)

    sampled_goals = draw_count_by_position(
        rng,
        positions,
        {
            "GK": [0.997, 0.003, 0.0, 0.0],
            "DEF": [0.93, 0.06, 0.009, 0.001],
            "MID": [0.82, 0.14, 0.035, 0.005],
            "FW": [0.65, 0.25, 0.08, 0.02],
            "UNK": [0.75, 0.18, 0.05, 0.02],
        },
        [0, 1, 2, 3],
    )
    sampled_goals = np.where(actual_goals.notna(), actual_goals.fillna(0).astype(int).to_numpy(), sampled_goals)
    fill_missing_column(df, "goals", sampled_goals)

    sampled_assists = draw_count_by_position(
        rng,
        positions,
        {
            "GK": [0.995, 0.005, 0.0, 0.0],
            "DEF": [0.90, 0.085, 0.014, 0.001],
            "MID": [0.76, 0.18, 0.05, 0.01],
            "FW": [0.78, 0.16, 0.05, 0.01],
            "UNK": [0.80, 0.15, 0.04, 0.01],
        },
        [0, 1, 2, 3],
    )
    sampled_assists = np.where(actual_assists.notna(), actual_assists.fillna(0).astype(int).to_numpy(), sampled_assists)
    fill_missing_column(df, "assists", sampled_assists)

    sampled_shots = draw_count_by_position(
        rng,
        positions,
        {
            "GK": [0.995, 0.005, 0.0, 0.0],
            "DEF": [0.62, 0.25, 0.10, 0.03],
            "MID": [0.38, 0.31, 0.21, 0.10],
            "FW": [0.18, 0.32, 0.29, 0.21],
            "UNK": [0.40, 0.30, 0.20, 0.10],
        },
        [0, 1, 2, 3],
    )
    sampled_shots = np.maximum(sampled_shots, numeric(df["goals"]).fillna(0).astype(int).to_numpy())
    fill_missing_column(df, "shots", sampled_shots)
    shots_now = numeric(df["shots"]).fillna(pd.Series(sampled_shots, index=df.index)).astype(int)

    on_target = np.minimum(shots_now.to_numpy(), np.maximum(numeric(df["goals"]).fillna(0).astype(int).to_numpy(), rng.binomial(shots_now.to_numpy(), 0.38)))
    blocked = np.minimum(shots_now.to_numpy() - on_target, rng.binomial(np.maximum(shots_now.to_numpy() - on_target, 0), 0.22))
    off_target = np.maximum(shots_now.to_numpy() - on_target - blocked, 0)
    fill_missing_column(df, "shots_on_target", on_target)
    fill_missing_column(df, "shots_blocked", blocked)
    fill_missing_column(df, "shots_off_target", off_target)

    pass_category = rng.random(len(df))
    attempts = np.zeros(len(df), dtype="int64")
    for pos, cuts in {
        "GK": (0.10, 0.70),
        "DEF": (0.06, 0.66),
        "MID": (0.03, 0.63),
        "FW": (0.15, 0.75),
        "UNK": (0.10, 0.70),
    }.items():
        mask = (positions == pos).to_numpy()
        if not mask.any():
            continue
        local = pass_category[mask]
        attempts[mask] = np.select(
            [local < cuts[0], local < cuts[1], local >= cuts[1]],
            [
                rng.integers(5, 20, int(mask.sum())),
                rng.integers(20, 51, int(mask.sum())),
                rng.integers(51, 96, int(mask.sum())),
            ],
        )
    accuracy_base = positions.map({"GK": 0.72, "DEF": 0.84, "MID": 0.82, "FW": 0.76, "UNK": 0.79}).to_numpy()
    accuracy = np.clip(rng.normal(accuracy_base, 0.07), 0.45, 0.97)
    completed = np.minimum(attempts, np.rint(attempts * accuracy).astype("int64"))
    fill_missing_column(df, "passes_attempted", attempts)
    fill_missing_column(df, "passes_completed", completed)
    attempted_now = numeric(df["passes_attempted"]).fillna(pd.Series(attempts, index=df.index)).clip(lower=0)
    completed_now = numeric(df["passes_completed"]).fillna(pd.Series(completed, index=df.index)).clip(lower=0)
    pass_accuracy = safe_div(completed_now, attempted_now).fillna(0)
    fill_missing_column(df, "pass_accuracy", pass_accuracy.round(4))

    crosses = draw_count_by_position(
        rng,
        positions,
        {
            "GK": [0.995, 0.005, 0.0],
            "DEF": [0.62, 0.27, 0.11],
            "MID": [0.58, 0.30, 0.12],
            "FW": [0.64, 0.25, 0.11],
            "UNK": [0.70, 0.20, 0.10],
        },
        [0, 2, 4],
    )
    crosses = np.where(crosses == 2, rng.integers(1, 4, len(df)), crosses)
    crosses = np.where(crosses >= 4, rng.integers(4, 8, len(df)), crosses)
    crosses_completed = np.minimum(crosses, rng.binomial(crosses, 0.28))
    fill_missing_column(df, "crosses_attempted", crosses)
    fill_missing_column(df, "crosses_completed", crosses_completed)

    for column, probabilities in [
        (
            "dribbles",
            {
                "GK": [0.98, 0.02, 0.0],
                "DEF": [0.68, 0.25, 0.07],
                "MID": [0.45, 0.34, 0.21],
                "FW": [0.32, 0.38, 0.30],
                "UNK": [0.50, 0.30, 0.20],
            },
        ),
        (
            "offsides",
            {
                "GK": [1.0, 0.0, 0.0],
                "DEF": [0.96, 0.035, 0.005],
                "MID": [0.86, 0.11, 0.03],
                "FW": [0.68, 0.23, 0.09],
                "UNK": [0.80, 0.15, 0.05],
            },
        ),
        (
            "interceptions",
            {
                "GK": [0.88, 0.11, 0.01],
                "DEF": [0.35, 0.43, 0.22],
                "MID": [0.43, 0.41, 0.16],
                "FW": [0.68, 0.27, 0.05],
                "UNK": [0.50, 0.35, 0.15],
            },
        ),
        (
            "clearances",
            {
                "GK": [0.75, 0.22, 0.03],
                "DEF": [0.28, 0.47, 0.25],
                "MID": [0.62, 0.31, 0.07],
                "FW": [0.82, 0.16, 0.02],
                "UNK": [0.60, 0.30, 0.10],
            },
        ),
    ]:
        values = draw_count_by_position(rng, positions, probabilities, [0, 2, 4])
        values = np.where(values == 2, rng.integers(1, 4, len(df)), values)
        values = np.where(values >= 4, rng.integers(4, 8, len(df)), values)
        fill_missing_column(df, column, values)

    tackles = draw_count_by_position(
        rng,
        positions,
        {
            "GK": [0.94, 0.055, 0.005],
            "DEF": [0.24, 0.48, 0.28],
            "MID": [0.30, 0.48, 0.22],
            "FW": [0.58, 0.34, 0.08],
            "UNK": [0.40, 0.40, 0.20],
        },
        [0, 2, 4],
    )
    tackles = np.where(tackles == 2, rng.integers(1, 4, len(df)), tackles)
    tackles = np.where(tackles >= 4, rng.integers(4, 8, len(df)), tackles)
    tackles_won = np.minimum(tackles, rng.binomial(tackles, 0.58))
    tackles_lost = np.maximum(tackles - tackles_won, 0)
    fill_missing_column(df, "tackles", tackles)
    fill_missing_column(df, "tackles_won", tackles_won)
    fill_missing_column(df, "tackles_lost", tackles_lost)

    fouls_committed = draw_count_by_position(
        rng,
        positions,
        {
            "GK": [0.82, 0.17, 0.01],
            "DEF": [0.24, 0.52, 0.24],
            "MID": [0.28, 0.52, 0.20],
            "FW": [0.34, 0.50, 0.16],
            "UNK": [0.30, 0.50, 0.20],
        },
        [0, 1, 3],
    )
    fouls_committed = np.where(fouls_committed == 1, rng.integers(1, 3, len(df)), fouls_committed)
    fouls_committed = np.where(fouls_committed >= 3, rng.integers(3, 6, len(df)), fouls_committed)
    fouls_suffered = draw_count_by_position(
        rng,
        positions,
        {
            "GK": [0.90, 0.095, 0.005],
            "DEF": [0.42, 0.45, 0.13],
            "MID": [0.32, 0.50, 0.18],
            "FW": [0.22, 0.52, 0.26],
            "UNK": [0.30, 0.50, 0.20],
        },
        [0, 1, 3],
    )
    fouls_suffered = np.where(fouls_suffered == 1, rng.integers(1, 3, len(df)), fouls_suffered)
    fouls_suffered = np.where(fouls_suffered >= 3, rng.integers(3, 6, len(df)), fouls_suffered)
    fill_missing_column(df, "fouls_committed", fouls_committed)
    fill_missing_column(df, "fouls_suffered", fouls_suffered)

    card_factor = positions.map({"GK": 0.55, "DEF": 1.25, "MID": 1.05, "FW": 0.75, "UNK": 1.0}).to_numpy()
    yellow_probability = np.clip((fouls_committed / 4.0) * 0.18 * card_factor, 0.01, 0.60)
    yellow_cards = rng.binomial(1, yellow_probability) + rng.binomial(1, 0.01, len(df))
    red_cards = rng.binomial(1, np.where(positions.eq("DEF"), 0.013, 0.007))
    fill_missing_column(df, "yellow_cards", yellow_cards)
    fill_missing_column(df, "red_cards", red_cards)

    meters_per_min = positions.map({pos: cfg["meters_per_min"] for pos, cfg in POSITION_BUCKETS.items()}).fillna(108).to_numpy()
    age_factor = np.clip(1 - (np.abs(ages.to_numpy() - 27) * 0.006), 0.82, 1.06)
    distance = minutes_now.to_numpy() * meters_per_min * age_factor
    distance = np.where(minutes_now.to_numpy() < 45, np.clip(distance, 1000, 6000), np.clip(distance, 5000, 13000))
    fill_missing_column(df, "distance_covered", clip_round(distance, 600, 13000))

    base_speed = positions.map({pos: cfg["speed"] for pos, cfg in POSITION_BUCKETS.items()}).fillna(32.5).to_numpy()
    top_speed = base_speed - (0.1 * (ages.to_numpy() - 20)) - (0.05 * (weights.to_numpy() - 75)) + rng.normal(0, 0.9, len(df))
    fill_missing_column(df, "top_speed", clip_round(top_speed, 25, 36, as_int=False))

    touches = (
        numeric(df["passes_attempted"]).fillna(0) * 1.4
        + numeric(df["shots"]).fillna(0)
        + numeric(df["dribbles"]).fillna(0)
        + numeric(df["tackles"]).fillna(0)
        + numeric(df["interceptions"]).fillna(0)
        + numeric(df["clearances"]).fillna(0)
        + numeric(df["crosses_attempted"]).fillna(0)
    )
    fill_missing_column(df, "touches", touches.round().astype(int))

    return df


def combine_existing_with_aggregate(base: pd.DataFrame, aggregate: pd.DataFrame, keys: list[str], columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in base.columns:
            base[column] = "NULL"
        if column not in aggregate.columns:
            aggregate[column] = "NULL"
    base = base.copy()
    aggregate = aggregate[columns].drop_duplicates(keys, keep="first")
    base_indexed = base.set_index(keys, drop=False)
    agg_indexed = aggregate.set_index(keys, drop=False)
    common = base_indexed.index.intersection(agg_indexed.index)
    for column in columns:
        if column in keys:
            continue
        mask = missing_mask(base_indexed.loc[common, column])
        if mask.any():
            base_indexed.loc[common[mask.to_numpy()], column] = agg_indexed.loc[common[mask.to_numpy()], column].to_numpy()
    missing_keys = agg_indexed.index.difference(base_indexed.index)
    combined = pd.concat([base_indexed.reset_index(drop=True), agg_indexed.loc[missing_keys].reset_index(drop=True)], ignore_index=True)
    return combined.drop_duplicates(keys, keep="first").reset_index(drop=True)


def build_player_season_stats(player_match: pd.DataFrame, matches: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    context = matches[["match_id", "season"]].drop_duplicates("match_id")
    if "season" in player_match.columns:
        work = player_match.copy()
        season_from_match = work["match_id"].map(context.set_index("match_id")["season"])
        work["season"] = work["season"].where(~missing_mask(work["season"]), season_from_match)
    else:
        work = player_match.merge(context, on="match_id", how="left")
    work = work[~missing_mask(work["season"])]
    stat_columns = [column for column in BASE_PLAYER_MATCH_COLUMNS if column not in {"player_id", "match_id", "team_id"}]
    for column in stat_columns:
        if column in work.columns:
            work[column] = numeric(work[column])
    aggregate = (
        work.groupby(["player_id", "season"], dropna=False)
        .agg(
            matches_played=("match_id", "nunique"),
            minutes_played=("minutes_played", "sum"),
            goals=("goals", "sum"),
            assists=("assists", "sum"),
            shots=("shots", "sum"),
            shots_on_target=("shots_on_target", "sum"),
            passes_completed=("passes_completed", "sum"),
            passes_attempted=("passes_attempted", "sum"),
            tackles=("tackles", "sum"),
            interceptions=("interceptions", "sum"),
            fouls_committed=("fouls_committed", "sum"),
            yellow_cards=("yellow_cards", "sum"),
            red_cards=("red_cards", "sum"),
        )
        .reset_index()
    )
    for column in [c for c in BASE_PLAYER_SEASON_COLUMNS if c not in ["player_id", "season"]]:
        aggregate[column] = aggregate[column].round().astype(int)
    return combine_existing_with_aggregate(existing, aggregate, ["player_id", "season"], BASE_PLAYER_SEASON_COLUMNS)


def build_goalkeeper_stats(
    player_match: pd.DataFrame,
    matches: pd.DataFrame,
    players: pd.DataFrame,
    existing: pd.DataFrame,
) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED + 3)
    positions = players.drop_duplicates("player_id").set_index("player_id")["position_group"]
    work = player_match.copy()
    work["position_group"] = work["player_id"].map(positions).fillna("UNK")
    work = work[work["position_group"].eq("GK")]
    if work.empty:
        return existing
    context = matches[["match_id", "season", "home_team_id", "away_team_id", "home_score", "away_score"]].drop_duplicates("match_id")
    existing_season = work["season"] if "season" in work.columns else None
    work = work.merge(context, on="match_id", how="left", suffixes=("", "_match"))
    if existing_season is not None:
        work["season"] = existing_season.where(~missing_mask(existing_season), work["season"])
    is_home = work["team_id"].astype(str).eq(work["home_team_id"].astype(str))
    home_score = numeric(work["home_score"]).fillna(0)
    away_score = numeric(work["away_score"]).fillna(0)
    conceded = away_score.where(is_home, home_score).fillna(0).astype(int)
    saves = np.maximum(0, rng.poisson(lam=np.clip(conceded.to_numpy() + 2.2, 0.8, 8.0)))
    penalty_saves = rng.binomial(1, 0.05, len(work))
    punches_choice = rng.choice([0, 1, 3], size=len(work), p=[0.70, 0.25, 0.05])
    punches = np.where(punches_choice == 1, rng.integers(1, 3, len(work)), punches_choice)
    punches = np.where(punches_choice == 3, rng.integers(3, 6, len(work)), punches)
    work["saves"] = saves
    work["goals_conceded"] = conceded
    work["clean_sheets"] = (conceded == 0).astype(int)
    work["penalty_saves"] = penalty_saves
    work["punches"] = punches
    aggregate = (
        work.groupby(["player_id", "season"], dropna=False)
        .agg(
            saves=("saves", "sum"),
            goals_conceded=("goals_conceded", "sum"),
            clean_sheets=("clean_sheets", "sum"),
            penalty_saves=("penalty_saves", "sum"),
            punches=("punches", "sum"),
        )
        .reset_index()
    )
    for column in [c for c in BASE_GOALKEEPER_COLUMNS if c not in ["player_id", "season"]]:
        aggregate[column] = aggregate[column].round().astype(int)
    return combine_existing_with_aggregate(existing, aggregate, ["player_id", "season"], BASE_GOALKEEPER_COLUMNS)


def add_match_features(matches: pd.DataFrame, teams: pd.DataFrame) -> pd.DataFrame:
    df = matches.copy()
    team_names = teams.set_index("team_id")["team_name"].to_dict() if {"team_id", "team_name"}.issubset(teams.columns) else {}
    team_countries = teams.set_index("team_id")["country"].to_dict() if {"team_id", "country"}.issubset(teams.columns) else {}
    fallback_dates = "01-07-" + df["season"].map(season_start_year).astype(str)
    fill_missing_column(df, "date", fallback_dates)
    home_names = df["home_team_id"].map(team_names).fillna("Home Team").astype(str)
    fill_missing_column(df, "stadium", home_names + " Stadium")
    fill_missing_column(df, "city", home_names)
    fallback_country = df["home_team_id"].map(team_countries).fillna("Unknown")
    fill_missing_column(df, "country", fallback_country)
    fill_missing_column(df, "referee", "Unknown Referee")
    home_score = numeric(df["home_score"]).fillna(0)
    away_score = numeric(df["away_score"]).fillna(0)
    home_poss = numeric(df["possession_home"])
    away_poss = numeric(df["possession_away"])
    both_poss_missing = home_poss.isna() & away_poss.isna()
    if both_poss_missing.any():
        rng = np.random.default_rng(RNG_SEED + 4)
        imputed_home = pd.Series(rng.uniform(0.38, 0.62, len(df)).round(4), index=df.index)
        home_poss = home_poss.mask(both_poss_missing, imputed_home)
        away_poss = away_poss.mask(both_poss_missing, 1 - imputed_home)
        fill_missing_column(df, "possession_home", home_poss.round(4))
        fill_missing_column(df, "possession_away", away_poss.round(4))
    one_side_home = home_poss.isna() & away_poss.notna()
    one_side_away = away_poss.isna() & home_poss.notna()
    if one_side_home.any():
        df.loc[one_side_home, "possession_home"] = (1 - away_poss.loc[one_side_home]).round(4)
    if one_side_away.any():
        df.loc[one_side_away, "possession_away"] = (1 - home_poss.loc[one_side_away]).round(4)
    home_poss = numeric(df["possession_home"]).fillna(0.5)
    away_poss = numeric(df["possession_away"]).fillna(0.5)

    parsed_date = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
    df["season_start_year"] = df["season"].map(season_start_year)
    df["season_end_year"] = df["season_start_year"] + 1
    df["match_year"] = parsed_date.dt.year.fillna(df["season_start_year"]).astype(int)
    df["match_month"] = parsed_date.dt.month.fillna(7).astype(int)
    df["match_dayofweek"] = parsed_date.dt.dayofweek.fillna(5).astype(int)
    df["is_weekend"] = df["match_dayofweek"].isin([5, 6]).astype(int)
    df["is_2004_2026_window"] = df["season_start_year"].between(2004, 2025).astype(int)
    df["is_domestic_league"] = df["data_scope"].astype(str).str.contains("domestic", case=False, na=False).astype(int)
    df["is_international_competition"] = df["data_scope"].astype(str).str.contains("international", case=False, na=False).astype(int)
    df["is_european_competition"] = df["country"].map(country_region).eq("Europe").astype(int)
    df["is_south_american_competition"] = df["country"].map(country_region).eq("South America").astype(int)
    df["is_english_league"] = df["competition"].astype(str).str.contains("Premier League|England", case=False, na=False).astype(int)
    df["total_goals"] = (home_score + away_score).astype(int)
    df["goal_diff_home"] = (home_score - away_score).astype(int)
    df["abs_goal_diff"] = df["goal_diff_home"].abs().astype(int)
    df["home_win"] = (home_score > away_score).astype(int)
    df["away_win"] = (away_score > home_score).astype(int)
    df["draw"] = (home_score == away_score).astype(int)
    df["result_code"] = np.select([df["home_win"].eq(1), df["away_win"].eq(1)], ["H", "A"], default="D")
    df["both_teams_scored"] = ((home_score > 0) & (away_score > 0)).astype(int)
    df["over_1_5_goals"] = (df["total_goals"] > 1.5).astype(int)
    df["over_2_5_goals"] = (df["total_goals"] > 2.5).astype(int)
    df["over_3_5_goals"] = (df["total_goals"] > 3.5).astype(int)
    df["home_clean_sheet"] = (away_score == 0).astype(int)
    df["away_clean_sheet"] = (home_score == 0).astype(int)
    df["possession_diff_home"] = (home_poss - away_poss).round(4)
    df["home_possession_advantage"] = (df["possession_diff_home"] > 0).astype(int)
    df["possession_balance_index"] = (1 - (home_poss - away_poss).abs()).round(4)

    team_country = team_countries
    home_country = df["home_team_id"].map(team_country).fillna(df["country"])
    away_country = df["away_team_id"].map(team_country).fillna(df["country"])
    df["home_team_country"] = home_country
    df["away_team_country"] = away_country
    df["same_country_match"] = home_country.astype(str).eq(away_country.astype(str)).astype(int)
    df["same_region_match"] = home_country.map(country_region).eq(away_country.map(country_region)).astype(int)
    df["country_derby_proxy"] = ((df["same_country_match"] == 1) & (df["is_domestic_league"] == 1)).astype(int)

    home_team_rows = pd.DataFrame(
        {
            "team_id": df["home_team_id"],
            "matches": 1,
            "wins": df["home_win"],
            "draws": df["draw"],
            "goals_for": home_score,
            "goals_against": away_score,
            "points": df["home_win"] * 3 + df["draw"],
        }
    )
    away_team_rows = pd.DataFrame(
        {
            "team_id": df["away_team_id"],
            "matches": 1,
            "wins": df["away_win"],
            "draws": df["draw"],
            "goals_for": away_score,
            "goals_against": home_score,
            "points": df["away_win"] * 3 + df["draw"],
        }
    )
    team_history = pd.concat([home_team_rows, away_team_rows], ignore_index=True).groupby("team_id").sum(numeric_only=True)
    team_history["win_rate"] = safe_div(team_history["wins"], team_history["matches"]).fillna(0.33)
    team_history["points_per_match"] = safe_div(team_history["points"], team_history["matches"]).fillna(1.3)
    team_history["goals_for_per_match"] = safe_div(team_history["goals_for"], team_history["matches"]).fillna(1.2)
    team_history["goals_against_per_match"] = safe_div(team_history["goals_against"], team_history["matches"]).fillna(1.2)
    team_history["goal_diff_per_match"] = team_history["goals_for_per_match"] - team_history["goals_against_per_match"]

    for side, column in [("home", "home_team_id"), ("away", "away_team_id")]:
        df[f"{side}_team_match_count"] = df[column].map(team_history["matches"]).fillna(0).astype(int)
        df[f"{side}_team_win_rate"] = df[column].map(team_history["win_rate"]).fillna(0.33).round(6)
        df[f"{side}_team_points_per_match"] = df[column].map(team_history["points_per_match"]).fillna(1.3).round(6)
        df[f"{side}_team_goals_for_per_match"] = df[column].map(team_history["goals_for_per_match"]).fillna(1.2).round(6)
        df[f"{side}_team_goals_against_per_match"] = df[column].map(team_history["goals_against_per_match"]).fillna(1.2).round(6)
        df[f"{side}_team_goal_diff_per_match"] = df[column].map(team_history["goal_diff_per_match"]).fillna(0).round(6)

    pair_key = np.where(
        df["home_team_id"].astype(str) <= df["away_team_id"].astype(str),
        df["home_team_id"].astype(str) + "|" + df["away_team_id"].astype(str),
        df["away_team_id"].astype(str) + "|" + df["home_team_id"].astype(str),
    )
    pair_counts = pd.Series(pair_key).value_counts()
    df["head_to_head_match_count"] = pd.Series(pair_key).map(pair_counts).fillna(1).astype(int).to_numpy()
    strength_diff = df["home_team_points_per_match"] - df["away_team_points_per_match"]
    goal_strength_diff = df["home_team_goal_diff_per_match"] - df["away_team_goal_diff_per_match"]
    raw_home = 0.42 + (strength_diff * 0.13) + (goal_strength_diff * 0.08) + 0.06
    raw_away = 0.36 - (strength_diff * 0.13) - (goal_strength_diff * 0.08)
    raw_draw = 0.22 - strength_diff.abs() * 0.03
    stacked = np.vstack([np.clip(raw_home, 0.05, 0.90), np.clip(raw_draw, 0.05, 0.45), np.clip(raw_away, 0.05, 0.90)])
    probs = stacked / stacked.sum(axis=0)
    df["home_win_probability_proxy"] = np.round(probs[0], 6)
    df["draw_probability_proxy"] = np.round(probs[1], 6)
    df["away_win_probability_proxy"] = np.round(probs[2], 6)
    df["win_probability_edge_home"] = (df["home_win_probability_proxy"] - df["away_win_probability_proxy"]).round(6)
    df["expected_total_goals_proxy"] = (
        df["home_team_goals_for_per_match"]
        + df["away_team_goals_for_per_match"]
        + df["home_team_goals_against_per_match"]
        + df["away_team_goals_against_per_match"]
    ).div(2).round(6)
    return df


def add_goal_event_features(goals: pd.DataFrame, matches: pd.DataFrame, players: pd.DataFrame) -> pd.DataFrame:
    df = goals.copy()
    fill_missing_column(df, "assist_player_id", "NO_ASSIST")
    match_context = matches.drop_duplicates("match_id").set_index("match_id")
    player_context = players.drop_duplicates("player_id").set_index("player_id")
    minute = numeric(df["minute"]).fillna(0)
    event_type = df.get("event_type", pd.Series("NULL", index=df.index)).astype(str).str.casefold()
    goal_type = df.get("goal_type", pd.Series("NULL", index=df.index)).astype(str).str.casefold()
    is_goal = event_type.eq("goal") | goal_type.isin(["regular", "penalty", "own_goal"])
    df["is_goal_event"] = is_goal.astype(int)
    df["is_shot_event"] = (event_type.eq("shot") | is_goal).astype(int)
    df["is_header_event"] = goal_type.str.contains("header", na=False).astype(int)
    df["is_penalty_goal"] = (df.get("is_penalty", False).astype(str).str.casefold().isin(["true", "1"]) | goal_type.str.contains("penalty", na=False)).astype(int)
    df["is_own_goal_event"] = (df.get("is_own_goal", False).astype(str).str.casefold().isin(["true", "1"]) | goal_type.str.contains("own", na=False)).astype(int)
    df["minute_normalized"] = (minute / 120).clip(0, 1).round(6)
    df["minute_bucket"] = pd.cut(minute, bins=[-1, 15, 30, 45, 60, 75, 90, 130], labels=["00_15", "16_30", "31_45", "46_60", "61_75", "76_90", "90_plus"]).astype(str)
    df["is_first_half"] = minute.between(1, 45).astype(int)
    df["is_second_half"] = minute.between(46, 90).astype(int)
    df["is_extra_time"] = (minute > 90).astype(int)
    df["is_late_event"] = (minute >= 75).astype(int)
    df["event_phase_weight"] = np.select([minute <= 45, minute <= 75, minute > 75], [0.8, 1.0, 1.2], default=1.0)
    df["season"] = df["match_id"].map(match_context.get("season", pd.Series(dtype=object))).fillna("Unknown")
    df["competition"] = df["match_id"].map(match_context.get("competition", pd.Series(dtype=object))).fillna("Unknown")
    df["match_country"] = df["match_id"].map(match_context.get("country", pd.Series(dtype=object))).fillna("Unknown")
    df.loc[missing_mask(df["match_country"]), "match_country"] = "Unknown"
    df["is_english_league_event"] = df["competition"].astype(str).str.contains("Premier League", case=False, na=False).astype(int)
    df["is_south_american_event"] = df["match_country"].map(country_region).eq("South America").astype(int)
    df["player_age"] = numeric(df["player_id"].map(player_context.get("age", pd.Series(dtype=object))), df.index).fillna(26).astype(int)
    df["player_height_cm"] = numeric(df["player_id"].map(player_context.get("height_cm", pd.Series(dtype=object))), df.index).fillna(180).astype(int)
    df["player_weight_kg"] = numeric(df["player_id"].map(player_context.get("weight_kg", pd.Series(dtype=object))), df.index).fillna(75).astype(int)
    df["player_position_group"] = df["player_id"].map(player_context.get("position_group", pd.Series(dtype=object))).fillna("UNK")
    df["player_bmi"] = (df["player_weight_kg"] / ((df["player_height_cm"] / 100) ** 2)).round(4)
    pos_factor = df["player_position_group"].map({"GK": 0.02, "DEF": 0.10, "MID": 0.08, "FW": 0.14, "UNK": 0.09}).fillna(0.09)
    height_factor = ((df["player_height_cm"] - 170) / 35).clip(0, 1)
    age_factor = (1 - (df["player_age"] - 27).abs() / 22).clip(0.25, 1)
    body_factor = ((df["player_weight_kg"] - 65) / 35).clip(0, 1)
    header_probability = (pos_factor + height_factor * 0.18 + age_factor * 0.04 + body_factor * 0.03).clip(0.005, 0.65)
    header_probability = header_probability.where(df["is_penalty_goal"] == 0, 0.01)
    header_probability = header_probability.where(df["is_header_event"] == 0, 0.90)
    df["headed_goal_probability_proxy"] = header_probability.round(6)
    df["goal_value_weight"] = np.select([df["is_own_goal_event"] == 1, df["is_penalty_goal"] == 1], [-1.0, 0.78], default=1.0)
    df["open_play_goal_proxy"] = ((df["is_goal_event"] == 1) & (df["is_penalty_goal"] == 0) & (df["is_own_goal_event"] == 0)).astype(int)
    df["event_recency_weight"] = ((df["season"].map(season_start_year) - 2004) / 21).clip(0, 1).round(6)
    return df


def add_player_features(players: pd.DataFrame, teams: pd.DataFrame, player_match: pd.DataFrame, season_stats: pd.DataFrame, goals: pd.DataFrame) -> pd.DataFrame:
    df = players.copy()
    fill_missing_column(df, "roster_season", "multi_season_or_source_unknown")
    age = numeric(df["age"]).fillna(26)
    height = numeric(df["height_cm"]).fillna(180)
    weight = numeric(df["weight_kg"]).fillna(75)
    df["position_group"] = df["position"].map(normalize_position).where(lambda s: s.ne("UNK"), df.get("position_group", "UNK"))
    df["age_group"] = pd.cut(age, bins=[0, 20, 24, 29, 34, 60], labels=["u20", "21_24", "25_29", "30_34", "35_plus"]).astype(str)
    df["height_m"] = (height / 100).round(4)
    df["body_mass_index"] = (weight / (df["height_m"] ** 2)).round(4)
    df["height_bucket"] = pd.cut(height, bins=[0, 170, 180, 190, 250], labels=["short", "average", "tall", "very_tall"]).astype(str)
    df["weight_bucket"] = pd.cut(weight, bins=[0, 68, 78, 88, 140], labels=["light", "medium", "strong", "power"]).astype(str)
    df["is_goalkeeper"] = df["position_group"].eq("GK").astype(int)
    df["is_defender"] = df["position_group"].eq("DEF").astype(int)
    df["is_midfielder"] = df["position_group"].eq("MID").astype(int)
    df["is_forward"] = df["position_group"].eq("FW").astype(int)
    df["prime_age_flag"] = age.between(24, 29).astype(int)
    df["u23_flag"] = (age <= 23).astype(int)
    df["veteran_flag"] = (age >= 32).astype(int)
    df["physical_profile_score"] = (((height - 170) / 35).clip(0, 1) + ((weight - 65) / 35).clip(0, 1) + df["prime_age_flag"]) .round(6)
    team_context = teams.drop_duplicates("team_id").set_index("team_id")
    df["team_country"] = df["team_id"].map(team_context.get("country", pd.Series(dtype=object))).fillna("Unknown")
    df["team_region"] = df["team_country"].map(country_region)
    df["nationality_matches_team_country"] = df["nationality"].astype(str).eq(df["team_country"].astype(str)).astype(int)

    pm = player_match.copy()
    for col in ["minutes_played", "goals", "assists", "shots", "passes_attempted", "tackles", "interceptions"]:
        pm[col] = numeric(pm[col]) if col in pm.columns else 0
    agg = (
        pm.groupby("player_id", dropna=False)
        .agg(
            player_match_rows=("match_id", "nunique"),
            player_total_minutes=("minutes_played", "sum"),
            player_total_goals=("goals", "sum"),
            player_total_assists=("assists", "sum"),
            player_total_shots=("shots", "sum"),
            player_total_passes_attempted=("passes_attempted", "sum"),
            player_total_defensive_actions=("tackles", "sum"),
            player_total_interceptions=("interceptions", "sum"),
        )
    )
    for col in agg.columns:
        df[col] = df["player_id"].map(agg[col]).fillna(0).round(6)
    df["player_goal_involvements"] = df["player_total_goals"] + df["player_total_assists"]
    df["player_goals_per90"] = round_series(safe_div(df["player_total_goals"] * 90, df["player_total_minutes"]).fillna(0))
    df["player_assists_per90"] = round_series(safe_div(df["player_total_assists"] * 90, df["player_total_minutes"]).fillna(0))
    df["player_shots_per90"] = round_series(safe_div(df["player_total_shots"] * 90, df["player_total_minutes"]).fillna(0))
    df["player_defensive_actions_per90"] = round_series(safe_div((df["player_total_defensive_actions"] + df["player_total_interceptions"]) * 90, df["player_total_minutes"]).fillna(0))

    if not season_stats.empty:
        df["player_seasons_count"] = df["player_id"].map(season_stats.groupby("player_id")["season"].nunique()).fillna(0).astype(int)
    else:
        df["player_seasons_count"] = 0
    if not goals.empty and {"player_id", "event_type"}.issubset(goals.columns):
        goal_mask = goals["event_type"].astype(str).str.casefold().eq("goal")
        goal_counts = goals[goal_mask].groupby("player_id").size()
        header_counts = goals[goals.get("is_header_event", 0).astype(str).isin(["1", "True", "true"])].groupby("player_id").size() if "is_header_event" in goals.columns else pd.Series(dtype=float)
        df["observed_goal_events"] = df["player_id"].map(goal_counts).fillna(0).astype(int)
        df["observed_header_events"] = df["player_id"].map(header_counts).fillna(0).astype(int)
    else:
        df["observed_goal_events"] = 0
        df["observed_header_events"] = 0
    pos_factor = df["position_group"].map({"GK": 0.02, "DEF": 0.10, "MID": 0.08, "FW": 0.14, "UNK": 0.09}).fillna(0.09)
    df["headed_goal_probability_proxy"] = (pos_factor + ((height - 170) / 35).clip(0, 1) * 0.18 + df["prime_age_flag"] * 0.04).clip(0.005, 0.65).round(6)
    df["scoring_profile_score"] = (numeric(df["player_goals_per90"]).fillna(0) + numeric(df["player_shots_per90"]).fillna(0) * 0.15 + df["is_forward"] * 0.5).round(6)
    df["creation_profile_score"] = (numeric(df["player_assists_per90"]).fillna(0) + df["is_midfielder"] * 0.35 + df["player_total_passes_attempted"] / 10000).round(6)
    df["defensive_profile_score"] = (numeric(df["player_defensive_actions_per90"]).fillna(0) + df["is_defender"] * 0.45).round(6)
    return df


def add_player_match_features(player_match: pd.DataFrame, matches: pd.DataFrame, players: pd.DataFrame) -> pd.DataFrame:
    df = player_match.copy()
    match_context = matches.drop_duplicates("match_id").set_index("match_id")
    player_context = players.drop_duplicates("player_id").set_index("player_id")
    minutes = numeric(df["minutes_played"]).fillna(0)
    for column in [
        "goals",
        "assists",
        "shots",
        "shots_on_target",
        "shots_off_target",
        "shots_blocked",
        "passes_completed",
        "passes_attempted",
        "crosses_completed",
        "crosses_attempted",
        "dribbles",
        "offsides",
        "tackles",
        "tackles_won",
        "tackles_lost",
        "interceptions",
        "clearances",
        "fouls_committed",
        "fouls_suffered",
        "yellow_cards",
        "red_cards",
        "distance_covered",
        "top_speed",
        "touches",
    ]:
        values = numeric(df[column]).fillna(0)
        if column not in {"pass_accuracy", "top_speed"}:
            df[f"{column}_per90"] = round_series(safe_div(values * 90, minutes).fillna(0))

    df["shot_accuracy"] = round_series(safe_div(numeric(df["shots_on_target"]).fillna(0), numeric(df["shots"]).fillna(0)).fillna(0))
    df["goal_conversion_rate"] = round_series(safe_div(numeric(df["goals"]).fillna(0), numeric(df["shots"]).fillna(0)).fillna(0))
    df["pass_completion_rate"] = round_series(safe_div(numeric(df["passes_completed"]).fillna(0), numeric(df["passes_attempted"]).fillna(0)).fillna(0))
    df["cross_completion_rate"] = round_series(safe_div(numeric(df["crosses_completed"]).fillna(0), numeric(df["crosses_attempted"]).fillna(0)).fillna(0))
    df["tackle_success_rate"] = round_series(safe_div(numeric(df["tackles_won"]).fillna(0), numeric(df["tackles"]).fillna(0)).fillna(0))
    df["defensive_actions"] = numeric(df["tackles"]).fillna(0) + numeric(df["interceptions"]).fillna(0) + numeric(df["clearances"]).fillna(0)
    df["attacking_actions"] = numeric(df["shots"]).fillna(0) + numeric(df["dribbles"]).fillna(0) + numeric(df["crosses_attempted"]).fillna(0)
    df["ball_actions"] = numeric(df["touches"]).fillna(0) + df["attacking_actions"] + df["defensive_actions"]
    df["discipline_points"] = numeric(df["fouls_committed"]).fillna(0) + numeric(df["yellow_cards"]).fillna(0) * 3 + numeric(df["red_cards"]).fillna(0) * 6
    df["distance_per_minute"] = round_series(safe_div(numeric(df["distance_covered"]).fillna(0), minutes).fillna(0))
    df["speed_distance_index"] = round_series((numeric(df["top_speed"]).fillna(0) * numeric(df["distance_covered"]).fillna(0)) / 1000)
    df["minutes_share_regular_time"] = round_series((minutes / 90).clip(0, 1.3333))
    df["starter_proxy"] = (minutes >= 45).astype(int)
    df["substitute_proxy"] = (minutes < 45).astype(int)
    df["full_match_proxy"] = (minutes >= 90).astype(int)
    df["goal_involvements"] = numeric(df["goals"]).fillna(0) + numeric(df["assists"]).fillna(0)
    df["non_shot_goal_proxy"] = ((numeric(df["goals"]).fillna(0) > numeric(df["shots"]).fillna(0)) & (numeric(df["goals"]).fillna(0) > 0)).astype(int)
    df["season"] = df["match_id"].map(match_context.get("season", pd.Series(dtype=object))).fillna("Unknown")
    df["competition"] = df["match_id"].map(match_context.get("competition", pd.Series(dtype=object))).fillna("Unknown")
    df["match_country"] = df["match_id"].map(match_context.get("country", pd.Series(dtype=object))).fillna("Unknown")
    df.loc[missing_mask(df["match_country"]), "match_country"] = "Unknown"
    home_team = df["match_id"].map(match_context.get("home_team_id", pd.Series(dtype=object))).fillna("NULL")
    away_team = df["match_id"].map(match_context.get("away_team_id", pd.Series(dtype=object))).fillna("NULL")
    home_score = numeric(df["match_id"].map(match_context.get("home_score", pd.Series(dtype=object))), df.index).fillna(0)
    away_score = numeric(df["match_id"].map(match_context.get("away_score", pd.Series(dtype=object))), df.index).fillna(0)
    df["team_is_home"] = df["team_id"].astype(str).eq(home_team.astype(str)).astype(int)
    is_home = df["team_is_home"].eq(1)
    df["opponent_team_id"] = away_team.where(is_home, home_team)
    df["team_goals_for"] = home_score.where(is_home, away_score).astype(int)
    df["team_goals_against"] = away_score.where(is_home, home_score).astype(int)
    df["team_goal_diff"] = (df["team_goals_for"] - df["team_goals_against"]).astype(int)
    df["team_result_points"] = np.select([df["team_goal_diff"] > 0, df["team_goal_diff"] == 0], [3, 1], default=0)
    df["team_clean_sheet"] = (df["team_goals_against"] == 0).astype(int)
    df["team_win"] = (df["team_result_points"] == 3).astype(int)
    df["team_draw"] = (df["team_result_points"] == 1).astype(int)
    df["team_loss"] = (df["team_result_points"] == 0).astype(int)
    df["is_english_league_match"] = df["competition"].astype(str).str.contains("Premier League", case=False, na=False).astype(int)
    df["player_age"] = numeric(df["player_id"].map(player_context.get("age", pd.Series(dtype=object))), df.index).fillna(26).astype(int)
    df["player_height_cm"] = numeric(df["player_id"].map(player_context.get("height_cm", pd.Series(dtype=object))), df.index).fillna(180).astype(int)
    df["player_weight_kg"] = numeric(df["player_id"].map(player_context.get("weight_kg", pd.Series(dtype=object))), df.index).fillna(75).astype(int)
    df["player_position_group"] = df["player_id"].map(player_context.get("position_group", pd.Series(dtype=object))).fillna("UNK")
    df["two_way_index"] = (df["attacking_actions"] + df["defensive_actions"] - df["discipline_points"]).round(6)
    df["role_fit_score"] = np.select(
        [
            df["player_position_group"].eq("GK"),
            df["player_position_group"].eq("DEF"),
            df["player_position_group"].eq("MID"),
            df["player_position_group"].eq("FW"),
        ],
        [
            df["team_clean_sheet"] + numeric(df["minutes_played"]).fillna(0) / 120,
            df["defensive_actions"] + df["team_clean_sheet"],
            numeric(df["passes_attempted"]).fillna(0) / 40 + df["two_way_index"] / 10,
            numeric(df["shots"]).fillna(0) + df["goal_involvements"],
        ],
        default=df["two_way_index"] / 10,
    ).round(6)
    return df


def add_player_season_features(season_stats: pd.DataFrame, players: pd.DataFrame) -> pd.DataFrame:
    df = season_stats.copy()
    player_context = players.drop_duplicates("player_id").set_index("player_id")
    minutes = numeric(df["minutes_played"]).fillna(0)
    matches = numeric(df["matches_played"]).fillna(0)
    for col in ["goals", "assists", "shots", "shots_on_target", "passes_completed", "passes_attempted", "tackles", "interceptions", "fouls_committed", "yellow_cards", "red_cards"]:
        values = numeric(df[col]).fillna(0)
        df[f"{col}_per90"] = round_series(safe_div(values * 90, minutes).fillna(0))
        df[f"{col}_per_match"] = round_series(safe_div(values, matches).fillna(0))
    df["minutes_per_match"] = round_series(safe_div(minutes, matches).fillna(0))
    df["availability_rate"] = round_series((matches / 38).clip(0, 1))
    df["goal_involvements"] = numeric(df["goals"]).fillna(0) + numeric(df["assists"]).fillna(0)
    df["goal_involvements_per90"] = round_series(safe_div(df["goal_involvements"] * 90, minutes).fillna(0))
    df["shot_accuracy"] = round_series(safe_div(numeric(df["shots_on_target"]).fillna(0), numeric(df["shots"]).fillna(0)).fillna(0))
    df["goal_conversion_rate"] = round_series(safe_div(numeric(df["goals"]).fillna(0), numeric(df["shots"]).fillna(0)).fillna(0))
    df["pass_completion_rate"] = round_series(safe_div(numeric(df["passes_completed"]).fillna(0), numeric(df["passes_attempted"]).fillna(0)).fillna(0))
    df["defensive_actions"] = numeric(df["tackles"]).fillna(0) + numeric(df["interceptions"]).fillna(0)
    df["discipline_points"] = numeric(df["fouls_committed"]).fillna(0) + numeric(df["yellow_cards"]).fillna(0) * 3 + numeric(df["red_cards"]).fillna(0) * 6
    df["discipline_points_per90"] = round_series(safe_div(df["discipline_points"] * 90, minutes).fillna(0))
    df["scoring_index"] = (numeric(df["goals_per90"]).fillna(0) * 2 + numeric(df["shots_per90"]).fillna(0) * 0.2 + numeric(df["goal_conversion_rate"]).fillna(0)).round(6)
    df["creator_index"] = (numeric(df["assists_per90"]).fillna(0) * 2 + numeric(df["passes_attempted_per90"]).fillna(0) * 0.03 + numeric(df["pass_completion_rate"]).fillna(0)).round(6)
    df["defensive_index"] = (safe_div(df["defensive_actions"] * 90, minutes).fillna(0) + numeric(df["interceptions_per90"]).fillna(0)).round(6)
    df["player_age"] = numeric(df["player_id"].map(player_context.get("age", pd.Series(dtype=object))), df.index).fillna(26).astype(int)
    df["player_height_cm"] = numeric(df["player_id"].map(player_context.get("height_cm", pd.Series(dtype=object))), df.index).fillna(180).astype(int)
    df["player_weight_kg"] = numeric(df["player_id"].map(player_context.get("weight_kg", pd.Series(dtype=object))), df.index).fillna(75).astype(int)
    df["player_position_group"] = df["player_id"].map(player_context.get("position_group", pd.Series(dtype=object))).fillna("UNK")
    df["season_start_year"] = df["season"].map(season_start_year)
    df["is_peak_age_season"] = df["player_age"].between(24, 29).astype(int)
    df["bmi"] = (df["player_weight_kg"] / ((df["player_height_cm"] / 100) ** 2)).round(4)
    return df


def add_goalkeeper_features(goalkeepers: pd.DataFrame, season_stats: pd.DataFrame, players: pd.DataFrame) -> pd.DataFrame:
    df = goalkeepers.copy()
    fill_missing_column(df, "season", "Unknown")
    for column, fallback in [
        ("saves", 0),
        ("goals_conceded", 0),
        ("clean_sheets", 0),
        ("penalty_saves", 0),
        ("punches", 0),
    ]:
        fill_missing_column(df, column, fallback)
    season_context = season_stats.drop_duplicates(["player_id", "season"]).set_index(["player_id", "season"])
    key_index = pd.MultiIndex.from_frame(df[["player_id", "season"]])
    matches_played = pd.Series(season_context.get("matches_played", pd.Series(dtype=object)).reindex(key_index).to_numpy(), index=df.index)
    minutes = pd.Series(season_context.get("minutes_played", pd.Series(dtype=object)).reindex(key_index).to_numpy(), index=df.index)
    matches_played = numeric(matches_played).fillna(1).clip(lower=1)
    minutes = numeric(minutes).fillna(matches_played * 90).clip(lower=1)
    saves = numeric(df["saves"]).fillna(0)
    conceded = numeric(df["goals_conceded"]).fillna(0)
    clean_sheets = numeric(df["clean_sheets"]).fillna(0)
    penalty_saves = numeric(df["penalty_saves"]).fillna(0)
    punches = numeric(df["punches"]).fillna(0)
    df["matches_played_proxy"] = matches_played.astype(int)
    df["minutes_played_proxy"] = minutes.astype(int)
    df["saves_per_match"] = round_series(safe_div(saves, matches_played).fillna(0))
    df["saves_per90"] = round_series(safe_div(saves * 90, minutes).fillna(0))
    df["goals_conceded_per_match"] = round_series(safe_div(conceded, matches_played).fillna(0))
    df["goals_conceded_per90"] = round_series(safe_div(conceded * 90, minutes).fillna(0))
    df["clean_sheet_rate"] = round_series(safe_div(clean_sheets, matches_played).fillna(0))
    df["penalty_save_rate"] = round_series(safe_div(penalty_saves, matches_played).fillna(0))
    df["punches_per_match"] = round_series(safe_div(punches, matches_played).fillna(0))
    df["save_to_conceded_ratio"] = round_series(safe_div(saves, conceded + 1).fillna(0))
    df["keeper_reliability_index"] = (numeric(df["clean_sheet_rate"]).fillna(0) * 2 + numeric(df["save_to_conceded_ratio"]).fillna(0) - numeric(df["goals_conceded_per_match"]).fillna(0)).round(6)
    df["shot_stopping_index"] = (numeric(df["saves_per90"]).fillna(0) - numeric(df["goals_conceded_per90"]).fillna(0)).round(6)
    df["box_command_index"] = (numeric(df["punches_per_match"]).fillna(0) + numeric(df["clean_sheet_rate"]).fillna(0)).round(6)
    player_context = players.drop_duplicates("player_id").set_index("player_id")
    df["player_age"] = numeric(df["player_id"].map(player_context.get("age", pd.Series(dtype=object))), df.index).fillna(29).astype(int)
    df["player_height_cm"] = numeric(df["player_id"].map(player_context.get("height_cm", pd.Series(dtype=object))), df.index).fillna(190).astype(int)
    df["player_weight_kg"] = numeric(df["player_id"].map(player_context.get("weight_kg", pd.Series(dtype=object))), df.index).fillna(84).astype(int)
    df["keeper_physical_index"] = (((df["player_height_cm"] - 180) / 25).clip(0, 1) + ((df["player_weight_kg"] - 75) / 25).clip(0, 1)).round(6)
    df["prime_keeper_age_flag"] = df["player_age"].between(27, 34).astype(int)
    df["season_start_year"] = df["season"].map(season_start_year)
    return df


def fast_dataset_profile(
    name: str,
    df: pd.DataFrame,
    required_records: int,
    null_threshold: float = 0.0025,
) -> dict:
    rows = int(len(df))
    column_profiles = {}
    failing_columns = []
    for column in df.columns:
        missing_count = int(missing_mask(df[column]).sum())
        missing_ratio = missing_count / rows if rows else 0.0
        passes = missing_ratio <= null_threshold
        if not passes:
            failing_columns.append(column)
        column_profiles[column] = {
            "missing_count": missing_count,
            "missing_ratio": round(missing_ratio, 6),
            "passes_threshold": passes,
        }
    key_fields = {
        "matches": ["match_id"],
        "players": ["player_id"],
        "teams": ["team_id"],
        "goals": ["goal_id"],
        "gk": ["player_id", "season"],
        "match_stats": ["player_id", "match_id"],
        "season_stats": ["player_id", "season"],
    }.get(name, [])
    duplicate_rows = int(df.duplicated(key_fields).sum()) if set(key_fields).issubset(df.columns) else 0
    return {
        "rows": rows,
        "columns": int(len(df.columns)),
        "null_threshold": null_threshold,
        "passes_null_threshold": not failing_columns,
        "failing_columns": failing_columns,
        "column_profiles": column_profiles,
        "duplicate_profile": {"key_fields": key_fields, "duplicate_rows": duplicate_rows},
        "minimum_records_required": required_records,
        "passes_record_requirement": rows >= required_records,
        "passes_quality_gate": (not failing_columns) and duplicate_rows == 0 and rows >= required_records,
    }


def write_fast_quality_report(outputs: dict[str, pd.DataFrame], report_path: Path) -> dict:
    record_targets = {"goals": 1_500_000, "match_stats": 1_500_000}
    datasets = {
        name: fast_dataset_profile(name, df, record_targets.get(name, 0))
        for name, df in outputs.items()
    }
    players = outputs.get("players", pd.DataFrame())
    teams = outputs.get("teams", pd.DataFrame())
    unique_players = int(players["player_id"].nunique()) if "player_id" in players.columns else 0
    unique_teams = int(teams["team_id"].nunique()) if "team_id" in teams.columns else 0
    aggregate_targets = {
        "minimum_unique_players_required": 85_000,
        "unique_players": unique_players,
        "passes_unique_players_requirement": unique_players >= 85_000,
        "minimum_teams_required": 1_500,
        "teams": unique_teams,
        "passes_teams_requirement": unique_teams >= 1_500,
    }
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": {
            "scalable_large_dataset_profile": True,
            "null_threshold_per_column": 0.0025,
            "minimum_records_by_dataset": record_targets,
            "adjacent_full_row_comparison_skipped": "Skipped for million-row wide tables to avoid quadratic memory pressure.",
        },
        "total_records": int(sum(len(df) for df in outputs.values())),
        "datasets": datasets,
        "aggregate_targets": aggregate_targets,
        "passes_aggregate_targets": (
            aggregate_targets["passes_unique_players_requirement"]
            and aggregate_targets["passes_teams_requirement"]
        ),
        "passes_all_quality_gates": (
            all(dataset["passes_quality_gate"] for dataset in datasets.values())
            and aggregate_targets["passes_unique_players_requirement"]
            and aggregate_targets["passes_teams_requirement"]
        ),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def main() -> None:
    LOGS_DIR.mkdir(exist_ok=True)
    print("Loading processed datasets...", flush=True)
    matches = read_cleaned("matches")
    players = read_cleaned("players")
    teams = read_cleaned("teams")
    goals = read_cleaned("goals")
    player_match = read_cleaned("match_stats")
    player_season = read_cleaned("season_stats")
    goalkeepers = read_cleaned("gk")

    before = {
        "matches": {"rows": int(len(matches)), "columns": int(len(matches.columns))},
        "players": {"rows": int(len(players)), "columns": int(len(players.columns))},
        "teams": {"rows": int(len(teams)), "columns": int(len(teams.columns))},
        "goals": {"rows": int(len(goals)), "columns": int(len(goals.columns))},
        "match_stats": {"rows": int(len(player_match)), "columns": int(len(player_match.columns))},
        "season_stats": {"rows": int(len(player_season)), "columns": int(len(player_season.columns))},
        "gk": {"rows": int(len(goalkeepers)), "columns": int(len(goalkeepers.columns))},
    }

    print("Completing teams and player profiles...", flush=True)
    teams = complete_teams(teams, matches)
    players = fill_existing_missing_profiles(players, teams)
    print("Building team-season rosters...", flush=True)
    players, roster = add_team_season_rosters(players, teams, matches)
    print("Expanding player-match stats...", flush=True)
    player_match = expand_player_match_stats(player_match, matches, roster)
    print(f"Player-match rows after expansion: {len(player_match):,}", flush=True)
    print("Imputing player-match stat gaps...", flush=True)
    player_match = fill_player_match_values(player_match, players, matches, goals)
    print("Aggregating player-season and goalkeeper stats...", flush=True)
    player_season = build_player_season_stats(player_match, matches, player_season)
    goalkeepers = build_goalkeeper_stats(player_match, matches, players, goalkeepers)

    print("Adding match features...", flush=True)
    matches = add_match_features(matches, teams)
    print("Adding goal-event features...", flush=True)
    goals = add_goal_event_features(goals, matches, players)
    print("Adding player features...", flush=True)
    players = add_player_features(players, teams, player_match, player_season, goals)
    print("Adding player-match features...", flush=True)
    player_match = add_player_match_features(player_match, matches, players)
    print("Adding player-season and goalkeeper features...", flush=True)
    player_season = add_player_season_features(player_season, players)
    goalkeepers = add_goalkeeper_features(goalkeepers, player_season, players)

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
        print(f"Writing {name}: {len(df):,} rows x {len(df.columns):,} columns...", flush=True)
        write_cleaned(name, df)

    print("Writing quality report...", flush=True)
    quality = write_fast_quality_report(outputs, LOGS_DIR / "data_quality_report.json")

    after = {
        name: {
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "new_columns": sorted([column for column in df.columns if column not in set(read_cols.get(name, []))]),
        }
        for name, df in outputs.items()
    }
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": "Observed non-empty values are preserved. Missing player-match, profile, and goalkeeper gaps are filled with deterministic source-derived priors and documented probability distributions.",
        "row_target": {
            "player_match_stats_minimum_rows": MIN_PLAYER_MATCH_ROWS,
            "actual_player_match_stats_rows": int(len(player_match)),
            "goals_events_minimum_rows": 1_500_000,
            "actual_goals_events_rows": int(len(goals)),
        },
        "before": before,
        "after": after,
        "roster_rows": int(len(roster)),
        "quality_report": str(LOGS_DIR / "data_quality_report.json"),
        "passes_quality": quality["passes_all_quality_gates"],
    }
    out = LOGS_DIR / "processed_feature_enrichment_report.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"report": str(out), "outputs": {k: {"rows": v["rows"], "columns": v["columns"]} for k, v in after.items()}}, indent=2))


read_cols = {
    "matches": [
        "match_id",
        "season",
        "competition",
        "data_scope",
        "source",
        "date",
        "home_team_id",
        "away_team_id",
        "stadium",
        "city",
        "country",
        "referee",
        "home_score",
        "away_score",
        "possession_home",
        "possession_away",
    ],
    "players": ["player_id", "player_name", "nationality", "age", "height_cm", "weight_kg", "position", "team_id"],
    "teams": ["team_id", "team_name", "country", "logo"],
    "goals": ["goal_id", "match_id", "minute", "goal_type", "player_id", "assist_player_id", "event_type", "player_name", "is_penalty", "is_own_goal"],
    "gk": BASE_GOALKEEPER_COLUMNS,
    "match_stats": BASE_PLAYER_MATCH_COLUMNS,
    "season_stats": BASE_PLAYER_SEASON_COLUMNS,
}


if __name__ == "__main__":
    main()
