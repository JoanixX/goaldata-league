import hashlib
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from src.data_quality import write_quality_report
except ModuleNotFoundError:  # Allows `python src/build_processed.py`.
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from src.data_quality import write_quality_report


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

OUTPUTS = {
    "matches": PROCESSED_DIR / "core" / "matches_cleaned.csv",
    "players": PROCESSED_DIR / "core" / "players_cleaned.csv",
    "teams": PROCESSED_DIR / "core" / "teams_cleaned.csv",
    "goals": PROCESSED_DIR / "events" / "goals_events_cleaned.csv",
    "gk": PROCESSED_DIR / "stats" / "goalkeeper_stats_cleaned.csv",
    "match_stats": PROCESSED_DIR / "stats" / "player_match_stats_cleaned.csv",
    "season_stats": PROCESSED_DIR / "stats" / "player_season_stats_cleaned.csv",
}
MIN_RECORDS_REQUIRED = 1_500_000
NULL_THRESHOLD = 0.0025

MATCH_COLUMNS = [
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
]
PLAYER_COLUMNS = ["player_id", "player_name", "nationality", "age", "height_cm", "weight_kg", "position", "team_id"]
TEAM_COLUMNS = ["team_id", "team_name", "country", "logo"]
GOAL_COLUMNS = ["goal_id", "match_id", "minute", "goal_type", "player_id", "assist_player_id", "event_type", "player_name", "is_penalty", "is_own_goal"]
PLAYER_MATCH_COLUMNS = [
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
PLAYER_SEASON_COLUMNS = [
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
GOALKEEPER_COLUMNS = ["player_id", "season", "saves", "goals_conceded", "clean_sheets", "penalty_saves", "punches"]
FOOTBALL_DATA_LEAGUES = {
    "E0": {"competition": "Premier League", "country": "England"},
    "SP1": {"competition": "La Liga", "country": "Spain"},
    "D1": {"competition": "Bundesliga", "country": "Germany"},
    "I1": {"competition": "Serie A", "country": "Italy"},
    "F1": {"competition": "Ligue 1", "country": "France"},
    "P1": {"competition": "Primeira Liga", "country": "Portugal"},
}
FOOTBALL_DATA_COLUMNS = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "Referee"}
TOP_LEAGUE_NAMES = {"Premier League", "La Liga", "Ligue 1", "Bundesliga", "Serie A"}
COUNTRY_CODES = {
    "ARG": "Argentina",
    "BRA": "Brazil",
    "COL": "Colombia",
    "ECU": "Ecuador",
    "PAR": "Paraguay",
    "PER": "Peru",
    "URU": "Uruguay",
    "CHI": "Chile",
    "BOL": "Bolivia",
    "VEN": "Venezuela",
    "ENG": "England",
    "ESP": "Spain",
    "FRA": "France",
    "GER": "Germany",
    "ITA": "Italy",
    "POR": "Portugal",
}
NUMERIC_COLUMNS_BY_OUTPUT = {
    "matches": ["home_score", "away_score"],
    "players": ["age", "height_cm", "weight_kg"],
    "goals": ["minute"],
    "gk": ["saves", "goals_conceded", "clean_sheets", "penalty_saves", "punches"],
    "match_stats": [
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
        "touches",
    ],
    "season_stats": [
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
    ],
}
BOOLEAN_COLUMNS_BY_OUTPUT = {
    "goals": ["is_penalty", "is_own_goal"],
}
STRING_COLUMNS_BY_OUTPUT = {
    "matches": ["possession_home", "possession_away"],
}


def clean_text(value):
    if pd.isna(value):
        return "NULL"
    value = str(value).strip()
    if value.upper() in {"NOT_AVAILABLE_IN_SOURCE", "NO_ASSIST_OR_NOT_RECORDED"}:
        return "NULL"
    return value if value else "NULL"


def strip_accents(value):
    return "".join(
        char for char in unicodedata.normalize("NFKD", str(value))
        if not unicodedata.combining(char)
    )


def soft_norm(value):
    value = clean_text(value)
    if value == "NULL":
        return ""
    value = strip_accents(value)
    value = re.sub(r"[^A-Za-z0-9 ]+", " ", value)
    return " ".join(value.casefold().split())


def stable_id(prefix, *parts):
    raw = "|".join(soft_norm(p) for p in parts)
    return f"{prefix}_{hashlib.md5(raw.encode('utf-8')).hexdigest()[:12]}"


def parse_score(score):
    match = re.search(r"(\d+)\s*-\s*(\d+)", clean_text(score))
    if not match:
        return "NULL", "NULL"
    return int(match.group(1)), int(match.group(2))


def pct_to_decimal(value):
    value = clean_text(value).replace("%", "")
    if value == "NULL":
        return "NULL"
    try:
        number = float(value)
    except ValueError:
        return "NULL"
    return round(number / 100, 4) if number > 1 else round(number, 4)


def to_number(value):
    if pd.isna(value):
        return "NULL"
    text = str(value).replace("%", "").strip()
    if not text or text.upper() in {"NOT_AVAILABLE_IN_SOURCE", "NO_ASSIST_OR_NOT_RECORDED"}:
        return "NULL"
    try:
        number = float(text)
    except ValueError:
        return "NULL"
    return int(number) if number.is_integer() else round(number, 4)


def read_csv(path, **kwargs):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, keep_default_na=False, encoding="utf-8", **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, keep_default_na=False, encoding="latin1", **kwargs)


def append_unique(rows, row, key_fields):
    key = tuple(row.get(k, "NULL") for k in key_fields)
    if key not in rows["_seen"]:
        rows["_seen"].add(key)
        rows["items"].append(row)


def missing_score(row):
    return sum(1 for value in row.values() if clean_text(value) != "NULL")


def merge_duplicate_rows(df, key, order_columns):
    if df.empty:
        return df
    merged = []
    for _, group in df.groupby(key, sort=False, dropna=False):
        group = group.copy()
        group["__score"] = group.apply(lambda row: missing_score(row.to_dict()), axis=1)
        group = group.sort_values(["__score"], ascending=False)
        base = group.iloc[0].drop(labels=["__score"]).to_dict()
        for _, row in group.drop(columns=["__score"]).iterrows():
            for column in order_columns:
                if clean_text(base.get(column)) == "NULL" and clean_text(row.get(column)) != "NULL":
                    base[column] = row.get(column)
        merged.append(base)
    return pd.DataFrame(merged)[order_columns]


def build_player_alias_map(players):
    if players.empty:
        return {}
    work = players.copy()
    work["__name_norm"] = work["player_name"].map(soft_norm)
    work["__parts"] = work["__name_norm"].str.split()
    work["__last"] = work["__parts"].map(lambda parts: parts[-1] if parts else "")
    alias_map = {}
    for _, group in work.groupby(["team_id", "__last"], dropna=False):
        full = group[group["__parts"].map(len) > 1]
        short = group[group["__parts"].map(len) == 1]
        if len(full) != 1 or short.empty:
            continue
        target = full.iloc[0]
        for _, source in short.iterrows():
            if source["player_id"] != target["player_id"]:
                alias_map[source["player_id"]] = target["player_id"]
    return alias_map


def apply_player_aliases(outputs):
    players = outputs.get("players", pd.DataFrame())
    alias_map = build_player_alias_map(players)
    if not alias_map:
        return outputs, {}
    for name in ["players", "goals", "gk", "match_stats", "season_stats"]:
        df = outputs.get(name)
        if df is not None and "player_id" in df.columns:
            df = df.copy()
            df["player_id"] = df["player_id"].replace(alias_map)
            outputs[name] = df
    if "goals" in outputs and "assist_player_id" in outputs["goals"].columns:
        outputs["goals"] = outputs["goals"].copy()
        outputs["goals"]["assist_player_id"] = outputs["goals"]["assist_player_id"].replace(alias_map)
    outputs["players"] = merge_duplicate_rows(outputs["players"], "player_id", PLAYER_COLUMNS)
    outputs["gk"] = frame(outputs["gk"].to_dict("records"), GOALKEEPER_COLUMNS, ["player_id", "season"])
    outputs["match_stats"] = frame(outputs["match_stats"].to_dict("records"), PLAYER_MATCH_COLUMNS, ["player_id", "match_id"])
    outputs["season_stats"] = frame(outputs["season_stats"].to_dict("records"), PLAYER_SEASON_COLUMNS, ["player_id", "season"])
    return outputs, {"player_aliases_applied": len(alias_map), "aliases": alias_map}


def parse_lineups(lineups, team_ids):
    players = []
    if clean_text(lineups) == "NULL":
        return players
    for chunk in str(lineups).split("|"):
        if ":" not in chunk:
            continue
        team_name, names = chunk.split(":", 1)
        team_id = team_ids.get(soft_norm(team_name), stable_id("team", team_name))
        for name in re.split(r";|,", names):
            player_name = clean_text(name)
            if player_name != "NULL":
                players.append((player_name, team_id))
    return players


def parse_goal_events(goal_text, match_id):
    events = []
    if clean_text(goal_text) == "NULL":
        return events
    current_player = None
    for token in [t.strip() for t in str(goal_text).split(";") if t.strip()]:
        first = re.match(r"(.+?)\s+(\d+(?:\+\d+)?)'?(\s*\((P|OG)\))?", token)
        repeat = re.match(r"^(\d+(?:\+\d+)?)'?(\s*\((P|OG)\))?$", token)
        if first:
            current_player = clean_text(first.group(1))
            minute = first.group(2)
            marker = first.group(4)
        elif repeat and current_player:
            minute = repeat.group(1)
            marker = repeat.group(3)
        else:
            continue
        goal_type = "penalty" if marker == "P" else "own_goal" if marker == "OG" else "regular"
        player_id = stable_id("player", current_player)
        goal_id = stable_id("goal", match_id, current_player, minute, goal_type)
        events.append(
            {
                "goal_id": goal_id,
                "match_id": match_id,
                "minute": minute,
                "goal_type": goal_type,
                "player_id": player_id,
                "assist_player_id": "NULL",
                "event_type": "goal",
                "player_name": current_player,
                "is_penalty": goal_type == "penalty",
                "is_own_goal": goal_type == "own_goal",
            }
        )
    return events


def build_from_completed():
    df = read_csv(RAW_DIR / "cl_2010_2025_completed.csv")
    team_rows = {"_seen": set(), "items": []}
    player_rows = {"_seen": set(), "items": []}
    match_rows = []
    goal_rows = []
    player_match_rows = {"_seen": set(), "items": []}

    for _, row in df.iterrows():
        home_team = clean_text(row.get("local"))
        away_team = clean_text(row.get("visitante"))
        home_team_id = stable_id("team", home_team)
        away_team_id = stable_id("team", away_team)
        append_unique(team_rows, {"team_id": home_team_id, "team_name": home_team, "country": "NULL", "logo": "NULL"}, ["team_id"])
        append_unique(team_rows, {"team_id": away_team_id, "team_name": away_team, "country": "NULL", "logo": "NULL"}, ["team_id"])

        home_score, away_score = parse_score(row.get("marcador"))
        match_id = stable_id("match", row.get("season"), row.get("fecha"), home_team, away_team)
        match_rows.append(
            {
                "match_id": match_id,
                "season": clean_text(row.get("season")),
                "competition": "UEFA Champions League",
                "data_scope": "champions_league",
                "source": "cl_2010_2025_completed.csv",
                "date": clean_text(row.get("fecha")),
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "stadium": clean_text(row.get("estadio")),
                "city": clean_text(row.get("ciudad")),
                "country": clean_text(row.get("pais")),
                "referee": clean_text(row.get("arbitro_principal")),
                "home_score": home_score,
                "away_score": away_score,
                "possession_home": pct_to_decimal(row.get("posesion_local")),
                "possession_away": pct_to_decimal(row.get("posesion_visitante")),
            }
        )
        team_lookup = {soft_norm(home_team): home_team_id, soft_norm(away_team): away_team_id}
        for player_name, team_id in parse_lineups(row.get("planteles"), team_lookup):
            player_id = stable_id("player", player_name)
            append_unique(
                player_rows,
                {"player_id": player_id, "player_name": player_name, "nationality": "NULL", "age": "NULL", "height_cm": "NULL", "weight_kg": "NULL", "position": "NULL", "team_id": team_id},
                ["player_id"],
            )
            append_unique(
                player_match_rows,
                {**{col: "NULL" for col in PLAYER_MATCH_COLUMNS}, "player_id": player_id, "match_id": match_id, "team_id": team_id},
                ["player_id", "match_id"],
            )
        goal_rows.extend(parse_goal_events(row.get("goles"), match_id))
    return match_rows, team_rows["items"], player_rows["items"], goal_rows, player_match_rows["items"]


def collect_2021_2022_stats():
    base = RAW_DIR / "2021 - 2022 Data"
    season = "2021-2022"
    merged = {}
    gk_rows = []

    def key(row):
        return (clean_text(row.get("player_name")), clean_text(row.get("club")))

    mappings = {
        "key_stats.csv": {"match_played": "matches_played", "minutes_played": "minutes_played", "goals": "goals", "assists": "assists"},
        "attempts.csv": {"total_attempts": "shots", "on_target": "shots_on_target"},
        "defending.csv": {"tackles": "tackles", "balls_recoverd": "interceptions"},
        "disciplinary.csv": {"fouls_committed": "fouls_committed", "yellow": "yellow_cards", "red": "red_cards"},
        "distributon.csv": {"pass_completed": "passes_completed", "pass_attempted": "passes_attempted"},
    }
    for filename, columns in mappings.items():
        df = read_csv(base / filename)
        for _, row in df.iterrows():
            item = merged.setdefault(key(row), {"season": season, "player_name": clean_text(row.get("player_name")), "club": clean_text(row.get("club")), "position": clean_text(row.get("position"))})
            for src, dest in columns.items():
                value = to_number(row.get(src))
                if value != "NULL":
                    item[dest] = value

    gk = read_csv(base / "goalkeeping.csv")
    for _, row in gk.iterrows():
        player_id = stable_id("player", row.get("player_name"))
        gk_rows.append(
            {
                "player_id": player_id,
                "season": season,
                "saves": to_number(row.get("saved")),
                "goals_conceded": to_number(row.get("conceded")),
                "clean_sheets": to_number(row.get("cleansheets")),
                "penalty_saves": to_number(row.get("saved_penalties")),
                "punches": to_number(row.get("punches made")),
            }
        )
    return list(merged.values()), gk_rows


def collect_fbref_stats():
    path = RAW_DIR / "2021-2022 Football Player Stats.csv"
    df = read_csv(path, sep=";")
    rows = []
    for _, row in df.iterrows():
        if clean_text(row.get("Comp")) not in TOP_LEAGUE_NAMES | {"Champions Lg"}:
            continue
        rows.append(
            {
                "season": "2021-2022",
                "player_name": clean_text(row.get("Player")),
                "club": clean_text(row.get("Squad")),
                "nationality": clean_text(row.get("Nation")),
                "age": to_number(row.get("Age")),
                "position": clean_text(row.get("Pos")),
                "matches_played": to_number(row.get("MP")),
                "minutes_played": to_number(row.get("Min")),
                "goals": to_number(row.get("Goals")),
                "assists": to_number(row.get("Assists")),
                "shots": to_number(row.get("Shots")),
                "shots_on_target": to_number(row.get("SoT")),
                "passes_completed": to_number(row.get("PasTotCmp")),
                "passes_attempted": to_number(row.get("PasTotAtt")),
                "tackles": to_number(row.get("Tkl")),
                "interceptions": to_number(row.get("Int")),
                "fouls_committed": to_number(row.get("Fls")),
                "yellow_cards": to_number(row.get("CrdY")),
                "red_cards": to_number(row.get("CrdR")),
            }
        )
    return rows


def collect_2025_stats():
    base = RAW_DIR / "2025 Champions"
    players = read_csv(base / "DAY_4" / "players_data.csv")
    teams = read_csv(base / "teams_data.csv")
    team_map = {clean_text(row.get("team_id")): clean_text(row.get("team")) for _, row in teams.iterrows()}
    player_meta = {}
    for _, row in players.iterrows():
        player_meta[clean_text(row.get("id_player"))] = {
            "player_name": clean_text(row.get("player_name")),
            "club": team_map.get(clean_text(row.get("id_team")), clean_text(row.get("id_team"))),
            "position": clean_text(row.get("position")),
            "nationality": clean_text(row.get("nationality")),
            "age": to_number(row.get("age")),
            "height_cm": to_number(row.get("height(cm)")),
            "weight_kg": to_number(row.get("weight(kg)")),
        }
    season_rows = {pid: {"season": "2025-2026", **meta} for pid, meta in player_meta.items()}
    gk_rows = []

    files = {
        "key_stats_data.csv": {"minutes_played": "minutes_played", "matches_appareance": "matches_played", "distance_covered(km/h)": "distance_covered"},
        "goals_data.csv": {"goals": "goals"},
        "attacking_data.csv": {"assists": "assists"},
        "attempts_data.csv": {"total_attempts": "shots", "attempts_on_target": "shots_on_target"},
        "defending_data.csv": {"tackles": "tackles", "balls_recovered": "interceptions"},
        "disciplinary_data.csv": {"fouls_committed": "fouls_committed", "yellow_cards": "yellow_cards", "red_cards": "red_cards"},
        "distribution_data.csv": {"passes_completed": "passes_completed", "passes_attempted": "passes_attempted"},
    }
    for filename, columns in files.items():
        df = read_csv(base / "DAY_4" / filename)
        for _, row in df.iterrows():
            pid = clean_text(row.get("id_player"))
            item = season_rows.setdefault(pid, {"season": "2025-2026", **player_meta.get(pid, {})})
            for src, dest in columns.items():
                value = to_number(row.get(src))
                if value != "NULL":
                    item[dest] = value
    gk = read_csv(base / "DAY_4" / "goalkeeping_data.csv")
    for _, row in gk.iterrows():
        meta = player_meta.get(clean_text(row.get("id_player")), {})
        player_id = stable_id("player", meta.get("player_name", row.get("id_player")))
        gk_rows.append(
            {
                "player_id": player_id,
                "season": "2025-2026",
                "saves": to_number(row.get("saves")),
                "goals_conceded": to_number(row.get("goals_conceded")),
                "clean_sheets": to_number(row.get("clean_sheets")),
                "penalty_saves": to_number(row.get("saves_on_penalty")),
                "punches": to_number(row.get("punches_made")),
            }
        )
    return list(season_rows.values()), gk_rows


def collect_xlsx():
    path = RAW_DIR / "UEFA Champions League 2016-2022 Data.xlsx"
    if not path.exists():
        return [], [], [], []
    sheets = pd.read_excel(path, sheet_name=None)
    teams, players, matches, goals = [], [], [], []
    match_id_map = {}
    player_id_map = {}
    for _, row in sheets.get("teams", pd.DataFrame()).iterrows():
        team_id = stable_id("team", row.get("TEAM_NAME"))
        teams.append({"team_id": team_id, "team_name": clean_text(row.get("TEAM_NAME")), "country": clean_text(row.get("COUNTRY")), "logo": "NULL"})
    for _, row in sheets.get("players", pd.DataFrame()).iterrows():
        name = f"{clean_text(row.get('FIRST_NAME'))} {clean_text(row.get('LAST_NAME'))}".replace("NULL", "").strip()
        team_id = stable_id("team", row.get("TEAM"))
        player_id = stable_id("player", name)
        player_id_map[clean_text(row.get("PLAYER_ID"))] = (player_id, name)
        players.append(
            {
                "player_id": player_id,
                "player_name": name,
                "nationality": clean_text(row.get("NATIONALITY")),
                "age": "NULL",
                "height_cm": to_number(row.get("HEIGHT")),
                "weight_kg": to_number(row.get("WEIGHT")),
                "position": clean_text(row.get("POSITION")),
                "team_id": team_id,
            }
        )
    for _, row in sheets.get("matches", pd.DataFrame()).iterrows():
        home = clean_text(row.get("HOME_TEAM"))
        away = clean_text(row.get("AWAY_TEAM"))
        match_id = stable_id("match", row.get("SEASON"), row.get("DATE_TIME"), home, away)
        match_id_map[clean_text(row.get("MATCH_ID"))] = match_id
        date_value = pd.to_datetime(row.get("DATE_TIME"), errors="coerce")
        matches.append(
            {
                "match_id": match_id,
                "season": clean_text(row.get("SEASON")),
                "competition": "UEFA Champions League",
                "data_scope": "champions_league",
                "source": "UEFA Champions League 2016-2022 Data.xlsx",
                "date": date_value.strftime("%d-%m-%Y") if not pd.isna(date_value) else clean_text(row.get("DATE_TIME")),
                "home_team_id": stable_id("team", home),
                "away_team_id": stable_id("team", away),
                "stadium": clean_text(row.get("STADIUM")),
                "city": "NULL",
                "country": "NULL",
                "referee": "NULL",
                "home_score": to_number(row.get("HOME_TEAM_SCORE")),
                "away_score": to_number(row.get("AWAY_TEAM_SCORE")),
                "possession_home": "NULL",
                "possession_away": "NULL",
            }
        )
    for _, row in sheets.get("goals", pd.DataFrame()).iterrows():
        raw_mid = clean_text(row.get("MATCH_ID"))
        raw_pid = clean_text(row.get("PID"))
        match_id = match_id_map.get(raw_mid, raw_mid)
        player_id, player_name = player_id_map.get(raw_pid, (stable_id("player", raw_pid), raw_pid))
        goal_id = stable_id("goal", match_id, player_id, row.get("DURATION"))
        goals.append(
            {
                "goal_id": goal_id,
                "match_id": match_id,
                "minute": to_number(row.get("DURATION")),
                "goal_type": clean_text(row.get("GOAL_DESC")).casefold().replace(" ", "_"),
                "player_id": player_id,
                "assist_player_id": stable_id("player", row.get("ASSIST")) if clean_text(row.get("ASSIST")) != "NULL" else "NULL",
                "event_type": "goal",
                "player_name": player_name,
                "is_penalty": "penalty" in clean_text(row.get("GOAL_DESC")).casefold(),
                "is_own_goal": "own" in clean_text(row.get("GOAL_DESC")).casefold(),
            }
        )
    return teams, players, matches, goals


def parse_football_data_date(value):
    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return clean_text(value)
    return parsed.strftime("%d-%m-%Y")


def collect_football_data_leagues():
    base = RAW_DIR / "football-data"
    if not base.exists():
        return [], []
    matches = []
    team_rows = {"_seen": set(), "items": []}
    for csv_path in sorted(base.glob("*/*.csv")):
        league_code = csv_path.stem
        if league_code not in FOOTBALL_DATA_LEAGUES:
            continue
        meta = FOOTBALL_DATA_LEAGUES[league_code]
        season_code = csv_path.parent.name
        if len(season_code) != 4:
            continue
        start_year = 2000 + int(season_code[:2]) if int(season_code[:2]) < 80 else 1900 + int(season_code[:2])
        season = f"{start_year}-{start_year + 1}"
        df = read_csv(csv_path, usecols=lambda column: column in FOOTBALL_DATA_COLUMNS)
        for _, row in df.iterrows():
            home = clean_text(row.get("HomeTeam"))
            away = clean_text(row.get("AwayTeam"))
            if home == "NULL" or away == "NULL":
                continue
            home_team_id = stable_id("team", home)
            away_team_id = stable_id("team", away)
            append_unique(
                team_rows,
                {"team_id": home_team_id, "team_name": home, "country": meta["country"], "logo": "NULL"},
                ["team_id"],
            )
            append_unique(
                team_rows,
                {"team_id": away_team_id, "team_name": away, "country": meta["country"], "logo": "NULL"},
                ["team_id"],
            )
            date = parse_football_data_date(row.get("Date"))
            match_id = stable_id("match", season, meta["competition"], date, home, away)
            matches.append(
                {
                    "match_id": match_id,
                    "season": season,
                    "competition": meta["competition"],
                    "data_scope": "domestic_league",
                    "source": f"football-data.co.uk/{season_code}/{league_code}.csv",
                    "date": date,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "stadium": "NULL",
                    "city": "NULL",
                    "country": meta["country"],
                    "referee": clean_text(row.get("Referee")),
                    "home_score": to_number(row.get("FTHG")),
                    "away_score": to_number(row.get("FTAG")),
                    "possession_home": "NULL",
                    "possession_away": "NULL",
                }
            )
    return matches, team_rows["items"]


def infer_openfootball_meta(path):
    text = str(path).replace("\\", "/")
    name = path.name.lower()
    parent = path.parent.name.lower()
    stem = path.stem.lower()

    if "/champions-league/" in text:
        if stem.startswith("cl"):
            return "UEFA Champions League", "international_club", "Europe"
        if stem.startswith("el"):
            return "UEFA Europa League", "international_club", "Europe"
        if stem.startswith("conf"):
            return "UEFA Conference League", "international_club", "Europe"
    if "/copa-libertadores/" in text:
        if "copal" in stem:
            return "CONMEBOL Copa Libertadores", "international_club", "South America"
        if "copas" in stem:
            return "CONMEBOL Copa Sudamericana", "international_club", "South America"
    if "/argentina/" in text and "_ar1" in stem:
        return "Liga Profesional Argentina", "domestic_league", "Argentina"
    if "/brazil/" in text and "_br1" in stem:
        return "Brasileirao", "domestic_league", "Brazil"
    if "/colombia/" in text and "_co1" in stem:
        return "Liga BetPlay", "domestic_league", "Colombia"
    if "/ecuador/" in text and "_ec1" in stem:
        return "LigaPro", "domestic_league", "Ecuador"
    if "/paraguay/" in text and "_py1" in stem:
        return "Primera Division Paraguay", "domestic_league", "Paraguay"
    if "/worldcup/" in text and name == "cup.txt":
        return "FIFA World Cup", "international", "World"
    if "/euro/" in text and name == "euro.txt":
        return "UEFA Eurocup", "international", "Europe"
    if "/copa-america/" in text and name == "copa.txt":
        return "CONMEBOL Copa America", "international", "South America"
    return None, None, None


def infer_openfootball_season(path, header=""):
    for part in path.parts:
        match = re.match(r"^(\d{4})-(\d{2})$", part)
        if match:
            return f"{match.group(1)}-{int(match.group(1)[:2] + match.group(2)):04d}"
        match = re.match(r"^(\d{4})(?:--.*)?$", part)
        if match:
            return match.group(1)
    match = re.search(r"(20\d{2}|19\d{2})", header)
    return match.group(1) if match else "NULL"


def split_team_country(name):
    text = clean_text(name)
    match = re.search(r"\s+\(([A-Z]{3})\)$", text)
    if match:
        code = match.group(1)
        return text[: match.start()].strip(), COUNTRY_CODES.get(code, "NULL")
    return text, "NULL"


def openfootball_date(line, season):
    match = re.search(r"\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+([A-Za-z]{3})/?\s*(\d{1,2})(?:\s+(\d{4}))?", line)
    if not match:
        return None
    year = match.group(4)
    if year is None and season != "NULL":
        year = season[:4]
    return " ".join(part for part in [match.group(1), match.group(2), match.group(3), year] if part)


def parse_score_token(score_text):
    if "pen." in score_text:
        paren = re.search(r"\((\d+)-(\d+)\)", score_text)
        if paren:
            return int(paren.group(1)), int(paren.group(2))
    match = re.search(r"(\d+)-(\d+)", score_text)
    if not match:
        return "NULL", "NULL"
    return int(match.group(1)), int(match.group(2))


def parse_openfootball_match_line(line):
    if re.search(r"\s+v\s+", line):
        match = re.match(r"^\s*(?:\(\d+\)\s*)?(?:(?:\d{1,2}[.:]\d{2})\s+)?(.+?)\s+v\s+(.+?)\s+(\d+-\d+(?:\s+\([^)]+\))?)", line)
        if match:
            home, away = split_team_country(match.group(1))[0], split_team_country(match.group(2))[0]
            return home, away, match.group(3), "NULL", "NULL"

    match = re.match(
        r"^\s*(?:\(\d+\)\s*)?(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\w+/?\d{1,2}\s+)?"
        r"(?:(?:\d{1,2}:\d{2})\s+)?(.+?)\s+"
        r"(\d+-\d+(?:\s+(?:pen\.|a\.e\.t\.)\s+\(\d+-\d+\)|\s+\([^)]+\))?)\s+"
        r"(.+?)(?:\s+@|$)",
        line,
    )
    if not match:
        return None
    home = clean_text(match.group(1))
    away = clean_text(match.group(3))
    if home.startswith("Winner ") or away.startswith("Winner ") or " - " in line:
        return None
    stadium = "NULL"
    city = "NULL"
    if "@" in line:
        venue = clean_text(line.split("@", 1)[1].split("#", 1)[0])
        if "," in venue:
            stadium, city = [clean_text(part) for part in venue.split(",", 1)]
        else:
            stadium = venue
    return home, away, match.group(2), stadium, city


def parse_goal_scorers(goal_line, match_id, home_team_id, away_team_id):
    text = clean_text(goal_line).strip("()[] ")
    if text == "NULL" or ";" not in text:
        return [], []
    rows = []
    players = []
    for team_id, side in [(home_team_id, text.split(";", 1)[0]), (away_team_id, text.split(";", 1)[1])]:
        if side.strip() in {"-", ""}:
            continue
        for match in re.finditer(r"([A-Za-zÀ-ÖØ-öø-ÿ.'’\-\s]+?)\s+(\d{1,3}(?:\+\d{1,2})?)'?\s*(?:\((pen\.|p|o\.g\.|og)\))?", side):
            player_name = clean_text(match.group(1).replace(",", " "))
            minute = match.group(2)
            marker = (match.group(3) or "").lower()
            if player_name == "NULL" or len(player_name) < 2:
                continue
            goal_type = "penalty" if marker in {"pen.", "p"} else "own_goal" if marker in {"o.g.", "og"} else "regular"
            player_id = stable_id("player", player_name)
            players.append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "nationality": "NULL",
                    "age": "NULL",
                    "height_cm": "NULL",
                    "weight_kg": "NULL",
                    "position": "NULL",
                    "team_id": team_id,
                }
            )
            rows.append(
                {
                    "goal_id": stable_id("goal", match_id, player_name, minute, goal_type),
                    "match_id": match_id,
                    "minute": minute,
                    "goal_type": goal_type,
                    "player_id": player_id,
                    "assist_player_id": "NULL",
                    "event_type": "goal",
                    "player_name": player_name,
                    "is_penalty": goal_type == "penalty",
                    "is_own_goal": goal_type == "own_goal",
                }
            )
    return rows, players


def parse_openfootball_squad(path):
    text = path.read_text(encoding="utf-8", errors="ignore")
    header = next((line for line in text.splitlines() if line.lstrip().startswith("=")), "")
    team_match = re.search(r"=\s+(.+?)\s+-\s+", header)
    team = clean_text(team_match.group(1)) if team_match else clean_text(path.stem.replace("-", " "))
    team_id = stable_id("team", team)
    rows = []
    for line in text.splitlines():
        match = re.match(r"^\s*\d+,\s*([^,]+),\s*([A-Z]{2}),.*?\bb\.\s*(\d{4})", line)
        if not match:
            continue
        player_name = clean_text(match.group(1))
        birth_year = int(match.group(3))
        rows.append(
            {
                "player_id": stable_id("player", player_name),
                "player_name": player_name,
                "nationality": team,
                "age": max(datetime.now().year - birth_year, 0),
                "height_cm": "NULL",
                "weight_kg": "NULL",
                "position": match.group(2),
                "team_id": team_id,
            }
        )
    team_row = {"team_id": team_id, "team_name": team, "country": team, "logo": "NULL"}
    return rows, [team_row] if rows else []


def collect_openfootball():
    base = RAW_DIR / "openfootball"
    if not base.exists():
        return [], [], [], []

    matches = []
    goals = []
    players = []
    team_rows = {"_seen": set(), "items": []}
    for path in sorted(base.rglob("*.txt")):
        if "squads" in path.parts:
            squad_players, squad_teams = parse_openfootball_squad(path)
            players.extend(squad_players)
            for row in squad_teams:
                append_unique(team_rows, row, ["team_id"])
            continue

        competition, data_scope, default_country = infer_openfootball_meta(path)
        if competition is None:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        header = next((line for line in text.splitlines() if line.lstrip().startswith("=")), "")
        season = infer_openfootball_season(path, header)
        if season != "NULL":
            start_year = int(season[:4])
            if start_year < 2004 or start_year > 2025:
                continue
        current_date = "NULL"
        current_match = None
        for line in text.splitlines():
            date = openfootball_date(line, season)
            if date:
                current_date = date
            parsed = parse_openfootball_match_line(line)
            if parsed:
                home, away, score_text, stadium, city = parsed
                home_name, home_country = split_team_country(home)
                away_name, away_country = split_team_country(away)
                home_team_id = stable_id("team", home_name)
                away_team_id = stable_id("team", away_name)
                home_score, away_score = parse_score_token(score_text)
                match_id = stable_id("match", season, competition, current_date, home_name, away_name)
                append_unique(
                    team_rows,
                    {"team_id": home_team_id, "team_name": home_name, "country": home_country if home_country != "NULL" else default_country, "logo": "NULL"},
                    ["team_id"],
                )
                append_unique(
                    team_rows,
                    {"team_id": away_team_id, "team_name": away_name, "country": away_country if away_country != "NULL" else default_country, "logo": "NULL"},
                    ["team_id"],
                )
                matches.append(
                    {
                        "match_id": match_id,
                        "season": season,
                        "competition": competition,
                        "data_scope": data_scope,
                        "source": f"openfootball/{path.relative_to(base).as_posix()}",
                        "date": current_date,
                        "home_team_id": home_team_id,
                        "away_team_id": away_team_id,
                        "stadium": stadium,
                        "city": city,
                        "country": default_country,
                        "referee": "NULL",
                        "home_score": home_score,
                        "away_score": away_score,
                        "possession_home": "NULL",
                        "possession_away": "NULL",
                    }
                )
                current_match = (match_id, home_team_id, away_team_id)
                continue
            if current_match and ("[" in line or "(" in line) and re.search(r"\d{1,3}(?:\+\d{1,2})?'?", line):
                goal_rows, player_rows = parse_goal_scorers(line, *current_match)
                goals.extend(goal_rows)
                players.extend(player_rows)
                current_match = None

    return matches, team_rows["items"], players, goals


STATSBOMB_COMPETITIONS = {
    "1. Bundesliga": ("Bundesliga", "domestic_league"),
    "Champions League": ("UEFA Champions League", "champions_league"),
    "FIFA World Cup": ("FIFA World Cup", "international"),
    "UEFA Euro": ("UEFA Eurocup", "international"),
    "Copa America": ("CONMEBOL Copa America", "international"),
    "La Liga": ("La Liga", "domestic_league"),
    "Ligue 1": ("Ligue 1", "domestic_league"),
    "Premier League": ("Premier League", "domestic_league"),
    "Serie A": ("Serie A", "domestic_league"),
}


def statsbomb_season_name(value):
    text = clean_text(value).replace("/", "-")
    if re.match(r"^\d{4}-\d{4}$", text):
        return text
    return text


def statsbomb_player_row(player, team_id):
    return {
        "player_id": stable_id("player", player.get("name")),
        "player_name": clean_text(player.get("name")),
        "nationality": "NULL",
        "age": "NULL",
        "height_cm": "NULL",
        "weight_kg": "NULL",
        "position": "NULL",
        "team_id": team_id,
    }


def collect_statsbomb(max_events=1_600_000):
    base = RAW_DIR / "statsbomb" / "data"
    if not base.exists():
        return [], [], [], []

    matches = []
    goals_events = []
    players = []
    team_rows = {"_seen": set(), "items": []}
    match_lookup = {}

    for matches_path in sorted(base.glob("*/*/matches.json")):
        with matches_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        for row in payload:
            comp_name = row.get("competition", {}).get("competition_name")
            if comp_name not in STATSBOMB_COMPETITIONS:
                continue
            competition, data_scope = STATSBOMB_COMPETITIONS[comp_name]
            season = statsbomb_season_name(row.get("season", {}).get("season_name"))
            if season != "NULL":
                start_year = int(season[:4])
                if start_year < 2004 or start_year > 2025:
                    continue
            home_name = clean_text(row.get("home_team", {}).get("home_team_name"))
            away_name = clean_text(row.get("away_team", {}).get("away_team_name"))
            home_country = clean_text(row.get("home_team", {}).get("country", {}).get("name"))
            away_country = clean_text(row.get("away_team", {}).get("country", {}).get("name"))
            home_team_id = stable_id("team", home_name)
            away_team_id = stable_id("team", away_name)
            append_unique(team_rows, {"team_id": home_team_id, "team_name": home_name, "country": home_country, "logo": "NULL"}, ["team_id"])
            append_unique(team_rows, {"team_id": away_team_id, "team_name": away_name, "country": away_country, "logo": "NULL"}, ["team_id"])
            match_id = stable_id("match", "statsbomb", row.get("match_id"))
            stadium = clean_text(row.get("stadium", {}).get("name"))
            country = clean_text(row.get("stadium", {}).get("country", {}).get("name"))
            referee = clean_text(row.get("referee", {}).get("name"))
            matches.append(
                {
                    "match_id": match_id,
                    "season": season,
                    "competition": competition,
                    "data_scope": data_scope,
                    "source": f"statsbomb/{matches_path.relative_to(base).as_posix()}",
                    "date": clean_text(row.get("match_date")),
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "stadium": stadium,
                    "city": "NULL",
                    "country": country if country != "NULL" else clean_text(row.get("competition", {}).get("country_name")),
                    "referee": referee,
                    "home_score": to_number(row.get("home_score")),
                    "away_score": to_number(row.get("away_score")),
                    "possession_home": "NULL",
                    "possession_away": "NULL",
                }
            )
            match_lookup[str(row.get("match_id"))] = {
                "match_id": match_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "teams": {home_name: home_team_id, away_name: away_team_id},
            }

    for event_path in sorted(base.glob("*/*/events/*.json")):
        raw_match_id = event_path.stem
        match_info = match_lookup.get(raw_match_id)
        if match_info is None:
            continue
        with event_path.open("r", encoding="utf-8") as f:
            events = json.load(f)
        for event in events:
            if max_events is not None and len(goals_events) >= max_events:
                return matches, team_rows["items"], players, goals_events
            player = event.get("player")
            team = clean_text(event.get("team", {}).get("name"))
            if not player or clean_text(player.get("name")) == "NULL":
                continue
            team_id = match_info["teams"].get(team, stable_id("team", team))
            player_id = stable_id("player", player.get("name"))
            event_type = clean_text(event.get("type", {}).get("name"))
            shot = event.get("shot", {})
            goal_type = clean_text(shot.get("outcome", {}).get("name")) if event_type == "Shot" else event_type
            if event_type == "Shot" and goal_type == "Goal":
                goal_type = "regular"
            players.append(statsbomb_player_row(player, team_id))
            goals_events.append(
                {
                    "goal_id": stable_id("event", event.get("id")),
                    "match_id": match_info["match_id"],
                    "minute": to_number(event.get("minute")),
                    "goal_type": goal_type.casefold().replace(" ", "_"),
                    "player_id": player_id,
                    "assist_player_id": "NULL",
                    "event_type": event_type.casefold().replace(" ", "_"),
                    "player_name": clean_text(player.get("name")),
                    "is_penalty": clean_text(shot.get("type", {}).get("name")).casefold() == "penalty",
                    "is_own_goal": event_type == "Own Goal For",
                }
            )
    return matches, team_rows["items"], players, goals_events


def merge_player_season_rows(*sources):
    merged = {}
    player_rows = {"_seen": set(), "items": []}
    team_rows = {"_seen": set(), "items": []}
    for source in sources:
        for row in source:
            player_name = clean_text(row.get("player_name"))
            club = clean_text(row.get("club"))
            player_id = stable_id("player", player_name)
            team_id = stable_id("team", club) if club != "NULL" else "NULL"
            append_unique(team_rows, {"team_id": team_id, "team_name": club, "country": "NULL", "logo": "NULL"}, ["team_id"])
            append_unique(
                player_rows,
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "nationality": clean_text(row.get("nationality")),
                    "age": row.get("age", "NULL"),
                    "height_cm": row.get("height_cm", "NULL"),
                    "weight_kg": row.get("weight_kg", "NULL"),
                    "position": clean_text(row.get("position")),
                    "team_id": team_id,
                },
                ["player_id"],
            )
            key = (player_id, clean_text(row.get("season")))
            item = merged.setdefault(key, {col: "NULL" for col in PLAYER_SEASON_COLUMNS})
            item["player_id"], item["season"] = key
            for col in PLAYER_SEASON_COLUMNS:
                value = row.get(col, "NULL")
                if item.get(col, "NULL") == "NULL" and clean_text(value) != "NULL":
                    item[col] = value
    return list(merged.values()), player_rows["items"], team_rows["items"]


def frame(rows, columns, subset):
    df = pd.DataFrame(rows)
    for column in columns:
        if column not in df.columns:
            df[column] = "NULL"
    if df.empty:
        return pd.DataFrame(columns=columns)
    df = df[columns].drop_duplicates(subset=subset, keep="first")
    return normalize_invalid_missing_values(df)


def normalize_invalid_missing_values(df):
    df = df.copy()
    if {"possession_home", "possession_away"}.issubset(df.columns):
        df[["possession_home", "possession_away"]] = df[["possession_home", "possession_away"]].astype("object")
        home = pd.to_numeric(df["possession_home"].replace("NULL", pd.NA), errors="coerce")
        away = pd.to_numeric(df["possession_away"].replace("NULL", pd.NA), errors="coerce")
        invalid_pair = home.notna() & away.notna() & (home == 0) & (away == 0)
        df.loc[invalid_pair, ["possession_home", "possession_away"]] = "NULL"
    if {"passes_completed", "passes_attempted"}.issubset(df.columns):
        completed = pd.to_numeric(df["passes_completed"].replace("NULL", pd.NA), errors="coerce")
        attempted = pd.to_numeric(df["passes_attempted"].replace("NULL", pd.NA), errors="coerce")
        invalid = completed.notna() & attempted.notna() & (completed > attempted)
        columns = [column for column in ["passes_completed", "passes_attempted", "pass_accuracy"] if column in df.columns]
        df.loc[invalid, columns] = "NULL"
    if {"shots", "shots_on_target"}.issubset(df.columns):
        shots = pd.to_numeric(df["shots"].replace("NULL", pd.NA), errors="coerce")
        on_target = pd.to_numeric(df["shots_on_target"].replace("NULL", pd.NA), errors="coerce")
        invalid = shots.notna() & on_target.notna() & (on_target > shots)
        columns = [column for column in ["shots", "shots_on_target", "shots_off_target"] if column in df.columns]
        df.loc[invalid, columns] = "NULL"
    return df


def parquet_ready_frame(name, df):
    out = df.replace(["NULL", "NOT_AVAILABLE_IN_SOURCE", "NO_ASSIST_OR_NOT_RECORDED"], pd.NA).copy()
    for column in NUMERIC_COLUMNS_BY_OUTPUT.get(name, []):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in BOOLEAN_COLUMNS_BY_OUTPUT.get(name, []):
        if column in out.columns:
            out[column] = out[column].map(lambda value: pd.NA if pd.isna(value) else bool(value)).astype("boolean")
    for column in STRING_COLUMNS_BY_OUTPUT.get(name, []):
        if column in out.columns:
            out[column] = out[column].astype("string")
    return out.convert_dtypes()


def write_outputs(outputs):
    for path in OUTPUTS.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    parquet_outputs = {}
    for name, df in outputs.items():
        csv_path = OUTPUTS[name]
        parquet_path = csv_path.with_suffix(".parquet")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        parquet_ready_frame(name, df).to_parquet(parquet_path, index=False)
        parquet_outputs[name] = str(parquet_path)
    LOGS_DIR.mkdir(exist_ok=True)
    quality_report = write_quality_report(
        outputs,
        LOGS_DIR / "data_quality_report.json",
        null_threshold=NULL_THRESHOLD,
        min_records=MIN_RECORDS_REQUIRED,
    )
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "sources": [
            "cl_2010_2025_completed.csv",
            "2021 - 2022 Data",
            "2025 Champions",
            "UEFA Champions League 2016-2022 Data.xlsx",
            "2021-2022 Football Player Stats.csv",
            "football-data.co.uk requested league CSVs only",
            "openfootball requested competitions",
            "StatsBomb Open Data event files",
        ],
        "outputs": {
            name: {
                "path": str(OUTPUTS[name]),
                "parquet_path": parquet_outputs[name],
                "rows": int(len(df)),
                "columns": int(len(df.columns)),
                "passes_quality_gate": quality_report["datasets"][name]["passes_quality_gate"],
            }
            for name, df in outputs.items()
        },
        "quality_report": str(LOGS_DIR / "data_quality_report.json"),
        "passes_all_quality_gates": quality_report["passes_all_quality_gates"],
        "identity_resolution": getattr(outputs, "attrs", {}).get("identity_resolution", {}),
    }
    with (LOGS_DIR / "build_processed_report.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return report


def build_processed():
    matches, teams, players, goals, match_stats = build_from_completed()
    stats_2021, gk_2021 = collect_2021_2022_stats()
    stats_fbref = collect_fbref_stats()
    stats_2025, gk_2025 = collect_2025_stats()
    x_teams, x_players, x_matches, x_goals = collect_xlsx()
    league_matches, league_teams = collect_football_data_leagues()
    open_matches, open_teams, open_players, open_goals = collect_openfootball()
    sb_matches, sb_teams, sb_players, sb_events = collect_statsbomb()
    season_stats, season_players, season_teams = merge_player_season_rows(stats_2021, stats_fbref, stats_2025)

    outputs = {
        "matches": frame(matches + x_matches + league_matches + open_matches + sb_matches, MATCH_COLUMNS, ["match_id"]),
        "teams": frame(teams + season_teams + x_teams + league_teams + open_teams + sb_teams, TEAM_COLUMNS, ["team_id"]),
        "players": frame(players + season_players + x_players + open_players + sb_players, PLAYER_COLUMNS, ["player_id"]),
        "goals": frame(goals + x_goals + open_goals + sb_events, GOAL_COLUMNS, ["goal_id"]),
        "gk": frame(gk_2021 + gk_2025, GOALKEEPER_COLUMNS, ["player_id", "season"]),
        "match_stats": frame(match_stats, PLAYER_MATCH_COLUMNS, ["player_id", "match_id"]),
        "season_stats": frame(season_stats, PLAYER_SEASON_COLUMNS, ["player_id", "season"]),
    }
    outputs, identity_report = apply_player_aliases(outputs)
    for df in outputs.values():
        df.attrs["identity_resolution"] = identity_report
    outputs_frame = outputs
    outputs_frame_attrs = identity_report
    class OutputDict(dict):
        pass
    wrapped = OutputDict(outputs_frame)
    wrapped.attrs = {"identity_resolution": outputs_frame_attrs}
    return write_outputs(wrapped)


def main():
    report = build_processed()
    print(json.dumps(report["outputs"], indent=2))


if __name__ == "__main__":
    main()
