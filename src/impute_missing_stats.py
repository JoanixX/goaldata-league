import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.build_processed import OUTPUTS, parquet_ready_frame
from src.data_quality import write_quality_report

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

PLAYER_MATCH_PATH = PROCESSED_DIR / "stats" / "player_match_stats_cleaned.csv"
PLAYER_SEASON_PATH = PROCESSED_DIR / "stats" / "player_season_stats_cleaned.csv"
PLAYERS_PATH = PROCESSED_DIR / "core" / "players_cleaned.csv"
MATCHES_PATH = PROCESSED_DIR / "core" / "matches_cleaned.csv"
TEAMS_PATH = PROCESSED_DIR / "core" / "teams_cleaned.csv"
GOALS_PATH = PROCESSED_DIR / "events" / "goals_events_cleaned.csv"
GOALKEEPER_PATH = PROCESSED_DIR / "stats" / "goalkeeper_stats_cleaned.csv"
TOP_LEAGUES_STATS_PATH = RAW_DIR / "2021-2022 Football Player Stats.csv"
TRANSFERMARKT_DUMP_PATH = BASE_DIR / "tests" / "api_diagnostics" / "results" / "transfermarkt" / "raw_dump.json"

FORMULA_REFERENCES = [
    {
        "topic": "missing_data",
        "citation": "Little, R. J. A., & Rubin, D. B. (2002). Statistical Analysis with Missing Data.",
        "url": "https://doi.org/10.1002/9781119013563",
        "use": "Only impute missing cells and keep observed values unchanged.",
    },
    {
        "topic": "multiple_imputation_practice",
        "citation": "van Buuren, S. (2018). Flexible Imputation of Missing Data.",
        "url": "https://doi.org/10.1201/9780429492259",
        "use": "Use related observed variables and grouped medians for missing-at-random style gaps.",
    },
    {
        "topic": "football_per_90_features",
        "citation": "Decroos et al. (2019). Actions Speak Louder than Goals: Valuing Player Actions in Soccer.",
        "url": "https://arxiv.org/abs/1802.07127",
        "use": "Normalize player actions by playing time before comparing or estimating rates.",
    },
    {
        "topic": "single_imputation_improvement",
        "citation": "Khan, S. I., & Hoque, A. S. M. L. (2020). SICE: an improved missing data imputation technique. Journal of Big Data.",
        "url": "https://doi.org/10.1186/s40537-020-00313-w",
        "use": "Use statistically related records and avoid unconditioned constants when imputing missing sports metrics.",
    },
    {
        "topic": "high_dimensional_imputation",
        "citation": "Brini, A., & van den Heuvel, E. R. (2024). Missing Data Imputation with High-Dimensional Data. The American Statistician.",
        "url": "https://doi.org/10.1080/00031305.2023.2259962",
        "use": "Prefer regularized/grouped donor information when many correlated performance fields are missing.",
    },
    {
        "topic": "football_interpolation",
        "citation": "Kontos, P., & Karlis, D. (2023). Football analytics based on player tracking data using interpolation techniques for the prediction of missing coordinates.",
        "url": "https://doi.org/10.36253/ijas-15707",
        "use": "Document football-specific inference separately from directly observed event data.",
    },
]

NUMERIC_FIELDS = [
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
]
TOP_LEAGUE_MAP = {
    "MP": "matches_played",
    "Min": "minutes_played",
    "Goals": "goals",
    "Assists": "assists",
    "Shots": "shots",
    "SoT": "shots_on_target",
    "BlkSh": "shots_blocked",
    "PasTotCmp": "passes_completed",
    "PasTotAtt": "passes_attempted",
    "PasTotCmp%": "pass_accuracy",
    "PasCrs": "crosses_attempted",
    "Crs": "crosses_completed",
    "DriAtt": "dribbles",
    "Off": "offsides",
    "Tkl": "tackles",
    "TklWon": "tackles_won",
    "TklDriPast": "tackles_lost",
    "Int": "interceptions",
    "Clr": "clearances",
    "Fls": "fouls_committed",
    "Fld": "fouls_suffered",
    "CrdY": "yellow_cards",
    "CrdR": "red_cards",
    "Touches": "touches",
}
TOP_LEAGUE_NAMES = {"Premier League", "La Liga", "Ligue 1", "Bundesliga", "Serie A"}
STRUCTURAL_MARKER = "NOT_AVAILABLE_IN_SOURCE"
NO_ASSIST_MARKER = "NO_ASSIST_OR_NOT_RECORDED"
COUNT_FIELDS = {
    "minutes_played",
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
    "touches",
    "matches_played",
    "saves",
    "goals_conceded",
    "clean_sheets",
    "penalty_saves",
    "punches",
}
ZERO_ONLY_AS_MISSING_FIELDS = {
    "shots_blocked",
    "crosses_completed",
    "crosses_attempted",
    "dribbles",
    "offsides",
    "tackles_won",
    "tackles_lost",
    "clearances",
    "fouls_suffered",
}


def is_missing(value) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip().upper() in {
        "",
        "NULL",
        "NAN",
        "NONE",
        "NA",
        STRUCTURAL_MARKER,
        NO_ASSIST_MARKER,
    }


def numeric(series):
    return pd.to_numeric(series.map(lambda value: pd.NA if is_missing(value) else value), errors="coerce")


def nullify_source_unavailable_markers(df):
    df = df.astype("object").copy()
    for column in df.columns:
        df[column] = df[column].map(
            lambda value: "NULL" if str(value).strip().upper() in {STRUCTURAL_MARKER, NO_ASSIST_MARKER} else value
        )
    return df


def coerce_count_value(value):
    if pd.isna(value):
        return pd.NA
    number = float(value)
    if number < 0:
        return pd.NA
    return int(round(number))


def hard_normalize_stats(df, count_fields):
    df = nullify_source_unavailable_markers(df.copy())
    for column in count_fields:
        if column in df.columns:
            df[column] = df[column].astype("object")
            values = numeric(df[column])
            df[column] = values.map(lambda value: "NULL" if pd.isna(value) or value < 0 else int(round(value)))
    if "pass_accuracy" in df.columns:
        df["pass_accuracy"] = df["pass_accuracy"].astype("object")
        values = numeric(df["pass_accuracy"])
        values = values.where(values <= 1, values / 100)
        df["pass_accuracy"] = values.map(lambda value: "NULL" if pd.isna(value) or value < 0 or value > 1 else round(float(value), 4))
    if {"passes_completed", "passes_attempted"}.issubset(df.columns):
        df = df.astype({c: "object" for c in df.columns})
        completed = numeric(df["passes_completed"])
        attempted = numeric(df["passes_attempted"])
        invalid = completed.notna() & attempted.notna() & (completed > attempted)
        cols = [c for c in ["passes_completed", "passes_attempted", "pass_accuracy"] if c in df.columns]
        df.loc[invalid, cols] = "NULL"
    if {"shots", "shots_on_target"}.issubset(df.columns):
        shots = numeric(df["shots"])
        on_target = numeric(df["shots_on_target"])
        invalid = shots.notna() & on_target.notna() & (on_target > shots)
        cols = [c for c in ["shots", "shots_on_target", "shots_off_target", "shots_blocked"] if c in df.columns]
        df.loc[invalid, cols] = "NULL"
    if {"shots", "shots_on_target", "shots_off_target", "shots_blocked"}.issubset(df.columns):
        shots = numeric(df["shots"])
        parts = numeric(df["shots_on_target"]).fillna(0) + numeric(df["shots_off_target"]).fillna(0) + numeric(df["shots_blocked"]).fillna(0)
        invalid = shots.notna() & (parts > shots)
        df.loc[invalid, ["shots_off_target", "shots_blocked"]] = "NULL"
    if {"tackles", "tackles_won"}.issubset(df.columns):
        tackles = numeric(df["tackles"])
        won = numeric(df["tackles_won"])
        invalid = tackles.notna() & won.notna() & (won > tackles)
        cols = [c for c in ["tackles", "tackles_won", "tackles_lost"] if c in df.columns]
        df.loc[invalid, cols] = "NULL"
    return df


def nullify_fake_zero_columns(df, fields=ZERO_ONLY_AS_MISSING_FIELDS):
    df = df.copy()
    changes = []
    for column in fields:
        if column not in df.columns:
            continue
        values = numeric(df[column])
        observed = values.dropna()
        if not observed.empty and (observed == 0).all():
            df[column] = "NULL"
            changes.append(column)
    return df, changes


def usable_rate(field, rate):
    if pd.isna(rate):
        return False
    if field in ZERO_ONLY_AS_MISSING_FIELDS and rate <= 0:
        return False
    return True


def fill_if_missing(df, idx, column, value, reason, changes):
    if column not in df.columns:
        df[column] = "NULL"
    df[column] = df[column].astype("object")
    if is_missing(df.at[idx, column]) and not pd.isna(value):
        if column in COUNT_FIELDS:
            value = coerce_count_value(value)
            if pd.isna(value):
                return
            if column in ZERO_ONLY_AS_MISSING_FIELDS and value == 0:
                return
        elif isinstance(value, float):
            value = round(value, 4)
        if hasattr(value, "item"):
            value = value.item()
        df.at[idx, column] = value
        changes.append({"row": int(idx), "column": column, "value": value, "reason": reason})


def soft_norm(value):
    if is_missing(value):
        return ""
    return " ".join(str(value).casefold().replace(".", " ").replace("-", " ").split())


def read_top_league_priors():
    if not TOP_LEAGUES_STATS_PATH.exists():
        return pd.DataFrame(), {"skipped": str(TOP_LEAGUES_STATS_PATH)}
    df = pd.read_csv(TOP_LEAGUES_STATS_PATH, sep=";", keep_default_na=False, encoding="latin1")
    df = df[df["Comp"].isin(TOP_LEAGUE_NAMES)].copy()
    for src, dest in TOP_LEAGUE_MAP.items():
        if src not in df.columns:
            continue
        df[dest] = pd.to_numeric(df[src].replace("", pd.NA), errors="coerce")
    if "pass_accuracy" in df.columns:
        df["pass_accuracy"] = df["pass_accuracy"].where(df["pass_accuracy"] <= 1, df["pass_accuracy"] / 100)
    if {"shots", "shots_on_target", "shots_blocked"}.issubset(df.columns):
        df["shots_off_target"] = (df["shots"] - df["shots_on_target"] - df["shots_blocked"].fillna(0)).clip(lower=0)
    df["player_key"] = df["Player"].map(soft_norm)
    df["position"] = df["Pos"].astype(str).str.split(",").str[0].str.strip().replace("", "NULL")
    return df, {
        "path": str(TOP_LEAGUES_STATS_PATH),
        "rows": int(len(df)),
        "competitions": sorted(df["Comp"].dropna().unique().tolist()),
    }


def build_top_league_rates(priors):
    if priors.empty:
        return {}, {}
    minutes = priors["minutes_played"].where(priors["minutes_played"] > 0)
    rates = {}
    global_rates = {}
    for field in [field for field in NUMERIC_FIELDS if field not in {"pass_accuracy", "top_speed", "distance_covered"}]:
        if field not in priors.columns:
            continue
        per90 = priors[field] / minutes * 90
        temp = pd.DataFrame({"position": priors["position"], "rate": per90})
        rates[field] = temp.groupby("position")["rate"].median().dropna().to_dict()
        global_rates[field] = per90.median()
    if "pass_accuracy" in priors.columns:
        temp = pd.DataFrame({"position": priors["position"], "rate": priors["pass_accuracy"]})
        rates["pass_accuracy"] = temp.groupby("position")["rate"].median().dropna().to_dict()
        global_rates["pass_accuracy"] = priors["pass_accuracy"].median()
    return rates, global_rates


def read_champions_2025_physical_priors():
    base = RAW_DIR / "2025 Champions" / "DAY_4"
    players_path = base / "players_data.csv"
    key_stats_path = base / "key_stats_data.csv"
    if not players_path.exists():
        return pd.DataFrame(), {"skipped": str(players_path)}
    players = pd.read_csv(players_path, keep_default_na=False)
    players["position"] = players.get("position", "NULL").astype(str).replace("", "NULL")
    for src, dest in [("height(cm)", "height_cm"), ("weight(kg)", "weight_kg"), ("age", "age")]:
        if src in players.columns:
            players[dest] = pd.to_numeric(players[src], errors="coerce")
    if key_stats_path.exists():
        key_stats = pd.read_csv(key_stats_path, keep_default_na=False)
        key_stats = key_stats.rename(columns={"distance_covered(km/h)": "distance_covered", "top_speed": "top_speed"})
        for col in ["distance_covered", "top_speed", "minutes_played", "matches_appareance"]:
            if col in key_stats.columns:
                key_stats[col] = pd.to_numeric(key_stats[col], errors="coerce")
        players = players.merge(key_stats[["id_player", "distance_covered", "top_speed", "minutes_played", "matches_appareance"]], on="id_player", how="left")
        if {"distance_covered", "matches_appareance"}.issubset(players.columns):
            matches = players["matches_appareance"].where(players["matches_appareance"] > 0)
            players["distance_covered_per_match"] = players["distance_covered"] / matches
    return players, {"path": str(players_path), "rows": int(len(players))}


def read_transfermarkt_profiles():
    if not TRANSFERMARKT_DUMP_PATH.exists():
        return {}, {"skipped": str(TRANSFERMARKT_DUMP_PATH)}
    with TRANSFERMARKT_DUMP_PATH.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    profiles = {}
    for row in payload:
        name = row.get("player_name")
        if is_missing(name):
            continue
        # Bare surnames/nicknames are too ambiguous for exact profile merges.
        # Keep them in the dump for review, but do not apply them automatically.
        if len(str(name).replace(".", " ").split()) < 2:
            continue
        birth_date = pd.to_datetime(row.get("birth_date"), dayfirst=True, errors="coerce")
        age = "NULL"
        if pd.notna(birth_date):
            today = date.today()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        profile = {
            "nationality": row.get("nationality", "NULL"),
            "age": age,
            "height_cm": row.get("height_cm", "NULL"),
            "position": row.get("position", "NULL"),
        }
        clean_profile = {k: v for k, v in profile.items() if not is_missing(v)}
        if clean_profile:
            profiles[soft_norm(name)] = clean_profile
    return profiles, {
        "path": str(TRANSFERMARKT_DUMP_PATH),
        "rows": len(payload),
        "usable_profiles": len(profiles),
    }


def impute_player_match_stats():
    if not PLAYER_MATCH_PATH.exists():
        return {"skipped": str(PLAYER_MATCH_PATH)}

    df = nullify_source_unavailable_markers(pd.read_csv(PLAYER_MATCH_PATH, keep_default_na=False))
    df, zero_columns = nullify_fake_zero_columns(df)
    players = pd.read_csv(PLAYERS_PATH, keep_default_na=False) if PLAYERS_PATH.exists() else pd.DataFrame()
    matches = pd.read_csv(MATCHES_PATH, keep_default_na=False) if MATCHES_PATH.exists() else pd.DataFrame()
    season_stats = pd.read_csv(PLAYER_SEASON_PATH, keep_default_na=False) if PLAYER_SEASON_PATH.exists() else pd.DataFrame()
    if "touches" not in df.columns:
        df["touches"] = "NULL"
    priors, priors_report = read_top_league_priors()
    top_league_rates, top_league_global_rates = build_top_league_rates(priors)

    if not players.empty and "position" in players.columns:
        df = df.merge(players[["player_id", "position"]], on="player_id", how="left")
    else:
        df["position"] = "NULL"
    if not matches.empty and "season" in matches.columns:
        df = df.merge(matches[["match_id", "season"]], on="match_id", how="left")
    else:
        df["season"] = "NULL"

    changes = []
    rate_fields = [
        "goals",
        "assists",
        "shots",
        "shots_on_target",
        "passes_completed",
        "passes_attempted",
        "shots_blocked",
        "tackles",
        "tackles_won",
        "tackles_lost",
        "interceptions",
        "fouls_committed",
        "yellow_cards",
        "red_cards",
    ]
    exact_rates = {}
    global_rates = {}
    minutes_per_match = {}
    if not season_stats.empty:
        ss = season_stats.copy()
        ss_minutes = numeric(ss["minutes_played"]) if "minutes_played" in ss.columns else pd.Series([pd.NA] * len(ss))
        ss_matches = numeric(ss["matches_played"]) if "matches_played" in ss.columns else pd.Series([pd.NA] * len(ss))
        safe_minutes = ss_minutes.where(ss_minutes > 0)
        for idx, row in ss.iterrows():
            key = (row.get("player_id"), row.get("season"))
            if pd.notna(ss_minutes.loc[idx]) and pd.notna(ss_matches.loc[idx]) and ss_matches.loc[idx] > 0:
                minutes_per_match[key] = ss_minutes.loc[idx] / ss_matches.loc[idx]
            for field in rate_fields:
                if field not in ss.columns:
                    continue
                value = pd.to_numeric(row.get(field), errors="coerce")
                if pd.isna(value):
                    continue
                if pd.notna(safe_minutes.loc[idx]):
                    exact_rates[(key[0], key[1], field)] = value / safe_minutes.loc[idx] * 90
        for field in rate_fields:
            if field not in ss.columns:
                continue
            values = numeric(ss[field])
            per90 = values / safe_minutes * 90
            global_rates[field] = per90.median()

    for idx, row in df.iterrows():
        key = (row.get("player_id"), row.get("season"))
        if "minutes_played" in df.columns and is_missing(row.get("minutes_played")):
            estimate = minutes_per_match.get(key)
            if pd.isna(estimate):
                position = row.get("position", "NULL")
                estimate = top_league_rates.get("minutes_played", {}).get(position, top_league_global_rates.get("minutes_played", 90))
            fill_if_missing(df, idx, "minutes_played", estimate, "exact season average or top-five-league position median minutes per match", changes)

    # Refresh numeric helper columns after minutes are estimated.
    for col in NUMERIC_FIELDS:
        if col in df.columns:
            df[f"__{col}"] = numeric(df[col])

    minutes_now = numeric(df["minutes_played"]).fillna(90).replace(0, 90) if "minutes_played" in df.columns else pd.Series([90] * len(df), index=df.index)
    for idx, row in df.iterrows():
        key = (row.get("player_id"), row.get("season"))
        for field in rate_fields:
            if field not in df.columns:
                df[field] = "NULL"
            if not is_missing(df.at[idx, field]):
                continue
            rate = exact_rates.get((key[0], key[1], field), global_rates.get(field))
            if pd.isna(rate):
                position = row.get("position", "NULL")
                rate = top_league_rates.get(field, {}).get(position, top_league_global_rates.get(field))
            if not usable_rate(field, rate):
                continue
            estimate = rate * minutes_now.at[idx] / 90
            fill_if_missing(df, idx, field, estimate, "season or top-five-league per-90 rate scaled by match minutes", changes)

    for col in NUMERIC_FIELDS:
        df[f"__{col}"] = numeric(df[col]) if col in df.columns else pd.Series([pd.NA] * len(df), index=df.index)

    for idx, row in df.iterrows():
        shots = row.get("__shots")
        sot = row.get("__shots_on_target")
        blocked = row.get("__shots_blocked")
        if pd.notna(shots) and pd.notna(sot):
            off_target = max(shots - sot - (blocked if pd.notna(blocked) else 0), 0)
            fill_if_missing(df, idx, "shots_off_target", off_target, "shots - shots_on_target - shots_blocked", changes)

        passes_completed = row.get("__passes_completed")
        passes_attempted = row.get("__passes_attempted")
        if pd.notna(passes_completed) and pd.notna(passes_attempted) and passes_attempted > 0:
            fill_if_missing(df, idx, "pass_accuracy", passes_completed / passes_attempted, "passes_completed / passes_attempted", changes)

        tackles = row.get("__tackles")
        tackles_won = row.get("__tackles_won")
        if pd.notna(tackles) and pd.notna(tackles_won):
            fill_if_missing(df, idx, "tackles_lost", max(tackles - tackles_won, 0), "tackles - tackles_won", changes)

        touches_parts = [
            row.get("__passes_attempted"),
            row.get("__dribbles"),
            row.get("__shots"),
            row.get("__tackles"),
            row.get("__interceptions"),
            row.get("__clearances"),
            row.get("__crosses_attempted"),
        ]
        if any(pd.notna(v) for v in touches_parts):
            touches = sum(v for v in touches_parts if pd.notna(v))
            fill_if_missing(df, idx, "touches", touches, "sum observed on-ball actions", changes)

    # Grouped per-90 fallback for action-count gaps. Observed values stay fixed;
    # estimates are rounded counts so mixed per-90/total scales cannot leak out.
    if "minutes_played" in df.columns:
        minutes = numeric(df["minutes_played"]).fillna(90).replace(0, 90)
    else:
        minutes = pd.Series([90] * len(df), index=df.index)

    for col in [
        "dribbles",
        "offsides",
        "crosses_completed",
        "crosses_attempted",
        "shots_blocked",
        "tackles_won",
        "tackles_lost",
        "clearances",
        "fouls_suffered",
        "touches",
    ]:
        if col not in df.columns:
            df[col] = "NULL"
        values = numeric(df[col]) if col in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
        per90 = values / minutes * 90
        temp = pd.DataFrame({"position": df["position"].fillna("NULL"), "rate": per90})
        group_medians = temp.groupby("position")["rate"].median().dropna().to_dict()
        global_median = per90.median()
        for idx, row in df.iterrows():
            if not is_missing(df.at[idx, col]):
                continue
            rate = group_medians.get(row.get("position"), global_median)
            if pd.isna(rate):
                rate = top_league_rates.get(col, {}).get(row.get("position"), top_league_global_rates.get(col))
            if not usable_rate(col, rate):
                continue
            estimate = rate * minutes.at[idx] / 90
            fill_if_missing(df, idx, col, estimate, "position/top-five-league median per-90 scaled by minutes", changes)

    for col in NUMERIC_FIELDS:
        df[f"__{col}"] = numeric(df[col]) if col in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
    for idx, row in df.iterrows():
        touches_parts = [
            row.get("__passes_attempted"),
            row.get("__dribbles"),
            row.get("__shots"),
            row.get("__tackles"),
            row.get("__interceptions"),
            row.get("__clearances"),
            row.get("__crosses_attempted"),
        ]
        if any(pd.notna(v) for v in touches_parts):
            touches = sum(v for v in touches_parts if pd.notna(v))
            fill_if_missing(df, idx, "touches", touches, "sum observed and imputed on-ball actions", changes)

    drop_cols = [c for c in df.columns if c.startswith("__")] + ["position", "season"]
    df = df.drop(columns=drop_cols, errors="ignore")
    df = hard_normalize_stats(df, COUNT_FIELDS.intersection(df.columns))
    df.to_csv(PLAYER_MATCH_PATH, index=False, encoding="utf-8")
    return {
        "path": str(PLAYER_MATCH_PATH),
        "top_league_priors": priors_report,
        "zero_only_columns_reclassified_as_missing": zero_columns,
        "changes": changes,
    }


def impute_player_season_stats():
    if not PLAYER_SEASON_PATH.exists():
        return {"skipped": str(PLAYER_SEASON_PATH)}
    df = nullify_source_unavailable_markers(pd.read_csv(PLAYER_SEASON_PATH, keep_default_na=False))
    priors, priors_report = read_top_league_priors()
    prior_by_name = pd.DataFrame()
    position_medians = {}
    global_medians = {}
    if not priors.empty and PLAYERS_PATH.exists():
        players = pd.read_csv(PLAYERS_PATH, keep_default_na=False)
        players["player_key"] = players["player_name"].map(soft_norm)
        prior_by_name = priors.drop_duplicates("player_key", keep="first").set_index("player_key")
        df = df.merge(players[["player_id", "player_key", "position"]], on="player_id", how="left")
        for field in [
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
        ]:
            if field in priors.columns:
                position_medians[field] = priors.groupby("position")[field].median().dropna().to_dict()
                global_medians[field] = priors[field].median()
    changes = []
    for idx, row in df.iterrows():
        passes_completed = pd.to_numeric(row.get("passes_completed"), errors="coerce")
        passes_attempted = pd.to_numeric(row.get("passes_attempted"), errors="coerce")
        shots = pd.to_numeric(row.get("shots"), errors="coerce")
        shots_on_target = pd.to_numeric(row.get("shots_on_target"), errors="coerce")
        if pd.notna(passes_completed) and pd.isna(passes_attempted):
            fill_if_missing(df, idx, "passes_attempted", passes_completed, "lower-bound passes_attempted from passes_completed", changes)
        if pd.notna(shots_on_target) and pd.isna(shots):
            fill_if_missing(df, idx, "shots", shots_on_target, "lower-bound shots from shots_on_target", changes)
        player_key = row.get("player_key")
        if not prior_by_name.empty and player_key in prior_by_name.index:
            prior = prior_by_name.loc[player_key]
            for field in [
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
            ]:
                if field in df.columns and field in prior.index:
                    fill_if_missing(df, idx, field, prior.get(field), "exact player match from top-five-league FBref prior", changes)
        for field, medians in position_medians.items():
            if field not in df.columns or not is_missing(df.at[idx, field]):
                continue
            estimate = medians.get(row.get("position"), global_medians.get(field))
            fill_if_missing(df, idx, field, estimate, "top-five-league position median season total rounded to count", changes)
    df = df.drop(columns=["player_key", "position"], errors="ignore")
    df = hard_normalize_stats(df, COUNT_FIELDS.intersection(df.columns))
    df.to_csv(PLAYER_SEASON_PATH, index=False, encoding="utf-8")
    return {"path": str(PLAYER_SEASON_PATH), "top_league_priors": priors_report, "changes": changes}


def impute_player_profiles():
    if not PLAYERS_PATH.exists():
        return {"skipped": str(PLAYERS_PATH)}
    players = nullify_source_unavailable_markers(pd.read_csv(PLAYERS_PATH, keep_default_na=False))
    priors, priors_report = read_top_league_priors()
    transfermarkt_profiles, transfermarkt_report = read_transfermarkt_profiles()
    changes = []
    if priors.empty:
        return {"path": str(PLAYERS_PATH), "top_league_priors": priors_report, "changes": changes}
    prior_by_name = priors.drop_duplicates("player_key", keep="first").set_index("player_key")
    for idx, row in players.iterrows():
        prior_key = soft_norm(row.get("player_name"))
        if prior_key in prior_by_name.index:
            prior = prior_by_name.loc[prior_key]
            for dest, src in [("nationality", "Nation"), ("position", "Pos"), ("age", "Age")]:
                if dest in players.columns and is_missing(row.get(dest)) and not is_missing(prior.get(src)):
                    players.at[idx, dest] = prior.get(src)
                    changes.append({"row": int(idx), "column": dest, "value": prior.get(src), "reason": "exact player match from top-five-league FBref prior"})
        if prior_key in transfermarkt_profiles:
            profile = transfermarkt_profiles[prior_key]
            for field, value in profile.items():
                if field in players.columns and is_missing(players.at[idx, field]):
                    fill_if_missing(players, idx, field, value, "exact player match from Transfermarkt profile dump", changes)
    for field in ["age", "height_cm", "weight_kg"]:
        if field in players.columns:
            values = numeric(players[field])
            players[field] = values.map(lambda value: "NULL" if pd.isna(value) or value <= 0 else int(round(value)))
    players.to_csv(PLAYERS_PATH, index=False, encoding="utf-8")
    return {
        "path": str(PLAYERS_PATH),
        "top_league_priors": priors_report,
        "transfermarkt_profiles": transfermarkt_report,
        "policy": "No median or demographic profile imputation; personal fields require exact player-source matches.",
        "changes": changes,
    }


def impute_misc_tables():
    changes = {}
    if MATCHES_PATH.exists():
        matches = nullify_source_unavailable_markers(pd.read_csv(MATCHES_PATH, keep_default_na=False))
        match_changes = []
        matches.to_csv(MATCHES_PATH, index=False, encoding="utf-8")
        changes["matches"] = match_changes

    if TEAMS_PATH.exists():
        teams = nullify_source_unavailable_markers(pd.read_csv(TEAMS_PATH, keep_default_na=False))
        team_changes = []
        country_by_name = {
            soft_norm(row["team_name"]): row["country"]
            for _, row in teams.iterrows()
            if "country" in teams.columns and not is_missing(row.get("country"))
        }
        for idx, row in teams.iterrows():
            if "country" in teams.columns and is_missing(row.get("country")):
                inferred = country_by_name.get(soft_norm(row.get("team_name")))
                if inferred is None:
                    continue
                fill_if_missing(teams, idx, "country", inferred, "same-team country donor or explicit unavailable marker", team_changes)
        teams.to_csv(TEAMS_PATH, index=False, encoding="utf-8")
        changes["teams"] = team_changes

    if GOALS_PATH.exists():
        goals = nullify_source_unavailable_markers(pd.read_csv(GOALS_PATH, keep_default_na=False))
        players = pd.read_csv(PLAYERS_PATH, keep_default_na=False) if PLAYERS_PATH.exists() else pd.DataFrame()
        player_names = dict(zip(players.get("player_id", []), players.get("player_name", []))) if not players.empty else {}
        goal_changes = []
        for idx, row in goals.iterrows():
            if "goal_type" in goals.columns and is_missing(row.get("goal_type")):
                fill_if_missing(goals, idx, "goal_type", "regular", "no penalty/own-goal marker present in source event", goal_changes)
            if "player_name" in goals.columns and is_missing(row.get("player_name")):
                name = player_names.get(row.get("player_id"))
                if name is None:
                    continue
                fill_if_missing(goals, idx, "player_name", name, "player table lookup or explicit unavailable marker", goal_changes)
        goals.to_csv(GOALS_PATH, index=False, encoding="utf-8")
        changes["goals"] = goal_changes

    if GOALKEEPER_PATH.exists():
        gk = nullify_source_unavailable_markers(pd.read_csv(GOALKEEPER_PATH, keep_default_na=False))
        gk_changes = []
        for col in ["saves", "goals_conceded", "clean_sheets", "penalty_saves", "punches"]:
            vals = pd.to_numeric(gk[col].replace("NULL", pd.NA), errors="coerce") if col in gk.columns else pd.Series(dtype="float64")
            median = vals.median()
            for idx, row in gk.iterrows():
                if col in gk.columns and is_missing(row.get(col)):
                    fill_if_missing(gk, idx, col, median, "goalkeeper stat median from observed Champions rows", gk_changes)
        gk.to_csv(GOALKEEPER_PATH, index=False, encoding="utf-8")
        changes["goalkeepers"] = gk_changes

    if PLAYER_MATCH_PATH.exists():
        stats = nullify_source_unavailable_markers(pd.read_csv(PLAYER_MATCH_PATH, keep_default_na=False))
        players = pd.read_csv(PLAYERS_PATH, keep_default_na=False) if PLAYERS_PATH.exists() else pd.DataFrame()
        stat_changes = []
        stats = hard_normalize_stats(stats, COUNT_FIELDS.intersection(stats.columns))
        stats.to_csv(PLAYER_MATCH_PATH, index=False, encoding="utf-8")
        changes["player_match_physical"] = stat_changes
    return changes


def write_processed_parquet_and_quality_report():
    outputs = {}
    paths = {
        "matches": PROCESSED_DIR / "core" / "matches_cleaned.csv",
        "players": PLAYERS_PATH,
        "teams": TEAMS_PATH,
        "goals": GOALS_PATH,
        "gk": GOALKEEPER_PATH,
        "match_stats": PLAYER_MATCH_PATH,
        "season_stats": PLAYER_SEASON_PATH,
    }
    parquet_paths = {}
    for name, path in paths.items():
        if not path.exists():
            continue
        df = pd.read_csv(path, keep_default_na=False)
        outputs[name] = df
        parquet_path = OUTPUTS[name].with_suffix(".parquet")
        parquet_ready_frame(name, df).to_parquet(parquet_path, index=False)
        parquet_paths[name] = str(parquet_path)
    quality = write_quality_report(outputs, LOGS_DIR / "data_quality_report.json")
    return {"parquet_paths": parquet_paths, "quality_report": str(LOGS_DIR / "data_quality_report.json"), "quality": quality}


def main():
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": "Observed non-empty values are preserved; only missing cells are imputed.",
        "formula_references": FORMULA_REFERENCES,
        "player_profiles": impute_player_profiles(),
        "player_match_stats": impute_player_match_stats(),
        "player_season_stats": impute_player_season_stats(),
        "misc_tables": impute_misc_tables(),
    }
    report["outputs"] = write_processed_parquet_and_quality_report()
    LOGS_DIR.mkdir(exist_ok=True)
    out = LOGS_DIR / "imputation_report.json"
    with out.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(json.dumps({"report": str(out)}, indent=2))


if __name__ == "__main__":
    main()
