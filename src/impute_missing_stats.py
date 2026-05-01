import json
from datetime import datetime
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

PLAYER_MATCH_PATH = PROCESSED_DIR / "stats" / "player_match_stats_cleaned.csv"
PLAYER_SEASON_PATH = PROCESSED_DIR / "stats" / "player_season_stats_cleaned.csv"
PLAYERS_PATH = PROCESSED_DIR / "core" / "players_cleaned.csv"
MATCHES_PATH = PROCESSED_DIR / "core" / "matches_cleaned.csv"

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


def is_missing(value) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip().upper() in {"", "NULL", "NAN", "NONE", "NA"}


def numeric(series):
    return pd.to_numeric(series.replace("NULL", pd.NA), errors="coerce")


def fill_if_missing(df, idx, column, value, reason, changes):
    if column not in df.columns:
        df[column] = "NULL"
    df[column] = df[column].astype("object")
    if is_missing(df.at[idx, column]) and not pd.isna(value):
        if isinstance(value, float):
            value = round(value, 4)
        df.at[idx, column] = value
        changes.append({"row": int(idx), "column": column, "value": value, "reason": reason})


def impute_player_match_stats():
    if not PLAYER_MATCH_PATH.exists():
        return {"skipped": str(PLAYER_MATCH_PATH)}

    df = pd.read_csv(PLAYER_MATCH_PATH, keep_default_na=False)
    players = pd.read_csv(PLAYERS_PATH, keep_default_na=False) if PLAYERS_PATH.exists() else pd.DataFrame()
    matches = pd.read_csv(MATCHES_PATH, keep_default_na=False) if MATCHES_PATH.exists() else pd.DataFrame()
    season_stats = pd.read_csv(PLAYER_SEASON_PATH, keep_default_na=False) if PLAYER_SEASON_PATH.exists() else pd.DataFrame()
    if "touches" not in df.columns:
        df["touches"] = "NULL"

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
        "tackles",
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
            estimate = minutes_per_match.get(key, 90)
            fill_if_missing(df, idx, "minutes_played", estimate, "season minutes / matches_played fallback", changes)

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
                continue
            estimate = rate * minutes_now.at[idx] / 90
            fill_if_missing(df, idx, field, estimate, "season per-90 rate scaled by match minutes", changes)

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

    # Grouped per-90 fallback for numeric gaps. This is deliberately conservative:
    # observed values stay fixed, estimates are position medians scaled by minutes.
    if "minutes_played" in df.columns:
        minutes = numeric(df["minutes_played"]).fillna(90).replace(0, 90)
    else:
        minutes = pd.Series([90] * len(df), index=df.index)

    for col in ["dribbles", "offsides", "crosses_completed", "crosses_attempted", "clearances", "fouls_suffered", "touches"]:
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
                continue
            estimate = rate * minutes.at[idx] / 90
            fill_if_missing(df, idx, col, estimate, "position median per-90 scaled by minutes", changes)

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
    df.to_csv(PLAYER_MATCH_PATH, index=False, encoding="utf-8")
    return {"path": str(PLAYER_MATCH_PATH), "changes": changes}


def impute_player_season_stats():
    if not PLAYER_SEASON_PATH.exists():
        return {"skipped": str(PLAYER_SEASON_PATH)}
    df = pd.read_csv(PLAYER_SEASON_PATH, keep_default_na=False)
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
    df.to_csv(PLAYER_SEASON_PATH, index=False, encoding="utf-8")
    return {"path": str(PLAYER_SEASON_PATH), "changes": changes}


def main():
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": "Observed non-empty values are preserved; only missing cells are imputed.",
        "formula_references": FORMULA_REFERENCES,
        "player_match_stats": impute_player_match_stats(),
        "player_season_stats": impute_player_season_stats(),
    }
    LOGS_DIR.mkdir(exist_ok=True)
    out = LOGS_DIR / "imputation_report.json"
    with out.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(json.dumps({"report": str(out)}, indent=2))


if __name__ == "__main__":
    main()
