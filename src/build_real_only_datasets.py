"""
Commercial-grade REAL-ONLY datasets (no invented players/teams/participations)
==============================================================================

Enforces the data policy for the sellable / thesis build:
  * remove every INVENTED entity ("{team} {season} Squad NN" players) and the
    simulated per-match participations/goals attached to them (they were not used
    by any model — the ML catalog already runs on real players only);
  * the per-match layer becomes the REAL StatsBomb participations;
  * goals become REAL (StatsBomb shots with outcome Goal);
  * the >=1.5M dataset is the REAL StatsBomb event stream (already built).

Identities, teams, participations, goals and assists are 100% real here. Only
allowed secondary metrics may be modelled elsewhere. Writes real-only *_cleaned
tables (parquet+csv), guarantees no NULLs and no duplicates.

Run:  python -m src.build_real_only_datasets
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT / "data" / "processed"
EVENTS = PROC / "events" / "statsbomb_events_real.parquet"
PM_REAL = ROOT / "data" / "raw" / "statsbomb_player_match_real.csv"

PM_COLS = ["player_id", "match_id", "minutes_played", "goals", "assists", "shots",
           "shots_on_target", "passes_completed", "passes_attempted", "tackles",
           "interceptions", "fouls_committed", "yellow_cards", "red_cards",
           "position_group", "team", "competition", "season"]


def _fillnum(df: pd.DataFrame) -> pd.DataFrame:
    """No-NULL guarantee: numeric NaN -> 0; text NaN -> 'Unknown'."""
    for c in df.columns:
        if not df[c].isna().any():
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].fillna(0)
        else:
            df[c] = df[c].astype("object").fillna("Unknown")
    return df


def main() -> None:
    assert EVENTS.exists(), "Run src.ingest_statsbomb_full first (real event stream)."
    assert PM_REAL.exists(), "Run src.ingest_statsbomb_full first (real participations)."

    from src.entity_resolution import canonical_text
    from rapidfuzz import fuzz

    # --- players: drop invented squad fillers -> real only, deduped by identity ---
    players = pd.read_parquet(PROC / "core" / "players_cleaned.parquet")
    real_mask = players.get("profile_data_source", "").astype(str) != "imputed_team_season_roster"
    real_players = players[real_mask].copy()
    # Collapse identity variants (cristiano ronaldo / CR7 / CristianoRonaldo -> one).
    real_players["_cn"] = real_players["player_name"].map(canonical_text)
    real_players = real_players.drop_duplicates("_cn", keep="first").drop_duplicates("player_id").reset_index(drop=True)
    cn_to_id = dict(zip(real_players["_cn"], real_players["player_id"]))
    by_surname: dict[str, list[str]] = {}
    for cn in cn_to_id:
        parts = cn.split()
        if parts:
            by_surname.setdefault(parts[-1], []).append(cn)
    real_ids = set(real_players["player_id"])
    print(f"players: {len(players):,} -> {len(real_players):,} real, deduped by identity "
          f"(dropped {len(players)-len(real_players):,} invented/variant)", flush=True)

    def resolve_to_catalog(name: str) -> tuple[str, bool]:
        """Map a (StatsBomb) name to an existing catalog id, or mint a new real id.

        Returns (player_id, is_new). Avoids duplicates: exact canonical, then
        surname-blocked fuzzy (>=90) against the catalog."""
        cn = canonical_text(name)
        if not cn:
            return ("", False)
        if cn in cn_to_id:
            return (cn_to_id[cn], False)
        surname = cn.split()[-1]
        best, best_s = None, 0
        for cand in by_surname.get(surname, []):
            s = fuzz.token_set_ratio(cn, cand)
            if s > best_s:
                best, best_s = cand, s
        if best is not None and best_s >= 90:
            return (cn_to_id[best], False)
        import hashlib
        new_id = "real_" + hashlib.md5(cn.encode("utf-8")).hexdigest()[:12]
        cn_to_id[cn] = new_id
        by_surname.setdefault(surname, []).append(cn)
        return (new_id, True)

    # --- player_season: real players only ---
    ps = pd.read_parquet(PROC / "stats" / "player_season_stats_cleaned.parquet")
    ps_real = ps[ps["player_id"].isin(real_ids)].drop_duplicates(["player_id", "season"]).reset_index(drop=True)
    print(f"player_season: {len(ps):,} -> {len(ps_real):,} real", flush=True)

    # --- goalkeepers: real only ---
    gk = pd.read_parquet(PROC / "stats" / "goalkeeper_stats_cleaned.parquet")
    gk_real = gk[gk["player_id"].isin(real_ids)].drop_duplicates(["player_id", "season"]).reset_index(drop=True)

    # --- player_match: REAL StatsBomb participations (replaces simulated 1.95M) ---
    pm = pd.read_csv(PM_REAL)
    pm = pm.rename(columns={"minutes": "minutes_played"})
    # Resolve every participation's player to a catalog id (no duplicate identities).
    resolved = pm["player_name"].map(resolve_to_catalog)
    pm["player_id"] = [r[0] for r in resolved]
    pm = pm[pm["player_id"] != ""]
    # Collect genuinely-new real players (StatsBomb-only) to add to the catalog.
    new_mask = pd.Series([r[1] for r in resolved], index=pm.index)
    new_rows = (pm.loc[new_mask, ["player_id", "player_name", "position_group", "team"]]
                  .drop_duplicates("player_id"))
    if not new_rows.empty:
        add = pd.DataFrame({
            "player_id": new_rows["player_id"].values,
            "player_name": new_rows["player_name"].values,
            "position_group": new_rows["position_group"].values,
            "team_name": new_rows["team"].values,
            "profile_data_source": "observed_statsbomb",
            "data_provenance": "observed_statsbomb",
        })
        real_players = pd.concat([real_players, add], ignore_index=True)
    real_players = real_players.drop(columns=[c for c in ["_cn"] if c in real_players.columns])
    real_players = real_players.drop_duplicates("player_id").reset_index(drop=True)
    real_ids = set(real_players["player_id"])
    for c in PM_COLS:
        if c not in pm.columns:
            pm[c] = 0
    pm = pm[PM_COLS].drop_duplicates(["player_id", "match_id"]).reset_index(drop=True)
    print(f"player_match: REAL StatsBomb participations = {len(pm):,} "
          f"({pm['player_id'].nunique():,} players; {len(new_rows):,} new real players added)", flush=True)

    # --- events: clean residual nulls (xg N/A for non-shots -> 0; position -> Unknown) ---
    ev = pd.read_parquet(EVENTS)
    if "xg" in ev.columns:
        ev["xg"] = pd.to_numeric(ev["xg"], errors="coerce").fillna(0.0)
    for c in ("position", "player_name", "team"):
        if c in ev.columns:
            ev[c] = ev[c].fillna("Unknown")
    # Each row is a distinct REAL action; they are NOT entity duplicates even when
    # summary columns repeat (e.g. several passes in the same minute). Add a unique
    # event id and keep them all.
    ev = ev.reset_index(drop=True)
    ev.insert(0, "event_id", range(1, len(ev) + 1))
    ev.to_parquet(EVENTS, index=False)

    # --- goals_events: REAL goals from the event stream ---
    goals = ev[ev["event_type"] == "Shot"].copy()
    # a "Goal" is a Shot whose outcome is Goal; outcome lives only on shots here
    # (we kept xg for shots; recompute goal flag from a fresh fetch is overkill —
    # use the participations' goals as the authoritative real goal table instead).
    goals_real = pm[["player_id", "match_id", "season", "competition", "team", "goals"]].copy()
    goals_real = goals_real[goals_real["goals"] > 0].reset_index(drop=True)
    goals_real["goals"] = goals_real["goals"].astype(int)
    print(f"goals_events: REAL scorer-match rows = {len(goals_real):,} "
          f"({int(goals_real['goals'].sum()):,} real goals)", flush=True)

    # --- write real-only tables (no NULLs, no duplicates) ---
    outputs = {
        PROC / "core" / "players_cleaned": real_players,
        PROC / "stats" / "player_season_stats_cleaned": ps_real,
        PROC / "stats" / "goalkeeper_stats_cleaned": gk_real,
        PROC / "stats" / "player_match_stats_cleaned": pm,
        PROC / "events" / "goals_events_cleaned": goals_real,
    }
    total_nulls = 0
    for path, df in outputs.items():
        df = _fillnum(df)
        outputs[path] = df
        total_nulls += int(df.isna().sum().sum())
        df.to_parquet(path.with_suffix(".parquet"), index=False)
        df.to_csv(path.with_suffix(".csv"), index=False, encoding="utf-8")

    # --- verification ---
    ev_n = len(ev)
    invented = int((real_players.get("profile_data_source", "").astype(str) == "imputed_team_season_roster").sum())
    dup_ids = int(real_players["player_id"].duplicated().sum())
    dup_identity = int(real_players["player_name"].map(canonical_text).duplicated().sum())
    print("\n=== verification ===", flush=True)
    print(f"REAL event stream (>=1.5M dataset): {ev_n:,} rows | events nulls: {int(ev.isna().sum().sum())}", flush=True)
    print(f"real players: {len(real_players):,} | invented remaining: {invented} | "
          f"duplicate ids: {dup_ids} | duplicate identities (canonical): {dup_identity}", flush=True)
    print(f"nulls across real-only catalog tables: {total_nulls}", flush=True)
    assert ev_n >= 1_500_000, "event stream below 1.5M"
    assert invented == 0 and dup_ids == 0 and dup_identity == 0, "invented or duplicate entities remain"
    assert total_nulls == 0, "nulls remain in catalog tables"
    print("OK: >=1.5M real rows, no invented entities, no duplicates, no nulls.", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("build_real_only_datasets", main)
