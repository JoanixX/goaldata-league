"""
League / competition strength weights (season-specific, UEFA-anchored)
======================================================================

Raw output stats are not comparable across competitions: 50 goals in the
Primeira Liga are not worth 50 in the Premier League, and which league is
strongest changes by season (La Liga led the UEFA ranking through most of
2012-2017; the Premier League leads recent seasons). This module converts the
**official UEFA 5-year country coefficients** (ingested per season by
`src/ingest_uefa_coefficients.py`) into a multiplicative weight per
(competition, season):

* **Domestic league**: `coefficient(country, season) / max coefficient that
  season` — the strongest association that season gets weight 1.0.
* **UEFA club competitions**: anchored to UEFA's own bonus-point ratios
  (Champions League : Europa League : Conference League = 1.5 : 1.0 : 0.5,
  see https://www.uefa.com/nationalassociations/uefarankings/country/about/).
  The Champions League gets a 1.10 premium over the strongest domestic league
  (it aggregates the top clubs of every association); EL and Conference scale
  down by the official ratios.
* **National-team tournaments** (World Cup, Euro, Copa América): weight 1.0
  (top-level international football, no UEFA club coefficient applies).
* **Unknown competitions**: the median domestic weight of that season (never
  a hard-coded constant).

Consumed by `src/optimize_lineup.py` (decision layer). Representation models
(PCA / clustering / recommender) intentionally stay unweighted — they describe
playing style, not quality.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
COEFFICIENTS = BASE / "data" / "raw" / "uefa_country_coefficients.csv"

# Domestic competition labels (as they appear in processed tables) -> UEFA country.
COUNTRY_BY_COMPETITION = {
    "Premier League": "England",
    "La Liga": "Spain",
    "Serie A": "Italy",
    "Ligue 1": "France",
    "Bundesliga": "Germany",
    "1. Bundesliga": "Germany",
    "Primeira Liga": "Portugal",
    "Eredivisie": "Netherlands",
    "Super Lig": "Turkey",
    "Belgian Pro League": "Belgium",
    "Super League Greece": "Greece",
    "Scottish Premiership": "Scotland",
    "Copa del Rey": "Spain",
}

# Women's leagues have no UEFA men's coefficient and must not inherit their
# country's men's weight (a WSL season is not comparable to a Premier League
# season on the same scale). They take the seasonal default (median domestic)
# weight; documented as a limitation.

# UEFA bonus-point ratios (CL:EL:Conference = 1.5:1.0:0.5) and CL premium.
UCL_PREMIUM = 1.10
UEFA_CLUB_COMPETITIONS = {
    "UEFA Champions League": UCL_PREMIUM,
    "Champions League": UCL_PREMIUM,
    "UEFA Europa League": UCL_PREMIUM * (1.0 / 1.5),
    "UEFA Conference League": UCL_PREMIUM * (0.5 / 1.5),
}

INTERNATIONAL_TOURNAMENTS = {
    "FIFA World Cup": 1.0,
    "UEFA Euro": 1.0,
    "Copa America": 1.0,
    "African Cup of Nations": 1.0,
}


def load_coefficients(path: str | Path = COEFFICIENTS) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"{path} missing — run  python -m src.ingest_uefa_coefficients  first.")
    df = pd.read_csv(path)
    df["season"] = df["season"].astype(str)
    return df


def competition_weights(coefficients: pd.DataFrame | None = None) -> pd.DataFrame:
    """Tidy (competition, season, weight) table for every mapped competition."""
    coef = coefficients if coefficients is not None else load_coefficients()
    season_max = coef.groupby("season")["coefficient_5yr"].transform("max")
    coef = coef.assign(_norm=coef["coefficient_5yr"] / season_max)
    by_cs = coef.set_index(["country", "season"])["_norm"]

    rows = []
    seasons = sorted(coef["season"].unique())
    for season in seasons:
        domestic = []
        for comp, country in COUNTRY_BY_COMPETITION.items():
            w = by_cs.get((country, season))
            if w is not None and not pd.isna(w):
                rows.append((comp, season, round(float(w), 4)))
                domestic.append(float(w))
        for comp, w in UEFA_CLUB_COMPETITIONS.items():
            rows.append((comp, season, round(w, 4)))
        for comp, w in INTERNATIONAL_TOURNAMENTS.items():
            rows.append((comp, season, w))
        # fallback for competitions not mapped above
        med = pd.Series(domestic).median() if domestic else 0.6
        rows.append(("__default__", season, round(float(med), 4)))
    return pd.DataFrame(rows, columns=["competition", "season", "weight"])


def player_season_strength(player_match: pd.DataFrame,
                           weights: pd.DataFrame | None = None) -> pd.Series:
    """Minutes-weighted competition strength per (player_id, season).

    A player splitting minutes between a domestic league and the Champions
    League gets a blend; a Primeira Liga-only season is discounted relative to
    a Premier League season of that same year.
    """
    w = weights if weights is not None else competition_weights()
    widx = w.set_index(["competition", "season"])["weight"]
    default = w[w["competition"] == "__default__"].set_index("season")["weight"]

    pm = player_match[["player_id", "season", "competition", "minutes_played"]].copy()
    pm["season"] = pm["season"].astype(str)
    keys = list(zip(pm["competition"], pm["season"]))
    pm["_w"] = [widx.get(k) for k in keys]
    fallback = pm["season"].map(default).fillna(float(default.median()) if len(default) else 0.6)
    pm["_w"] = pm["_w"].astype(float).fillna(fallback)
    pm["_mins"] = pd.to_numeric(pm["minutes_played"], errors="coerce").clip(lower=1.0)

    grouped = pm.groupby(["player_id", "season"])
    return grouped.apply(lambda g: (g["_w"] * g["_mins"]).sum() / g["_mins"].sum(),
                         include_groups=False).rename("league_strength")


if __name__ == "__main__":
    w = competition_weights()
    latest = w[w["season"] == w["season"].max()].sort_values("weight", ascending=False)
    print(latest.to_string(index=False))
