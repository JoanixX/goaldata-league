# STANDARD FIELD FORMAT (MATCH DATASET)

## GENERAL FIELDS
- season:
  - YYYY-YYYY
  - Example: 2010-2011

- phase:
  - Knockout Play-offs
  - Round of 16
  - Quarter-finals
  - Semi-finals
  - Final

- leg:
  - First Leg
  - Second Leg
  - Final

- date:
  - DD-MM-YYYY
  - Example: 10-04-2024

- kickoff_time / fulltime_time:
  - HH:MM (24h format)
  - Example: 21:00, 22:50

## TEAMS
- home_team / away_team:
  - Official club name (no abbreviations)
  - Example: Borussia Dortmund

- score:
  - INT-INT
  - Example: 2-1

- aggregate:
  - INT-INT
  - NULL if Final

## LOCATION
- stadium:
  - Official name
  - Example: Parc des Princes

- city:
  - Standardized name (consistent language)
  - Example: Munich

- country:
  - Standardized name (consistent language)
  - Example: Germany

## REFEREES
- referee:
  - FirstName LastName

- assistant_referees:
  - FirstName LastName; FirstName LastName
  - Separator: "; "

## MANAGERS
- home_manager / away_manager:
  - FirstName LastName

## LINEUPS
- lineups:
  - Format:
    Team: Player; Player; Player | Team: Player; Player; Player

  - Rules:
    - Team separator: " | "
    - Player separator: "; "
    - Consistent order (ideally defense → midfield → attack)

  - Example:
    Real Madrid: Keylor Navas; Raphaël Varane | Juventus: Gianluigi Buffon; Giorgio Chiellini

## PLAYER RATINGS
- player_ratings:
  - Format:
    Team: Player X.X; Player X.X | Team: Player X.X

  - Rules:
    - Ratings with 1 decimal
    - Player separator: "; "
    - Team separator: "|"

## SHOT STATS
- total_shots:
  - INT

- total_shots_home / away:
  - INT

- shots_on_target:
  - INT

- shots_on_target_home / away:
  - INT

## EVENTS

### GOALS
- goals:
  - Format:
    Player MIN'; MIN' (P); MIN' (OG)

  - Valid cases:
    - Normal goal: Messi 45'
    - Stoppage time: Messi 45+3'
    - Penalty: Brahim 75' (P)
    - Own goal: Brown 82' (OG)
    - Multiple goals same player:
      Robinho 38'; 49'; 60'

  - Separator between players: "; "

### ASSISTS
- assists:
  - Format:
    Player MIN'; MIN'

  - Example:
    Dembélé 20'; 73'

### SUBSTITUTIONS
- substitutions:
  - Format:
    MIN' Player_IN x Player_OUT

  - Examples:
    - 66' Barcola x Doué
    - 90+4' Sørloth x Griezmann

  - Separator: "; "

### CARDS

- yellow_cards:
  - Player MIN'
  - Example: Hakimi 90+5'

- red_cards:
  - Player MIN'

## POSSESSION
- possession_home / away:
  - INT%
  - Example: 45%

## FOULS
- fouls_total:
  - INT

- fouls_home / away:
  - INT

## CORNERS
- corners_total:
  - INT

- corners_home / away:
  - INT

## IMPORTANT GLOBAL RULES

- Separators:
  - ";" → within lists
  - "|" → between teams
- ALWAYS include a space after ";" and "|"
- Use NULL in uppercase when applicable
- UTF-8 encoding (preserve accents: Munich, Clément, etc.)
- Do NOT mix commas with semicolons in list fields

---

# 1. Suggested structure

`data/raw/` keeps only the original downloaded sources. Do not create a
`core/events/stats` structure inside `raw/`; that normalisation belongs
exclusively to `data/processed/`.

The final datasets must exist in both CSV and Parquet. CSV is kept for
compatibility and Parquet is mandatory for performance. The report
`logs/data_quality_report.json` decides whether each table is ready for EDA/ML.
Rows must not be duplicated, and records must not be fabricated arbitrarily.

### Provenance and simulated data (IMPORTANT)

Because real granular data does not exist for the full historical scope, part of
the per-match/event tables is **simulated from real anchors** (the real match
scoreline, real per-position rates from FBref) with documented statistical
models — never arbitrary random fill. Every cell carries a label in the
`data_provenance` column (`observed` / `derived` / `simulated`). Rules:

- Per-match `goals` are allocated from the **real scoreline** (the sum of player
  goals equals the real score, verified at 100%).
- `goals_events_cleaned` is the goal table at its **real size (~254.6k goals,
  ≈2.69/match)**; it is NOT inflated to 1.5M (just as the players table is not
  forced to be huge). Only `player_match_stats` (~1.95M) exceeds 1.5M by logic.
- Player identities are unified (NLP): `CristianoRonaldo` / `cristiano_ronaldo`
  / `CR7` → a single `player_id` (see `src/entity_resolution.py`).

Full detail and citations: `../reports/methodology_and_citations.md` and
`dictionary.txt`. Regeneration: `python -m src.rebuild_realistic_datasets`.

```
data/
│   data_dictionary.csv
│   README.md
├───processed
│   ├───core
│   │       matches_cleaned.csv
│   │       matches_cleaned.parquet
│   │       players_cleaned.csv
│   │       players_cleaned.parquet
│   │       teams_cleaned.csv
│   │       teams_cleaned.parquet
│   │
│   ├───events
│   │       goals_events_cleaned.csv
│   │       goals_events_cleaned.parquet
│   │
│   └───stats
│           goalkeeper_stats_cleaned.csv
│           goalkeeper_stats_cleaned.parquet
│           player_match_stats_cleaned.csv
│           player_match_stats_cleaned.parquet
│           player_season_stats_cleaned.csv
│           player_season_stats_cleaned.parquet
└───raw
        cl_2010_2025.csv
        cl_2010_2025_completed.csv
        2021-2022 Football Player Stats.csv
        UEFA Champions League 2016-2022 Data.xlsx
        2021 - 2022 Data/
        2025 Champions/
```

Key relationships:

* players.player_id → stats / events
* teams.team_id → players / matches
* matches.match_id → stats / events

---

# 2. What each file contains + connections

## players.csv

Central player entity.

Fields:

player_id (PK)
player_name
nationality
age
height_cm
weight_kg
position
team_id (FK → teams)

---

## teams.csv

Teams.

Fields:

team_id (PK)
team_name
country
logo

---

## matches.csv

Matches.

Fields:

match_id (PK)
season
date
home_team_id (FK → teams)
away_team_id (FK → teams)
stadium
city
country
referee
home_score
away_score
possession_home
possession_away

---

## player_match_stats.csv

Per-player, per-match stats.

Fields:

player_id (FK → players)
match_id (FK → matches)

minutes_played
goals
assists

shots
shots_on_target
shots_off_target
shots_blocked

passes_completed
passes_attempted
pass_accuracy

crosses_completed
crosses_attempted

dribbles
offsides

tackles
tackles_won
tackles_lost
interceptions
clearances

fouls_committed
fouls_suffered
yellow_cards
red_cards

distance_covered
top_speed

Composite key:

(player_id, match_id)

---

## player_season_stats.csv

Per-season aggregate.

Fields:

player_id (FK → players)
season

matches_played
minutes_played

goals
assists

shots
shots_on_target

passes_completed
passes_attempted

tackles
interceptions

fouls_committed
yellow_cards
red_cards

---

## goalkeeper_stats.csv

Kept separate because the domain changes.

Fields:

player_id (FK → players)
season

saves
goals_conceded
clean_sheets
penalty_saves
punches

---

## goals_events.csv

Goal events (granular).

Fields:

goal_id (PK)
match_id (FK → matches)
player_id (FK → players)

minute
assist_player_id (FK → players, nullable)
goal_type

---

# 3. Normalisation techniques + glossary

## 3.1 Name unification (real examples)

| Original                  | New            | Note               |
| ------------------------- | -------------- | ------------------ |
| assists / PasAss          | assists        | same metric        |
| goals / Goals             | goals          | case normalization |
| match_played / MP         | matches_played | consistent         |
| minutes_played / Min      | minutes_played | unified            |
| conceded / goals_conceded | goals_conceded | clearer semantics  |
| saved / saves             | saves          | verb → noun        |
| yellow / CrdY             | yellow_cards   | readable           |
| red / CrdR                | red_cards      | readable           |

---

## 3.2 Derived-metric unification

pass_accuracy:

passes_completed / passes_attempted

shot_accuracy:

shots_on_target / shots

If already pre-computed:

* you may recompute it or keep a single source (recommended: recompute)

---

## 3.3 Units

| Field            | Rule                       |
| ---------------- | -------------------------- |
| distance_covered | always in km               |
| accuracy (%)     | convert to decimal (0–1)   |
| height           | cm                         |
| weight           | kg                         |

---

## 3.4 IDs

Problem:

* some datasets use player_name
* others use id_player

Solution:

player_id = hash(player_name + team_id)

More robust optional:

* add birth_year if available

---

## 3.5 Removed redundancies

Examples:

* goals appears in:

  * attacking
  * key_stats
    → a single final source is kept

* assists likewise

* matches_played repeated across several files
  → only one final source

---

# 4. Things to keep in mind

* Different levels:

  * match-level vs season-level → never mix
* Some datasets are incomplete → you will have NULLs
* Players change teams → team_id can vary per match (do not fix it only on players)
* Not every player has every stat (e.g. defenders vs forwards)
* Names can vary (e.g. "Cristiano Ronaldo" vs "C. Ronaldo")

---

# 5. Things you must not do

* Do not use player_name as a key
* Do not mix aggregated metrics with per-match metrics
* Do not duplicate columns (e.g. goals in 3 tables without control)
* Do not store percentages without knowing how they were computed
* Do not leave inconsistent names across datasets
* Do not lose granularity (events → do not aggregate them without keeping the original)
