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

# 1. Estructura sugerida

La carpeta `data/raw/` conserva solo fuentes originales descargadas. No se debe
crear dentro de `raw/` una estructura `core/events/stats`; esa normalización
pertenece exclusivamente a `data/processed/`.

Los datasets finales deben existir en CSV y Parquet. CSV se mantiene para
compatibilidad y Parquet es obligatorio para rendimiento. El reporte
`logs/data_quality_report.json` decide si cada tabla está lista para EDA/ML.
No se deben duplicar filas ni inventar registros para cumplir el umbral de
1.5M; ese requisito se cumple solo con fuentes reales integradas.

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
```
    │   cl_2010_2025.csv
    │   cl_2010_2025_completed.csv
    │   2021-2022 Football Player Stats.csv
    │   UEFA Champions League 2016-2022 Data.xlsx
    ├───2021 - 2022 Data
    └───2025 Champions
```

Relaciones clave:

* players.player_id → stats / events
* teams.team_id → players / matches
* matches.match_id → stats / events

---

# 2. Qué tendrá cada archivo + conexión

## players.csv

Entidad central de jugadores

Campos:

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

Equipos

Campos:

team_id (PK)
team_name
country
logo

---

## matches.csv

Partidos

Campos:

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

Stats por jugador por partido

Campos:

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

Clave compuesta:

(player_id, match_id)

---

## player_season_stats.csv

Agregado por temporada

Campos:

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

Separado porque cambia el dominio

Campos:

player_id (FK → players)
season

saves
goals_conceded
clean_sheets
penalty_saves
punches

---

## goals_events.csv

Eventos de gol (granular)

Campos:

goal_id (PK)
match_id (FK → matches)
player_id (FK → players)

minute
assist_player_id (FK → players, nullable)
goal_type

---

# 3. Técnicas de normalización + glosario

## 3.1 Unificación de nombres (ejemplos reales)

| Original                  | Nuevo          | Nota               |
| ------------------------- | -------------- | ------------------ |
| assists / PasAss          | assists        | misma métrica      |
| goals / Goals             | goals          | case normalization |
| match_played / MP         | matches_played | consistente        |
| minutes_played / Min      | minutes_played | unificado          |
| conceded / goals_conceded | goals_conceded | semántica clara    |
| saved / saves             | saves          | verbo → sustantivo |
| yellow / CrdY             | yellow_cards   | legible            |
| red / CrdR                | red_cards      | legible            |

---

## 3.2 Unificación de métricas derivadas

pass_accuracy:

passes_completed / passes_attempted

shot_accuracy:

shots_on_target / shots

Si ya viene calculado:

* lo puedes recalcular o mantener uno solo (recomendado: recalcular)

---

## 3.3 Unidades

| Campo            | Regla                     |
| ---------------- | ------------------------- |
| distance_covered | siempre en km             |
| accuracy (%)     | convertir a decimal (0–1) |
| height           | cm                        |
| weight           | kg                        |

---

## 3.4 IDs

Problema:

* algunos datasets usan player_name
* otros id_player

Solución:

player_id = hash(player_name + team_id)

Opcional más robusto:

* agregar birth_year si existe

---

## 3.5 Redundancias eliminadas

Ejemplos:

* goals aparece en:

  * attacking
  * key_stats
    → se mantiene una sola fuente final

* assists igual

* matches_played repetido en varios archivos
  → solo uno final

---

# 4. Cosas a tomar en cuenta

* Diferentes niveles:

  * match-level vs season-level → nunca mezclar
* Algunos datasets están incompletos → tendrás NULLs
* Jugadores cambian de equipo → team_id puede variar por match (no fijarlo solo en players)
* No todos los jugadores tienen todas las stats (ej: defensas vs delanteros)
* Los nombres pueden variar (ej: “Cristiano Ronaldo” vs “C. Ronaldo”)

---

# 5. Cosas que no se deben hacer

* No usar player_name como clave
* No mezclar métricas agregadas con métricas por partido
* No duplicar columnas (ej: goals en 3 tablas sin control)
* No guardar porcentajes sin saber cómo se calcularon
* No dejar nombres inconsistentes entre datasets
* No perder granularidad (eventos → no agregarlos sin guardar original)
