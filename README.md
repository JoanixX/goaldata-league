# UEFA Champions League — GoalData League

> **Curso:** Big Data · Ciclo 2026-I  
> **Milestone:** Semana 3 — Dataset Charter & Processed Dataset V1  
> **Rol:** Ingeniería, Modelado y Análisis de Partidos de la UEFA Champions League (2011–2025)

---

## 1. Core del Proyecto

El proyecto **PC1** tiene como objetivo construir un pipeline de datos reproducible end-to-end sobre los partidos de la **UEFA Champions League** desde la temporada **2011-12 hasta la 2024-25**.

La pregunta central de investigación es:

> **¿Cómo han evolucionado los patrones de rendimiento y resultados en la UEFA Champions League a lo largo de 14 temporadas, y qué estructuras emergen al analizar los enfrentamientos como una red (grafo) de equipos?**

Este trabajo responde al primer hito del curso: definir el estatuto del dataset, realizar la ingesta desde archivos JSON por temporada, consolidarlos en un único CSV maestro, y sentar la base para análisis estadísticos y de grafos en hitos posteriores.

---

## 2. Resumen

| Campo | Detalle |
|---|---|
| **Dominio** | Fútbol — UEFA Champions League |
| **Período cubierto** | 2011-12 → 2024-25 (10 temporadas con datos) |
| **Total de temporadas** | 10 archivos `uefa.cl.json` |
| **Entidad principal** | Partido (`match`) |
| **Campos por partido** | Temporada, ronda, fecha, hora, equipo local, equipo visitante, marcador HT, marcador FT |
| **Formato de ingesta** | JSON por temporada → CSV consolidado |
| **Output final (V1)** | `champions_league_2011_2025.csv` |
| **Herramienta de conversión** | `notebooks/conversion_csv.py` |

---

## 3. Stack Tecnológico

```
Python 3.x
├── pandas         → manipulación y consolidación de DataFrames
├── json           → parseo de archivos JSON por temporada
├── glob           → descubrimiento dinámico de carpetas/temporadas
└── os             → construcción de rutas de archivos multiplataforma
```

| Herramienta | Uso |
|---|---|
| `Python 3.x` | Lenguaje principal del pipeline |
| `pandas` | Carga, transformación y exportación del dataset |
| `json` | Lectura de archivos fuente por temporada |
| `glob` | Iteración automática sobre directorios de temporadas |
| `os` | Construcción robusta y portable de rutas |
| `LaTeX` | Documentación científica del proyecto (`bigData_pc1_uxxxxxxxxx.tex`) |
| `Git` | Control de versiones; `.gitignore` configurado para excluir `.venv`, `.env` y el `.tex` |

> **Dependencias:** Ver `requirements.txt` (actualmente en construcción — se añadirán `pandas`, `networkx`, y otras librerías en hitos posteriores).

---

## 4. Arquitectura

### Flujo de Datos (Pipeline V1)

```
data/
├── 2011-12/uefa.cl.json   ──┐
├── 2012-13/uefa.cl.json   ──┤
├── 2013-14/uefa.cl.json   ──┤
├── 2014-15/uefa.cl.json   ──┼──► notebooks/conversion_csv.py ──► champions_league_2011_2025.csv
├── 2015-16/uefa.cl.json   ──┤
├── 2016-17/uefa.cl.json   ──┤
├── 2017-18/uefa.cl.json   ──┤
├── 2018-19/uefa.cl.json   ──┤
├── 2019-20/uefa.cl.json   ──┤
└── 2024-25/uefa.cl.json   ──┘

data/interim/   → datos en transformación intermedia (reservado para hitos futuros)
data/processed/ → datasets limpios y listos para modelado (reservado)
data/raw/       → espacio para fuentes adicionales (formato no-estructurado)
```

### Estructura del Repositorio

```
PC1/
│
├── data/                          # Datos organizados por temporada y etapa
│   ├── 2011-12/uefa.cl.json       # Temporada 2011-12
│   ├── 2012-13/uefa.cl.json       # Temporada 2012-13
│   ├── 2013-14/uefa.cl.json       # Temporada 2013-14
│   ├── 2014-15/uefa.cl.json       # Temporada 2014-15
│   ├── 2015-16/uefa.cl.json       # Temporada 2015-16
│   ├── 2016-17/uefa.cl.json       # Temporada 2016-17
│   ├── 2017-18/uefa.cl.json       # Temporada 2017-18
│   ├── 2018-19/uefa.cl.json       # Temporada 2018-19
│   ├── 2019-20/uefa.cl.json       # Temporada 2019-20
│   ├── 2024-25/uefa.cl.json       # Temporada 2024-25 (más reciente)
│   ├── raw/                       # Fuentes brutas adicionales (placeholder)
│   ├── interim/                   # Datos en proceso de transformación
│   └── processed/                 # Datos listos para análisis/modelado
│
├── notebooks/
│   └── conversion_csv.py          # Script principal de ingesta y consolidación
│
├── src/                           # Módulos de código reutilizable (futuro)
│
├── artifacts/                     # Salidas y artefactos del proyecto
│
├── reports/                       # Reportes generados (LaTeX, PDFs)
│
├── champions_league_2011_2025.csv # ★ CSV maestro consolidado (output V1)
├── bigData_pc1_uxxxxxxxxx.tex     # Informe LaTeX del Milestone 1
├── requirements.txt               # Dependencias Python del proyecto
├── .gitignore                     # Exclusiones de Git
├── LICENSE                        # Licencia del proyecto
└── README.md                      # Este documento
```

### Esquema del Dataset Consolidado (V1)

Cada fila del CSV maestro representa **un partido de la UEFA Champions League**:

| Columna | Tipo | Descripción |
|---|---|---|
| `season` | `str` | Temporada del partido (ej. `"2024-25"`) |
| `round` | `str` | Fase/ronda del torneo (ej. `"League, Matchday 1"`, `"Quarter-finals"`) |
| `date` | `str` | Fecha del partido en formato `YYYY-MM-DD` |
| `time` | `str` | Hora de inicio del partido (UTC) |
| `team1` | `str` | Equipo local con país entre paréntesis (ej. `"Real Madrid CF (ESP)"`) |
| `team2` | `str` | Equipo visitante con país entre paréntesis |
| `score_ht1` | `int / null` | Goles del equipo local al descanso (HT) |
| `score_ht2` | `int / null` | Goles del equipo visitante al descanso (HT) |
| `score_ft1` | `int` | Goles del equipo local al final del partido (FT) |
| `score_ft2` | `int` | Goles del equipo visitante al final del partido (FT) |

> **Nota:** `score_ht*` puede ser `null` para algunos partidos donde solo se registró el marcador final.

---

## 5. Seguridad y Fuentes de Datos

### Origen de los Datos

Los datos de partidos de la UEFA Champions League provienen del repositorio público **`openfootball/champions-league`** en GitHub, mantenido por la comunidad Open Football Data:

| Fuente | URL |
|---|---|
| **Repositorio principal** | [github.com/openfootball/champions-league](https://github.com/openfootball/champions-league) |
| **Organización Open Football** | [github.com/openfootball](https://github.com/openfootball) |
| **Dataset en formato estructurado (referencia)** | [github.com/jokecamp/FootballData](https://github.com/jokecamp/FootballData) |

### Licencia

Los archivos JSON fuente están bajo licencia de **dominio público / uso libre** conforme a las políticas de Open Football Data. No existe restricción para uso académico, investigativo o educativo.

### Consideraciones de Seguridad y Privacidad

- **Sin datos personales (PII):** Los datos contienen únicamente información de partidos deportivos (equipos, fechas, marcadores). No se almacena ningún dato de personas físicas.
- **Sin datos sensibles:** No se manejan credenciales, tokens de API ni datos financieros.
- **`.gitignore` configurado:** Se excluyen archivos de entorno (`.venv`, `.env`) para proteger variables de entorno locales.
- **`.tex` excluido de Git:** El archivo de informe LaTeX (`bigData_pc1_uxxxxxxxxx.tex`) está en `.gitignore` para evitar comprometer borradores con datos del equipo académico.
- **Gap de datos identificado:** Las temporadas 2020-21, 2021-22, 2022-23 y 2023-24 **no están presentes** en el directorio de datos. Esto puede deberse a disponibilidad en la fuente o a un requerimiento de alcance del proyecto. Se debe documentar y rellenar en hitos posteriores si es requerido.

---

## 6. Accesos y Cómo se Conecta Todo

### Reproducción del Pipeline Completo

```bash
# 1. Clonar el repositorio
git clone <url-del-repo>
cd PC1

# 2. Crear entorno virtual e instalar dependencias
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS/Linux

pip install -r requirements.txt

# 3. Ejecutar el script de ingesta y consolidación
cd notebooks
python conversion_csv.py

# 4. Output generado:
#    champions_league_2011_2025.csv (en la raíz del proyecto)
```

### Cómo se Conectan los Componentes

```
[Fuente]                [Ingesta]                   [Output]
─────────────────────────────────────────────────────────────
data/XXXX-XX/          conversion_csv.py            champions_league_
  uefa.cl.json   ──►   (glob + json + pandas) ──►   2011_2025.csv
  (x10 temporadas)       
                            │
                            ▼
                    Campos extraídos:
                    season, round, date, time,
                    team1, team2,
                    score_ht1, score_ht2,
                    score_ft1, score_ft2
```

### Relación entre Archivos

| Archivo | Depende de | Produce |
|---|---|---|
| `notebooks/conversion_csv.py` | `data/*/uefa.cl.json` (x10) | `champions_league_2011_2025.csv` |
| `bigData_pc1_uxxxxxxxxx.tex` | Resultados del pipeline | Informe PDF (compilar con LaTeX) |
| `data/*/uefa.cl.json` | Fuente externa (openfootball) | Entrada al pipeline |

### Navegación del Repositorio

- **Para explorar los datos crudos:** `data/<temporada>/uefa.cl.json`
- **Para reejecutar la consolidación:** `notebooks/conversion_csv.py`
- **Para leer el informe científico:** `bigData_pc1_uxxxxxxxxx.tex` (compilar con `pdflatex`)
- **Para ver el dataset consolidado:** `champions_league_2011_2025.csv`

---

## 7. Explicación Detallada de Funcionalidades

### 7.1 Ingesta Multi-Temporada (`conversion_csv.py`)

El script principal implementa un **pipeline de ingesta genérico y extensible** que recorre automáticamente todas las subcarpetas de `data/` buscando archivos `uefa.cl.json`.

```python
import pandas as pd
import json
import glob
import os

all_matches = []

# Descubrimiento automático de carpetas de temporada
for folder in glob.glob("../data/*/"):
    file = os.path.join(folder, "uefa.cl.json")
    if os.path.exists(file):
        season = os.path.basename(os.path.normpath(folder))  # ej. "2024-25"
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "matches" in data:
                for m in data["matches"]:
                    match = {
                        "season":     season,
                        "round":      m.get("round"),
                        "date":       m.get("date"),
                        "time":       m.get("time"),
                        "team1":      m.get("team1"),
                        "team2":      m.get("team2"),
                        "score_ht1":  m.get("score", {}).get("ht", [None, None])[0],
                        "score_ht2":  m.get("score", {}).get("ht", [None, None])[1],
                        "score_ft1":  m.get("score", {}).get("ft", [None, None])[0],
                        "score_ft2":  m.get("score", {}).get("ft", [None, None])[1],
                    }
                    all_matches.append(match)

df = pd.DataFrame(all_matches)
df.to_csv("champions_league_2011_2025.csv", index=False)
```

**Decisiones de diseño clave:**

- **`glob.glob("../data/*/"):`** Permite agregar nuevas temporadas simplemente depositando la carpeta correspondiente — sin modificar el script.
- **`m.get("score", {}).get("ht", [None, None])[0]`:** Manejo robusto de valores nulos. El campo `ht` (half-time) no siempre está disponible en todos los partidos; el fallback `[None, None]` garantiza que el DataFrame no lance excepciones durante la ingesta.
- **`season = os.path.basename(...)`:** Extrae el nombre de la carpeta (ej. `"2024-25"`) como identificador de temporada, creando una columna adicional que no existe en los JSON originales.
- **`encoding="utf-8"`:** Asegura la correcta lectura de caracteres especiales presentes en nombres de equipos (ej. `"FC Bayern München"`, `"ŠK Slovan Bratislava"`).

### 7.2 Estructura de los Archivos Fuente (`uefa.cl.json`)

Cada temporada tiene su propio archivo JSON con la siguiente estructura:

```json
{
  "name": "UEFA Champions League 2024/25",
  "matches": [
    {
      "round": "League, Matchday 1",
      "date": "2024-09-17",
      "time": "21:00",
      "team1": "Real Madrid CF (ESP)",
      "team2": "VfB Stuttgart (GER)",
      "score": {
        "ht": [0, 0],
        "ft": [3, 1]
      }
    },
    ...
  ]
}
```

**Campos del JSON:**

| Campo JSON | Descripción |
|---|---|
| `name` | Nombre oficial de la competición y temporada |
| `matches` | Array de partidos de la temporada |
| `round` | Fase del torneo (ej. `"League, Matchday 1"`, `"Round of 16"`, `"Final"`) |
| `date` | Fecha en formato ISO 8601 (`YYYY-MM-DD`) |
| `time` | Hora de inicio (hora local del venue o UTC, según la fuente) |
| `team1` | Equipo local + código de país entre paréntesis |
| `team2` | Equipo visitante + código de país entre paréntesis |
| `score.ht` | Array de 2 elementos: `[goles_team1_HT, goles_team2_HT]` |
| `score.ft` | Array de 2 elementos: `[goles_team1_FT, goles_team2_FT]` |

### 7.3 Temporadas Disponibles

| Temporada | Archivo | Tamaño aprox. |
|---|---|---|
| 2011-12 | `data/2011-12/uefa.cl.json` | ~36 KB |
| 2012-13 | `data/2012-13/uefa.cl.json` | ~36 KB |
| 2013-14 | `data/2013-14/uefa.cl.json` | ~36 KB |
| 2014-15 | `data/2014-15/uefa.cl.json` | ~36 KB |
| 2015-16 | `data/2015-16/uefa.cl.json` | ~37 KB |
| 2016-17 | `data/2016-17/uefa.cl.json` | ~36 KB |
| 2017-18 | `data/2017-18/uefa.cl.json` | ~36 KB |
| 2018-19 | `data/2018-19/uefa.cl.json` | ~37 KB |
| 2019-20 | `data/2019-20/uefa.cl.json` | ~35 KB |
| 2024-25 | `data/2024-25/uefa.cl.json` | ~58 KB |

> **Temporadas faltantes:** Las temporadas 2020-21, 2021-22, 2022-23 y 2023-24 no están presentes en el repositorio. La temporada 2024-25 es notablemente más grande (~58 KB) porque la UCL 2024-25 adoptó el nuevo formato de **fase de liga** (League Phase) con 36 equipos y 8 partidos cada uno, reemplazando la antigua fase de grupos.

### 7.4 Formato del Output: CSV Maestro

El archivo `champions_league_2011_2025.csv` en la raíz del proyecto es el output consolidado de todas las temporadas disponibles. Estructura de ejemplo:

```
season,round,date,time,team1,team2,score_ht1,score_ht2,score_ft1,score_ft2
2024-25,League Matchday 1,2024-09-17,18:45,BSC Young Boys (SUI),Aston Villa FC (ENG),0,2,0,3
2024-25,League Matchday 1,2024-09-17,21:00,Real Madrid CF (ESP),VfB Stuttgart (GER),0,0,3,1
2011-12,Group Stage,2011-09-13,20:45,FC Barcelona (ESP),AFC Ajax (NED),5,0,5,0
...
```

### 7.5 Informe Científico (LaTeX)

El archivo `bigData_pc1_uxxxxxxxxx.tex` es el informe académico del Milestone 1. Contiene:

- **Project Proposal:** Dominio del problema, pregunta de investigación y justificación para el curso.
- **Source Inventory:** Tabla de metadatos de la fuente (formato, licencia, tamaño estimado).
- **Schema Design & Architecture:** Esquema estrella del dataset y relaciones entre tablas.
- **Scale Analysis:** Métricas de volumen de datos V1.
- **Ethics and Access Note:** Consideraciones de privacidad y anonimización.
- **Technical Execution:** Comandos de reproducibilidad del pipeline.
- **Data Dictionary:** Glosario de campos del dataset.

> El archivo `.tex` está en `.gitignore` y no es parte del historial de Git — debe compilarse localmente con `pdflatex` o `xelatex`.

### 7.6 Directorios de Datos (convención)

| Directorio | Propósito | Estado actual |
|---|---|---|
| `data/<temporada>/` | Datos crudos por temporada (JSON) | Poblado (10 temporadas) |
| `data/raw/` | Fuentes brutas adicionales no estructuradas | Placeholder (`.gitkeep`) |
| `data/interim/` | Datos en proceso de transformación intermedia | Placeholder (`.gitkeep`) |
| `data/processed/` | Datasets limpios listos para modelado | Placeholder (`.gitkeep`) |

### 7.7 Hitos Futuros (Roadmap)

Este pipeline es la base para los siguientes hitos del curso:

1. **Milestone 2 — Análisis Exploratorio (EDA):**  
   Estadísticas descriptivas sobre goles, equipos con más participaciones, distribución de resultados por ronda.

2. **Milestone 3 — Análisis de Grafos:**  
   Construir una red donde los nodos son equipos y las aristas representan enfrentamientos (ponderadas por diferencia de goles). Analizar centralidad y comunidades.

3. **Milestone 4 — Clustering y Modelado:**  
   Identificar "perfiles de equipo" mediante clustering no supervisado (K-Means, DBSCAN) sobre estadísticas históricas.

4. **Milestone 5 — Reducción Dimensional:**  
   Aplicar PCA o UMAP sobre embeddings de rendimiento por temporada.

---

## Archivos Importantes

| Archivo | Descripción |
|---|---|
| [`notebooks/conversion_csv.py`](notebooks/conversion_csv.py) | Script de ingesta y consolidación del CSV maestro |
| [`champions_league_2011_2025.csv`](champions_league_2011_2025.csv) | Dataset consolidado (output V1) |
| `bigData_pc1_uxxxxxxxxx.tex` | Informe LaTeX del Milestone 1 (excluido de Git) |
| [`requirements.txt`](requirements.txt) | Dependencias del proyecto |
| [`.gitignore`](.gitignore) | Archivos y carpetas excluidas del control de versiones |
| [`LICENSE`](LICENSE) | Licencia del repositorio |

---