# Fundamentacion de la arquitectura, feature engineering y PCA

## 1. Arquitectura de `data/processed`

La capa `data/processed` se organiza como una estructura relacional separada por granularidad. Esta decision responde a la naturaleza del dominio: en futbol no todos los registros representan el mismo tipo de unidad analitica. Un partido, un equipo, un jugador, un gol, una estadistica jugador-partido y una estadistica jugador-temporada no tienen la misma frecuencia ni el mismo significado.

La arquitectura actual separa los datos en:

- `core`: entidades principales y relativamente estables, como partidos, jugadores y equipos.
- `events`: eventos granulares que ocurren dentro de un partido, como goles.
- `stats`: mediciones de rendimiento por jugador-partido, jugador-temporada y arquero.
- `data/features`: matrices derivadas para analisis o modelado, como la matriz PCA de jugador-temporada.

Esta separacion evita mezclar niveles de detalle incompatibles. Por ejemplo, `matches_cleaned` tiene una fila por partido, mientras que `goals_events_cleaned` puede tener varias filas por partido y `player_match_stats_cleaned` puede tener varias filas por partido, una por jugador. Si todos estos datos se almacenaran en un unico dataset plano, la informacion del partido se repetiria por cada jugador o por cada evento. Esa repeticion no solo aumentaria el tamano del archivo, sino que tambien podria sesgar agregaciones, conteos y modelos posteriores.

La eleccion relacional permite conservar las llaves principales:

- `match_id`: conecta partidos con eventos y estadisticas.
- `player_id`: conecta jugadores con eventos, partidos y temporadas.
- `team_id`: conecta equipos con jugadores y partidos.

De esta forma, el proyecto preserva la trazabilidad del dato original y permite construir datasets derivados solo cuando el analisis lo requiere. La matriz de modelado no reemplaza a la capa procesada: se genera a partir de ella.

## 2. Comparacion frente a un dataset unico

Un dataset unico podria parecer mas simple, pero introduce problemas metodologicos. Al unir todas las tablas en una sola, se producirian duplicaciones inevitables. Un partido con 24 jugadores registrados y 3 goles podria terminar repitiendo el mismo estadio, pais, resultado y temporada muchas veces. Si luego se calcula el promedio de goles, la cantidad de partidos por temporada o la cobertura de campos, esos valores podrian quedar inflados por la cantidad de eventos o jugadores asociados.

La arquitectura actual ofrece ventajas concretas:

- Reduce redundancia porque cada entidad vive en su nivel natural.
- Mantiene integridad referencial mediante llaves.
- Permite controlar la granularidad antes de cada analisis.
- Facilita validaciones de calidad por tabla.
- Permite usar Parquet para consultas mas eficientes y CSV para compatibilidad.
- Evita que una matriz de ML arrastre columnas que son utiles para trazabilidad, pero no para modelado.

En este proyecto, la separacion es especialmente importante porque se combinan fuentes con distinta densidad: datos historicos de partidos, datos de jugadores, estadisticas agregadas y eventos. La arquitectura relacional permite integrar esas fuentes sin forzar artificialmente una sola unidad de analisis.

## 3. Granularidad y unidad de analisis

La granularidad define que representa una fila. Es una decision central porque determina que preguntas se pueden responder correctamente.

En la capa procesada se manejan estas granularidades:

- Partido: una fila por partido en `matches_cleaned`.
- Equipo: una fila por equipo en `teams_cleaned`.
- Jugador: una fila por jugador en `players_cleaned`.
- Evento: una fila por gol o evento registrado en `goals_events_cleaned`.
- Jugador-partido: una fila por jugador en un partido en `player_match_stats_cleaned`.
- Jugador-temporada: una fila por jugador en una temporada en `player_season_stats_cleaned`.
- Arquero-temporada: una fila por arquero en una temporada en `goalkeeper_stats_cleaned`.

La matriz PCA usa principalmente la granularidad jugador-temporada porque es mas estable que jugador-partido. Un partido individual puede estar afectado por lesiones, expulsiones, pocos minutos, rival, localia o decisiones tacticas especificas. La temporada resume mejor el perfil de rendimiento de un jugador y reduce ruido.

## 4. Feature engineering aplicado

El feature engineering transforma variables crudas en senales analiticas. En este proyecto no basta con contar goles, pases o minutos; se requiere construir variables comparables que respondan a preguntas de rendimiento, eficiencia, contexto y negocio.

Las features creadas se agrupan en varias familias.

### 4.1 Features temporales

Ejemplos:

- `season_start_year`
- `season_end_year`
- `match_year`
- `match_month`
- `match_dayofweek`
- `is_weekend`

Estas variables se generan a partir de `season` y `date`. Sirven para analizar evolucion historica, volumen de partidos por temporada, distribucion temporal y posibles efectos de calendario.

### 4.2 Features de resultado

Ejemplos:

- `total_goals = home_score + away_score`
- `goal_diff_home = home_score - away_score`
- `abs_goal_diff`
- `home_win`
- `away_win`
- `draw`
- `both_teams_scored`
- `over_1_5_goals`, `over_2_5_goals`, `over_3_5_goals`
- `home_clean_sheet`, `away_clean_sheet`

Estas variables combinan los goles de local y visitante para resumir el resultado del partido. Son utiles para analisis de competitividad, intensidad ofensiva, prediccion de resultados y segmentacion de partidos.

### 4.3 Features de contexto de equipos

Ejemplos:

- `same_country_match`
- `same_region_match`
- `country_derby_proxy`
- `home_team_win_rate`
- `away_team_win_rate`
- `home_team_points_per_match`
- `away_team_points_per_match`
- `head_to_head_match_count`

Estas variables combinan informacion de partidos y equipos. El objetivo es capturar contexto competitivo: si los equipos pertenecen al mismo pais o region, su rendimiento historico, puntos por partido y frecuencia de enfrentamiento.

En un contexto de negocio, estas features apoyan analisis de rivalidad, probabilidad de resultado, atractivo comercial de partidos y patrones de participacion recurrente.

### 4.4 Features por 90 minutos

Ejemplos:

- `goals_per90 = goals * 90 / minutes_played`
- `assists_per90 = assists * 90 / minutes_played`
- `shots_per90 = shots * 90 / minutes_played`
- `passes_attempted_per90 = passes_attempted * 90 / minutes_played`
- `tackles_per90 = tackles * 90 / minutes_played`
- `interceptions_per90 = interceptions * 90 / minutes_played`
- `fouls_committed_per90 = fouls_committed * 90 / minutes_played`

Estas features combinan produccion deportiva con minutos jugados. Son de las mas importantes porque permiten comparar jugadores con distinta participacion. Un suplente que juega 20 minutos no puede compararse directamente con un titular de 90 minutos usando solo totales.

El objetivo de negocio es identificar rendimiento ajustado por oportunidad. Esto es relevante para scouting, evaluacion de suplentes, comparacion de roles y deteccion de jugadores eficientes con pocos minutos.

### 4.5 Features de eficiencia

Ejemplos:

- `shot_accuracy = shots_on_target / shots`
- `goal_conversion_rate = goals / shots`
- `pass_completion_rate = passes_completed / passes_attempted`
- `cross_completion_rate = crosses_completed / crosses_attempted`
- `tackle_success_rate = tackles_won / tackles`

Estas variables combinan intentos y aciertos. Su valor esta en diferenciar volumen de eficiencia. Un jugador puede disparar mucho pero convertir poco; otro puede intentar menos pero ser mas efectivo.

En negocio, estas features sirven para identificar perfiles: finalizadores eficientes, pasadores seguros, laterales con precision en centros o defensores con alta efectividad en duelos.

### 4.6 Features compuestas de rol

Ejemplos:

- `defensive_actions = tackles + interceptions + clearances`
- `attacking_actions = shots + dribbles + crosses_attempted`
- `ball_actions = touches + attacking_actions + defensive_actions`
- `discipline_points = fouls_committed + yellow_cards * 3 + red_cards * 6`
- `goal_involvements = goals + assists`
- `two_way_index = attacking_actions + defensive_actions - discipline_points`
- `role_fit_score`

Estas features combinan varias acciones para representar dimensiones tacticas. Por ejemplo, `defensive_actions` resume actividad defensiva, mientras que `attacking_actions` aproxima volumen ofensivo. `discipline_points` penaliza comportamientos de riesgo y `two_way_index` aproxima aporte integral.

El proposito es construir senales mas interpretables para decisiones deportivas. Un club o analista no siempre necesita ver veinte columnas separadas; puede necesitar una medida sintetica de perfil defensivo, ofensivo o mixto.

### 4.7 Features fisicas y de perfil

Ejemplos:

- `age_group`
- `height_m`
- `body_mass_index`
- `height_bucket`
- `weight_bucket`
- `prime_age_flag`
- `u23_flag`
- `veteran_flag`
- `physical_profile_score`
- `position_group`

Estas variables combinan edad, altura, peso y posicion. No sustituyen el rendimiento deportivo, pero permiten contextualizarlo. Un arquero, un defensa central, un mediocampista y un delantero tienen exigencias fisicas y tacticas distintas.

En negocio, estas features ayudan a scouting, segmentacion por etapa de carrera y comparacion de jugadores dentro de roles similares.

## 5. Redundancias y control de calidad de features

Crear muchas features no implica que todas deban entrar a PCA o a un modelo. Algunas variables pueden ser utiles para interpretacion, pero redundantes para modelado.

En la revision de los datasets procesados se detectaron casos que deben controlarse:

- `pass_accuracy` y `pass_completion_rate` representan practicamente la misma formula. No deberian entrar juntas en una matriz PCA.
- `distance_covered_per90` y `distance_per_minute` son transformaciones de la misma relacion entre distancia y tiempo.
- `goal_involvements` esta altamente relacionada con `goals` y `assists`, porque se calcula como suma de ambas.
- Algunas banderas binarias pueden tener varianza muy baja o valores identicos en una muestra. Si una variable casi no cambia, no aporta separacion estadistica.

La politica recomendada es:

- Mantener features redundantes solo si tienen valor de lectura o reporting.
- Excluir duplicados o correlaciones casi perfectas en matrices de modelado.
- Revisar variables de varianza cero o casi cero.
- Documentar la formula y el objetivo de cada feature creada.

Esta depuracion es especialmente importante para PCA, porque las variables redundantes pueden duplicar peso dentro de los componentes principales.

## 6. PCA: aplicacion y justificacion

PCA es una tecnica de reduccion de dimensionalidad que transforma variables correlacionadas en componentes ortogonales. Su utilidad en este proyecto es resumir perfiles de jugadores a partir de muchas metricas de rendimiento.

La matriz actual se construye con una fila por `player_id` y `season`. Se usan variables numericas base y derivadas:

- Volumen: `matches_played`, `minutes_played`, `shots`, `passes_attempted`.
- Produccion ofensiva: `goals`, `assists`, `shots_on_target`.
- Participacion defensiva: `tackles`, `interceptions`, `fouls_committed`.
- Disciplina: `yellow_cards`, `red_cards`, `discipline_points_per90`.
- Eficiencia: `shot_accuracy`, `goal_conversion_rate`, `pass_completion_rate`.
- Normalizacion temporal: variables `per90` y `minutes_per_match`.

Antes de PCA se aplica escalamiento con `StandardScaler`. Esto es necesario porque PCA depende de la varianza. Sin escalamiento, variables como minutos o pases dominarian el analisis por tener magnitudes mas grandes, no necesariamente por ser mas importantes.

Tambien se pueden codificar variables categoricas con One-Hot Encoding cuando aportan contexto analitico. Por ejemplo, `position_group` permite que el espacio PCA considere diferencias tacticas entre arqueros, defensas, mediocampistas y delanteros.

## 7. Aplicabilidad de PCA por dataset

PCA no debe aplicarse automaticamente a todas las tablas. Debe existir una matriz numerica, con una unidad de analisis clara y variables comparables.

### Dataset elegido: `player_season_stats_cleaned`

Este es el dataset mas adecuado para PCA porque:

- Tiene una unidad clara: jugador-temporada.
- Resume rendimiento de forma mas estable que jugador-partido.
- Contiene variables numericas comparables.
- Incluye suficientes filas para observar patrones.
- Reduce ruido contextual de partidos individuales.

Por eso fue elegido como dataset principal para construir perfiles tacticos.

### Dataset posible con cautela: `player_match_stats_cleaned`

Tambien podria usarse PCA en jugador-partido porque tiene muchas filas. Sin embargo, su granularidad es mas ruidosa. Un partido aislado puede representar pocos minutos, una expulsion, un rival especifico o una decision tactica puntual. Ademas, al tener muchos registros generados o imputados, PCA podria capturar patrones de imputacion o contexto de partido en lugar de perfiles reales de jugador.

Seria util si el objetivo fuera analizar tipos de actuacion por partido, no perfiles estables de jugadores.

### Dataset posible con cautela: `goalkeeper_stats_cleaned`

Puede aplicarse PCA si se quiere analizar perfiles de arqueros. Tiene variables numericas coherentes como `saves`, `goals_conceded`, `clean_sheets`, `saves_per90` y `keeper_reliability_index`. La limitacion es que el universo es mas pequeno y especializado. No sirve para comparar todos los jugadores, pero si para un subanalisis de arqueros.

### Dataset no recomendado: `matches_cleaned`

Aunque tiene muchas filas, su unidad es partido, no jugador. PCA podria aplicarse para clasificar estilos de partidos o perfiles de encuentros, pero no para perfiles de jugadores. Ademas, muchas variables son de identificacion, contexto o resultado. Solo seria valido si se construye una matriz especifica de partido con variables como goles, posesion, localia, probabilidades proxy y diferencia de goles.

### Dataset no recomendado: `goals_events_cleaned`

No es ideal para PCA general porque cada fila representa un evento de gol. Tiene menos variables continuas y muchas columnas binarias o de contexto. Puede servir para analisis de eventos o clustering de tipos de gol, pero no para reduccion de perfiles de rendimiento amplios.

### Dataset no recomendado: `players_cleaned`

Tiene informacion descriptiva del jugador, como edad, altura, peso y posicion, pero no suficiente rendimiento deportivo por si solo. PCA sobre esta tabla produciria mas bien perfiles fisicos o demograficos, no perfiles tacticos.

La conclusion metodologica es que tener mas filas no basta. PCA requiere que las columnas representen dimensiones comparables de una misma unidad analitica. Por eso el dataset jugador-temporada es mas defendible que otros con mayor volumen.

## 8. Figuras del informe y reproducibilidad

El script para generar las figuras solicitadas se encuentra en:

```bash
src/generate_goaldata_figures.py
```

Para ejecutarlo desde la raiz del proyecto:

```bash
python src/generate_goaldata_figures.py
```

El script genera o actualiza las siguientes imagenes en `reports/figures`:

- `table_completeness.png`
- `possession_distribution.png`
- `matches_by_season.png`
- `match_field_coverage.png`
- `pca_player_season_generated.png`
- `pca_cumulative_variance_generated.png`
- `top_network_teams.png`

Las dos imagenes de PCA se copian desde `artifacts` si ya existen. Si no existen, el script ejecuta el generador de PCA definido en:

```bash
src/build_pca_feature_matrix.py
```

El PCA tambien puede regenerarse directamente con:

```bash
python src/build_pca_feature_matrix.py
```

Ese comando actualiza:

- `data/features/player_season_feature_matrix.csv`
- `artifacts/pca_player_season_2d.png`
- `artifacts/pca_cumulative_variance.png`
- `artifacts/pca_explained_variance.csv`
- `artifacts/pca_component_loadings.csv`
- `reports/pca_feature_matrix_report.md`

## 9. Sintesis metodologica

La arquitectura del proyecto se sostiene en tres decisiones principales. Primero, la capa `processed` conserva la granularidad natural de cada entidad para evitar duplicacion y sesgos. Segundo, el feature engineering convierte datos crudos en senales comparables de volumen, eficiencia, contexto, rol y riesgo disciplinario. Tercero, PCA se aplica sobre una matriz derivada de jugador-temporada porque esa unidad ofrece estabilidad, comparabilidad y sentido tactico.

La mejora pendiente no es crear mas columnas, sino depurar y justificar las existentes. Las features deben mantenerse cuando aportan interpretacion, separacion estadistica o valor de negocio. Las features redundantes deben excluirse de PCA y modelos para evitar sobreponderar una misma informacion.
