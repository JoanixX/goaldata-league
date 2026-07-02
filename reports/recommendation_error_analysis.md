# Week 10 - Recommendation Error Analysis

Generated: `2026-06-29T17:50:14`
System analysed: stronger (standardized 12-PC similarity).

### Strong cases (relevant comparable retrieved in top-5)

- **Thibaut Courtois** (2021-2022, Goalkeeper) -> first same-player hit at rank 2
  - top-5: Hugo Lloris (2017-2018, Goalkeeper), Thibaut Courtois (2024-2025, Goalkeeper), Hugo Lloris (2021-2022, Goalkeeper), Thibaut Courtois (2017-2018, Goalkeeper), Martin Dúbravka (2021-2022, Goalkeeper)
- **Vinícius Júnior** (2021-2022, Forward) -> first same-player hit at rank 3
  - top-5: Alassane Pléa (2021-2022, Forward), Robert Lewandowski (2023-2024, Forward), Vinícius Júnior (2023-2024, Forward), Harry Kane (2021-2022, Forward), Edin Dzeko (2021-2022, Forward)
- **Benzema** (2021-2022, Forward) -> first same-player hit at rank 2
  - top-5: Sébastien Haller (2021-2022, Forward), Benzema (2020-2021, Forward), Hugo Ekitike (2021-2022, Forward), Kaká (2011-2012, Forward), Álvaro Morata (2023-2024, Forward)
- **Luka Modric** (2021-2022, Midfielder) -> first same-player hit at rank 5
  - top-5: Toni Kroos (2020-2021, Midfielder), Joshua Kimmich (2020-2021, Midfielder), Toni Kroos (2023-2024, Midfielder), Joshua Kimmich (2018-2019, Midfielder), Luka Modric (2018-2019, Midfielder)
- **David Alaba** (2021-2022, Defender) -> first same-player hit at rank 2
  - top-5: Aymeric Laporte (2018-2019, Defender), David Alaba (2020-2021, Defender), Ghislain Konan (2021-2022, Defender), David Alaba (2017-2018, Defender), David Alaba (2022-2023, Defender)
- **Carvajal** (2021-2022, Defender) -> first same-player hit at rank 2
  - top-5: Larsson (2021-2022, Defender), Carvajal (2017-2018, Defender), Michael Lang (2021-2022, Defender), Nicolás Otamendi (2021-2022, Defender), Dejan Lovren (2021-2022, Defender)
- **Toni Kroos** (2021-2022, Midfielder) -> first same-player hit at rank 1
  - top-5: Toni Kroos (2018-2019, Midfielder), Toni Kroos (2019-2020, Midfielder), Nicolò Barella (2023-2024, Midfielder), Piotr Zielinski (2019-2020, Midfielder), Ivan Rakitić (2017-2018, Midfielder)
- **Rodrygo** (2021-2022, Forward) -> first same-player hit at rank 3
  - top-5: João Félix (2020-2021, Forward), Lamine Yamal (2023-2024, Forward), Rodrygo (2022-2023, Forward), Álvaro Morata (2021-2022, Forward), Karl Toko Ekambi (2019-2020, Forward)

### Failure cases (relevant comparable ranked poorly / not found)

- **Éder Militão** (2021-2022, Defender) -> first same-player hit at rank 160
  - top-5: Eric Dier (2017-2018, Defender), Thilo Kehrer (2018-2019, Defender), Mats Hummels (2017-2018, Defender), José Fonte (2021-2022, Defender), Josko Gvardiol (2021-2022, Defender)
- **Casemiro** (2021-2022, Midfielder) -> first same-player hit at rank 43
  - top-5: Fabinho (2018-2019, Midfielder), Scott McTominay (2021-2022, Midfielder), Josuha Guilavogui (2021-2022, Midfielder), John McGinn (2024-2025, Midfielder), Abdoulaye Doucouré (2021-2022, Midfielder)
- **Benjamin Mendy** (2021-2022, Defender) -> first same-player hit at rank 562
  - top-5: Lucas Digne (2013-2014, Defender), Michal Kadlec (2011-2012, Defender), Elderson Echiéjilé (2014-2015, Defender), Jordan Amavi (2025-2026, Defender), Adamo Nagalo (2024-2025, Defender)
- **Valverde** (2021-2022, Midfielder) -> first same-player hit at rank 154
  - top-5: Cuadrado (2021-2022, Midfielder), Rafa Silva (2021-2022, Midfielder), Gonçalo Inacio (2021-2022, Midfielder), Mohamed Camara (2021-2022, Midfielder), Lo Celso (2021-2022, Midfielder)
- **Nacho** (2021-2022, Defender) -> first same-player hit at rank 193
  - top-5: Ronald Araújo (2021-2022, Defender), Jan Bednarek (2021-2022, Defender), Matthijs de Ligt (2023-2024, Defender), Wesley Fofana (2022-2023, Defender), Marquinhos (2024-2025, Defender)
- **Lucas Vázquez** (2021-2022, Defender) -> first same-player hit at rank 553
  - top-5: Gabriel Magalhães (2023-2024, Defender), Gabriel Magalhães (2024-2025, Defender), Sergio Ramos (2020-2021, Defender), Mohamed Simakan (2021-2022, Defender), Davinson Sánchez (2021-2022, Defender)
- **Camavinga** (2021-2022, Midfielder) -> first same-player hit at rank 234
  - top-5: Pedraza (2021-2022, Midfielder), Morgan Schneiderlin (2021-2022, Midfielder), Sergio Busquets (2016-2017, Midfielder), Yannick Cahuzac (2021-2022, Midfielder), Cheikh Niasse (2025-2026, Midfielder)
- **Asensio** (2021-2022, Forward) -> first same-player hit at rank 120
  - top-5: Benzema (2014-2015, Forward), Tetê (2021-2022, Forward), Hélder Macedo Sousa (2011-2012, Forward), Antoine Griezmann (2025-2026, Forward), Gareth Bale (2015-2016, Forward)

## Interpretation
- Strong cases are typically players with a distinctive statistical profile, so
  their other-season self is an unambiguous nearest neighbour.
- Failure cases are usually players whose role/output changed a lot between
  seasons, or sit in a dense region where many profiles are near-identical, so
  the correct cross-season match is crowded out. This is the expected limit of a
  static profile-similarity model and motivates position-aware pooling and, in
  future work, time-aware or learned representations.
