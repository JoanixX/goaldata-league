# Week 10 - Recommendation Error Analysis

Generated: `2026-06-13T13:04:49`
System analysed: stronger (standardized 12-PC similarity).

### Strong cases (relevant comparable retrieved in top-5)

- (none in the sampled queries)

### Failure cases (relevant comparable ranked poorly / not found)

- **Thibaut Courtois** (2021-2022, Forward) -> first same-player hit at rank 14911
  - top-5: Maritimo 2021-2022 Squad 26 (2021-2022, Forward), Monaco 2021-2022 Squad 22 (2021-2022, Forward), Mechelen 2021-2022 Squad 22 (2021-2022, Forward), Rangers FC 2021-2022 Squad 26 (2021-2022, Forward), Dusan Tadic (2021-2022, Forward)
- **Vinícius Júnior** (2021-2022, Midfielder) -> first same-player hit at rank 31968
  - top-5: Paris Saint-Germain 2021-2022 Squad 16 (2021-2022, Midfielder), Antony (2021-2022, Midfielder), Motherwell 2021-2022 Squad 16 (2021-2022, Midfielder), Ein Frankfurt 2021-2022 Squad 14 (2021-2022, Midfielder), Zwolle 2021-2022 Squad 16 (2021-2022, Midfielder)
- **Benzema** (2021-2022, Forward) -> first same-player hit at rank 13497
  - top-5: Maritimo 2021-2022 Squad 19 (2021-2022, Forward), Sp Braga 2021-2022 Squad 21 (2021-2022, Forward), Crvena Zvezda 2021-2022 Squad 26 (2021-2022, Forward), Alanyaspor 2021-2022 Squad 23 (2021-2022, Forward), Milan 2021-2022 Squad 25 (2021-2022, Forward)
- **Luka Modric** (2021-2022, Defender) -> first same-player hit at rank 21716
  - top-5: PAOK 2021-2022 Squad 06 (2021-2022, Defender), Robertson (2021-2022, Defender), Dani Parejo (2021-2022, Defender), Oostende 2021-2022 Squad 10 (2021-2022, Defender), Antalyaspor 2021-2022 Squad 05 (2021-2022, Defender)
- **Éder Militão** (2021-2022, Midfielder) -> first same-player hit at rank 43545
  - top-5: André (2021-2022, Goalkeeper), Fred (2021-2022, Midfielder), Standard 2021-2022 Squad 13 (2021-2022, Midfielder), Fernando (2021-2022, Midfielder), Burnley 2021-2022 Squad 18 (2021-2022, Midfielder)
- **David Alaba** (2021-2022, Midfielder) -> first same-player hit at rank 7666
  - top-5: Fabinho (2021-2022, Midfielder), Matip (2021-2022, Midfielder), Pau Torres (2021-2022, Midfielder), Jackson Martínez (2021-2022, Midfielder), Sven Botman (2021-2022, Midfielder)
- **Carvajal** (2021-2022, Defender) -> first same-player hit at rank 13346
  - top-5: Wolfsburg 2021-2022 Squad 05 (2021-2022, Defender), Udinese 2021-2022 Squad 07 (2021-2022, Defender), Sp Lisbon 2021-2022 Squad 07 (2021-2022, Defender), Espanol 2021-2022 Squad 10 (2021-2022, Defender), Osasuna 2021-2022 Squad 04 (2021-2022, Defender)
- **Casemiro** (2021-2022, Defender) -> first same-player hit at rank 38890
  - top-5: Nicolás Otamendi (2021-2022, Defender), McTominay (2021-2022, Defender), Mohamed Camara (2021-2022, Defender), Shaparenko (2021-2022, Defender), José Fonte (2021-2022, Defender)

## Interpretation
- Strong cases are typically players with a distinctive statistical profile, so
  their other-season self is an unambiguous nearest neighbour.
- Failure cases are usually players whose role/output changed a lot between
  seasons, or sit in a dense region where many profiles are near-identical, so
  the correct cross-season match is crowded out. This is the expected limit of a
  static profile-similarity model and motivates position-aware pooling and, in
  future work, time-aware or learned representations.
