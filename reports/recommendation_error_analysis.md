# Recommendation Error Analysis

Generated: `2026-07-09T02:02:19`
System analysed: stronger (standardized 11-PC similarity).

### Strong cases (relevant comparable retrieved in top-5)

- **Thibaut Courtois** (2021-2022, Goalkeeper) -> first same-player hit at rank 2
  - top-5: Hugo Lloris (2017-2018, Goalkeeper), Thibaut Courtois (2024-2025, Goalkeeper), Hugo Lloris (2021-2022, Goalkeeper), Thibaut Courtois (2017-2018, Goalkeeper), Martin Dúbravka (2021-2022, Goalkeeper)
- **Vinícius Júnior** (2021-2022, Forward) -> first same-player hit at rank 3
  - top-5: Alassane Pléa (2021-2022, Forward), Robert Lewandowski (2023-2024, Forward), Vinícius Júnior (2023-2024, Forward), Raheem Sterling (2018-2019, Forward), Mario Mandzukic (2018-2019, Forward)
- **David Alaba** (2021-2022, Defender) -> first same-player hit at rank 1
  - top-5: David Alaba (2020-2021, Defender), David Alaba (2017-2018, Defender), Aymeric Laporte (2018-2019, Defender), David Alaba (2018-2019, Defender), Ghislain Konan (2021-2022, Defender)
- **Toni Kroos** (2021-2022, Midfielder) -> first same-player hit at rank 1
  - top-5: Toni Kroos (2018-2019, Midfielder), Nicolò Barella (2023-2024, Midfielder), Toni Kroos (2019-2020, Midfielder), Piotr Zielinski (2019-2020, Midfielder), Enzo Fernández (2022-2023, Midfielder)
- **Mohamed Salah** (2021-2022, Forward) -> first same-player hit at rank 1
  - top-5: Mohamed Salah (2022-2023, Forward), Mohamed Salah (2024-2025, Forward), Mohamed Salah (2019-2020, Forward), Mohamed Salah (2017-2018, Forward), Phil Foden (2023-2024, Forward)
- **Diogo Jota** (2021-2022, Forward) -> first same-player hit at rank 5
  - top-5: Patrik Schick (2019-2020, Forward), Álvaro Morata (2021-2022, Forward), Gonzalo Higuaín (2017-2018, Forward), Cristiano Ronaldo (2021-2022, Forward), Diogo Jota (2024-2025, Forward)
- **James Milner** (2021-2022, Midfielder) -> first same-player hit at rank 1
  - top-5: James Milner (2020-2021, Midfielder), James Milner (2022-2023, Midfielder), Joel Obi (2021-2022, Midfielder), Rade Krunic (2021-2022, Midfielder), Rade Krunic (2022-2023, Midfielder)
- **Riyad Mahrez** (2021-2022, Forward) -> first same-player hit at rank 1
  - top-5: Riyad Mahrez (2020-2021, Forward), Riyad Mahrez (2019-2020, Forward), Riyad Mahrez (2018-2019, Forward), Gabriel Jesus (2021-2022, Forward), Raheem Sterling (2021-2022, Forward)

### Failure cases (relevant comparable ranked poorly / not found)

- **Éder Militão** (2021-2022, Defender) -> first same-player hit at rank 191
  - top-5: Raphaël Varane (2017-2018, Defender), Eric Dier (2017-2018, Defender), Manuel Akanji (2022-2023, Defender), Aymeric Laporte (2017-2018, Defender), José Fonte (2021-2022, Defender)
- **Casemiro** (2021-2022, Midfielder) -> first same-player hit at rank 131
  - top-5: Scott McTominay (2021-2022, Midfielder), Fabinho (2018-2019, Midfielder), Geoffrey Kondogbia (2019-2020, Midfielder), Maxime Gonalons (2021-2022, Midfielder), Andre-Frank Zambo Anguissa (2021-2022, Midfielder)
- **Benjamin Mendy** (2021-2022, Defender) -> first same-player hit at rank 46
  - top-5: Ibrahima Mbaye (2021-2022, Defender), Sebastien De Maio (2021-2022, Defender), Calderón (2021-2022, Defender), Steven Fortes (2021-2022, Defender), Jamal Lewis (2021-2022, Defender)
- **Nacho** (2021-2022, Defender) -> first same-player hit at rank 29
  - top-5: Jakub Kiwior (2024-2025, Defender), Jan Bednarek (2021-2022, Defender), Chris Smalling (2017-2018, Defender), Marquinhos (2024-2025, Defender), Adam Webster (2021-2022, Defender)
- **Lucas Vázquez** (2021-2022, Defender) -> first same-player hit at rank 1266
  - top-5: Davinson Sánchez (2021-2022, Defender), Mohamed Simakan (2021-2022, Defender), Gabriel Magalhães (2023-2024, Defender), Gabriel Magalhães (2024-2025, Defender), Sergio Ramos (2020-2021, Defender)
- **Gareth Bale** (2021-2022, Forward) -> first same-player hit at rank 41
  - top-5: Shane Long (2021-2022, Forward), Ferran Jutglà (2021-2022, Forward), Olivier Giroud (2020-2021, Forward), Vinícius Júnior (2019-2020, Forward), Ádám Szalai (2021-2022, Forward)
- **Fabinho** (2021-2022, Midfielder) -> first same-player hit at rank 149
  - top-5: Julian Ryerson (2023-2024, Midfielder), Ellyes Skhiri (2021-2022, Midfielder), Ismaël Bennacer (2021-2022, Midfielder), Jorginho (2021-2022, Midfielder), Matteo Darmian (2021-2022, Midfielder)
- **Maximilian Arnold** (2021-2022, Midfielder) -> first same-player hit at rank 97
  - top-5: Jesse Lingard (2018-2019, Midfielder), Bertrand Traoré (2018-2019, Midfielder), Teun Koopmeiners (2021-2022, Midfielder), Vitinha (2024-2025, Midfielder), Leon Goretzka (2022-2023, Midfielder)

## Interpretation
- Strong cases are typically players with a distinctive statistical profile, so
  their other-season self is an unambiguous nearest neighbour.
- Failure cases are usually players whose role/output changed a lot between
  seasons, or sit in a dense region where many profiles are near-identical, so
  the correct cross-season match is crowded out. This is the expected limit of a
  static profile-similarity model and motivates position-aware pooling and, in
  future work, time-aware or learned representations.
