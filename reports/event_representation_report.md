# Event-based representation — evaluation (real, hybrid)

Generated: `2026-07-11T09:29:38` | features: **47** real event-style columns (event-type shares, pitch zones, pass direction/length, xG) | nodes: 1,577

| metric | value | note |
| --- | --- | --- |
| same-player recall@5 (proxy, low ceiling) | **0.2663** | vs 0.094 with aggregate stats — richer real features nearly 3x it |
| **same-position precision@5 (unfiltered)** | **0.8386** | higher-ceiling, scouting-relevant; NOT position-filtered (unlike PosPurity) |
| position classification macro-F1 | **0.9291** | supervised, real, in the 0.85-0.95 target |

The event representation is used for StatsBomb-covered players (hybrid); the aggregate PCA recommender covers the rest. All features are real; nothing is simulated.