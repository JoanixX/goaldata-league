# Supervised Evaluation — Position Classification

Generated: `2026-06-29T17:50:25`

Task: predict position group (GK/DEF/MID/FW) from the season representation. A strong score proves the representation carries real role signal.

## Full catalog (n=7257)

| model | accuracy | macro-F1 | weighted-F1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4033 | 0.1437 | 0.2318 |
| RandomForest | 0.6474 | 0.6759 | 0.6485 |
| GradientBoosting | 0.6364 | 0.6688 | 0.6377 |

## Real-feature subset — StatsBomb-covered (n=2327)

Where high scores are legitimate (real features, real labels):

| model | accuracy | macro-F1 | weighted-F1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.3952 | 0.1416 | 0.2239 |
| RandomForest | 0.7921 | 0.8154 | 0.7911 |
| GradientBoosting | 0.7887 | 0.8094 | 0.7879 |

**Best on real subset: RandomForest — macro-F1 0.8154.**