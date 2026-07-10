# Supervised Evaluation — Position Classification

Generated: `2026-07-09T02:02:27`

Task: predict position group (GK/DEF/MID/FW) from the season representation. A strong score proves the representation carries real role signal.

## Full catalog (n=3670)

| model | accuracy | macro-F1 | weighted-F1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4139 | 0.1464 | 0.2424 |
| RandomForest | 0.7386 | 0.7762 | 0.7381 |
| GradientBoosting | 0.7364 | 0.7746 | 0.7364 |

## Real-feature subset — StatsBomb-covered (n=1227)

Where high scores are legitimate (real features, real labels):

| model | accuracy | macro-F1 | weighted-F1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4104 | 0.1455 | 0.2389 |
| RandomForest | 0.8208 | 0.8388 | 0.8205 |
| GradientBoosting | 0.8404 | 0.8375 | 0.8381 |

**Best on real subset: RandomForest — macro-F1 0.8388.**