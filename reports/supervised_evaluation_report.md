# Supervised Evaluation — Position Classification

Generated: `2026-07-11T09:17:56`

Task: predict position group (GK/DEF/MID/FW) from the season representation plus real StatsBomb event-style features (career profile per player). A strong score proves the representation carries real role signal.

## Full catalog (n=3670)

| model | accuracy | macro-F1 | weighted-F1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4139 | 0.1464 | 0.2424 |
| RandomForest | 0.7440 | 0.7833 | 0.7434 |
| GradientBoosting | 0.7429 | 0.7792 | 0.7431 |
| HistGradientBoosting | 0.7571 | 0.7858 | 0.7566 |

## Real-feature subset — StatsBomb-covered (n=1227)

Where high scores are legitimate (real features, real labels):

| model | accuracy | macro-F1 | weighted-F1 |
| --- | --- | --- | --- |
| baseline (majority class) | 0.4104 | 0.1455 | 0.2389 |
| RandomForest | 0.8436 | 0.8618 | 0.8436 |
| GradientBoosting | 0.8697 | 0.8658 | 0.8685 |
| HistGradientBoosting | 0.8730 | 0.8842 | 0.8729 |

**Best on real subset: HistGradientBoosting — macro-F1 0.8842.**