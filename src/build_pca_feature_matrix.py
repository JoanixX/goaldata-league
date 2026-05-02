"""Build a PCA-ready feature matrix for UCL player-season profiles.

The unit of analysis is one player in one season. This level is denser than
player-match rows and is better suited for PCA because PCA is sensitive to
missingness and repeated artificial values.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import variance

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
FEATURES_DIR = PROCESSED_DIR / "features"
ARTIFACTS_DIR = BASE_DIR / "artifacts"
REPORTS_DIR = BASE_DIR / "reports"

os.environ.setdefault("MPLCONFIGDIR", str(ARTIFACTS_DIR / ".matplotlib-cache"))

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


PLAYER_SEASON_PATH = PROCESSED_DIR / "stats" / "player_season_stats_cleaned.parquet"
PLAYERS_PATH = PROCESSED_DIR / "core" / "players_cleaned.parquet"
FEATURE_MATRIX_PATH = PROCESSED_DIR / "player_season_feature_matrix.csv"
PCA_TRANSFORMED_PATH = ARTIFACTS_DIR / "pca_player_season_2d.csv"
PCA_VARIANCE_PATH = ARTIFACTS_DIR / "pca_explained_variance.csv"
PCA_LOADINGS_PATH = ARTIFACTS_DIR / "pca_component_loadings.csv"
PCA_PLOT_PATH = ARTIFACTS_DIR / "pca_player_season_2d.png"
PCA_REPORT_PATH = REPORTS_DIR / "pca_feature_matrix_report.md"
PCA_JSON_REPORT_PATH = ARTIFACTS_DIR / "pca_feature_matrix_report.json"

NULL_MARKERS = {"", "NULL", "NAN", "NA", "NONE"}

BASE_NUMERIC_COLUMNS = [
    "matches_played",
    "minutes_played",
    "goals",
    "assists",
    "shots",
    "shots_on_target",
    "passes_completed",
    "passes_attempted",
    "tackles",
    "interceptions",
    "fouls_committed",
    "yellow_cards",
    "red_cards",
]

ENGINEERED_NUMERIC_COLUMNS = [
    "minutes_per_match",
    "goals_per90",
    "assists_per90",
    "shots_per90",
    "shots_on_target_per90",
    "passes_completed_per90",
    "passes_attempted_per90",
    "tackles_per90",
    "interceptions_per90",
    "fouls_committed_per90",
    "cards_per90",
    "shot_accuracy",
    "pass_accuracy",
    "goal_conversion_rate",
    "defensive_actions_per90",
    "discipline_points_per90",
]

CATEGORICAL_COLUMNS = ["season", "position_group"]


@dataclass
class PCAResult:
    feature_matrix: pd.DataFrame
    transformed: pd.DataFrame
    variance: pd.DataFrame
    loadings: pd.DataFrame
    optimal_components_90: int
    numeric_columns: list[str]
    categorical_columns: list[str]
    encoded_feature_names: list[str]


def null_to_na(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.replace(list(NULL_MARKERS), pd.NA)


def to_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.where(denominator != 0)


def normalize_position(position: object) -> str:
    text = "" if pd.isna(position) else str(position).strip().casefold()
    if not text or text in NULL_MARKERS:
        return "Unknown"
    if any(token in text for token in ["keeper", "goalkeeper", "portero"]):
        return "Goalkeeper"
    if any(token in text for token in ["back", "defender", "defence", "defense", "centre-back", "full-back"]):
        return "Defender"
    if any(token in text for token in ["midfield", "midfielder"]):
        return "Midfielder"
    if any(token in text for token in ["forward", "striker", "winger", "attack"]):
        return "Forward"
    return "Other"


def load_player_season_dataset() -> pd.DataFrame:
    if not PLAYER_SEASON_PATH.exists():
        raise FileNotFoundError(f"Missing input: {PLAYER_SEASON_PATH}")

    # Parquet preserves column dtypes, so no null-string cleanup or manual
    # to_numeric coercion is needed.
    stats = pd.read_parquet(PLAYER_SEASON_PATH)
    players = pd.read_parquet(PLAYERS_PATH) if PLAYERS_PATH.exists() else pd.DataFrame()

    if not players.empty:
        player_meta = players[["player_id", "player_name", "position", "nationality", "team_id"]].drop_duplicates("player_id")
        stats = stats.merge(player_meta, on="player_id", how="left")
    else:
        stats["player_name"] = pd.NA
        stats["position"] = pd.NA
        stats["nationality"] = pd.NA
        stats["team_id"] = pd.NA

    stats["position_group"] = stats["position"].map(normalize_position)
    stats["season"] = stats["season"].fillna("Unknown").astype(str)
    return stats


def add_engineered_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    minutes = out["minutes_played"]
    matches = out["matches_played"]

    out["minutes_per_match"] = safe_div(minutes, matches)
    for column in [
        "goals",
        "assists",
        "shots",
        "shots_on_target",
        "passes_completed",
        "passes_attempted",
        "tackles",
        "interceptions",
        "fouls_committed",
    ]:
        out[f"{column}_per90"] = safe_div(out[column] * 90, minutes)

    yellow = out["yellow_cards"].fillna(0)
    red = out["red_cards"].fillna(0)
    out["cards_per90"] = safe_div((yellow + red) * 90, minutes)
    out["shot_accuracy"] = safe_div(out["shots_on_target"], out["shots"])
    out["pass_accuracy"] = safe_div(out["passes_completed"], out["passes_attempted"])
    out["goal_conversion_rate"] = safe_div(out["goals"], out["shots"])
    out["defensive_actions_per90"] = safe_div((out["tackles"] + out["interceptions"]) * 90, minutes)
    out["discipline_points_per90"] = safe_div((out["fouls_committed"] + yellow * 3 + red * 6) * 90, minutes)
    return out


def build_feature_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    keep_columns = [
        "player_id",
        "player_name",
        "season",
        "position_group",
        *BASE_NUMERIC_COLUMNS,
        *ENGINEERED_NUMERIC_COLUMNS,
    ]
    matrix = frame[keep_columns].copy()
    matrix = matrix.dropna(subset=["minutes_played"], how="all")
    return matrix


def fit_pca(feature_matrix: pd.DataFrame) -> PCAResult:
    numeric_columns = BASE_NUMERIC_COLUMNS + ENGINEERED_NUMERIC_COLUMNS
    categorical_columns = CATEGORICAL_COLUMNS

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_columns,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical_columns,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    x_prepared = preprocessor.fit_transform(feature_matrix)
    encoded_feature_names = list(preprocessor.get_feature_names_out())

    # StandardScaler is also applied after OHE so binary dummy variables do not
    # become negligible solely because their numerical range differs from rates.
    final_scaler = StandardScaler()
    x_scaled = final_scaler.fit_transform(x_prepared)

    pca = PCA()
    components = pca.fit_transform(x_scaled)
    cumulative = pca.explained_variance_ratio_.cumsum()
    optimal_components_90 = int((cumulative >= 0.90).argmax() + 1)

    transformed = pd.DataFrame(
        {
            "player_id": feature_matrix["player_id"].values,
            "player_name": feature_matrix["player_name"].fillna("Unknown").values,
            "season": feature_matrix["season"].values,
            "position_group": feature_matrix["position_group"].values,
            "PC1": components[:, 0],
            "PC2": components[:, 1],
        }
    )

    variance = pd.DataFrame(
        {
            "component": [f"PC{i + 1}" for i in range(len(pca.explained_variance_ratio_))],
            "explained_variance_ratio": pca.explained_variance_ratio_,
            "cumulative_explained_variance": cumulative,
        }
    )

    loadings = pd.DataFrame(
        pca.components_.T,
        index=encoded_feature_names,
        columns=[f"PC{i + 1}" for i in range(pca.n_components_)],
    ).reset_index(names="feature")

    return PCAResult(
        feature_matrix=feature_matrix,
        transformed=transformed,
        variance=variance,
        loadings=loadings,
        optimal_components_90=optimal_components_90,
        numeric_columns=numeric_columns,
        categorical_columns=categorical_columns,
        encoded_feature_names=encoded_feature_names,
    )


def plot_pca_2d(transformed: pd.DataFrame) -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    palette = {
        "Goalkeeper": "#4c78a8",
        "Defender": "#f58518",
        "Midfielder": "#54a24b",
        "Forward": "#e45756",
        "Other": "#72b7b2",
        "Unknown": "#9d9da1",
    }

    plt.figure(figsize=(9, 6))
    for group, subset in transformed.groupby("position_group"):
        plt.scatter(
            subset["PC1"],
            subset["PC2"],
            s=28,
            alpha=0.75,
            label=group,
            color=palette.get(group, "#9d9da1"),
            edgecolors="none",
        )
    plt.axhline(0, color="#d0d0d0", linewidth=0.8)
    plt.axvline(0, color="#d0d0d0", linewidth=0.8)
    plt.title("PCA 2D - UCL Player-Season Profiles")
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.legend(title="Position group", loc="best")
    plt.tight_layout()
    plt.savefig(PCA_PLOT_PATH, dpi=160)
    plt.close()

def plot_variance(variance: pd.DataFrame) -> None:
    plt.figure(figsize=(8,5))
    
    plt.plot(
    range(1, len(variance) + 1),
    variance["cumulative_explained_variance"]
)
    plt.xticks(range(1, len(variance) + 1))

    plt.title("Cumulative Explained Variance")
    plt.xlabel("Component")
    plt.ylabel("Variance")
    plt.tight_layout()

    plt.savefig(ARTIFACTS_DIR / "pca_cumulative_variance.png")
    plt.close()

def top_loadings(loadings: pd.DataFrame, component: str, n: int = 8) -> pd.DataFrame:
    temp = loadings[["feature", component]].copy()
    temp["abs_loading"] = temp[component].abs()
    return temp.sort_values("abs_loading", ascending=False).head(n)


def markdown_table(frame: pd.DataFrame) -> str:
    columns = list(frame.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in frame.iterrows():
        values = []
        for column in columns:
            value = row[column]
            if isinstance(value, float):
                values.append(f"{value:.6f}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_outputs(result: PCAResult) -> None:
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    result.feature_matrix.to_csv(FEATURE_MATRIX_PATH, index=False, encoding="utf-8")
    result.transformed.to_csv(PCA_TRANSFORMED_PATH, index=False, encoding="utf-8")
    result.variance.to_csv(PCA_VARIANCE_PATH, index=False, encoding="utf-8")
    result.loadings.to_csv(PCA_LOADINGS_PATH, index=False, encoding="utf-8")
    plot_pca_2d(result.transformed)
    plot_variance(result.variance)

    pc1_top = top_loadings(result.loadings, "PC1")
    pc2_top = top_loadings(result.loadings, "PC2")
    first_two_variance = float(result.variance.head(2)["explained_variance_ratio"].sum())
    cumulative_90 = float(result.variance.iloc[result.optimal_components_90 - 1]["cumulative_explained_variance"])

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input": str(PLAYER_SEASON_PATH),
        "feature_matrix": str(FEATURE_MATRIX_PATH),
        "rows": int(len(result.feature_matrix)),
        "numeric_columns": result.numeric_columns,
        "categorical_columns": result.categorical_columns,
        "encoded_feature_count": int(len(result.encoded_feature_names)),
        "optimal_components_90": result.optimal_components_90,
        "cumulative_variance_at_optimal": cumulative_90,
        "first_two_components_variance": first_two_variance,
        "outputs": {
            "pca_2d": str(PCA_TRANSFORMED_PATH),
            "variance": str(PCA_VARIANCE_PATH),
            "loadings": str(PCA_LOADINGS_PATH),
            "plot": str(PCA_PLOT_PATH),
            "report": str(PCA_REPORT_PATH),
        },
    }
    PCA_JSON_REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    markdown = f"""# PCA Feature Matrix Report

Generated at: `{report['generated_at']}`

## Unit of Analysis

The feature matrix uses one row per `player_id` and `season` from
`data/processed/stats/player_season_stats_cleaned.parquet`. This level was chosen
because player-season aggregates are denser and more stable than player-match
rows, which still contain many source-limited `NULL` values for tracking and
event-detail columns.

## Variable Selection

### Numeric Variables Used

Base numeric columns:

`{', '.join(BASE_NUMERIC_COLUMNS)}`

Engineered numeric columns:

`{', '.join(ENGINEERED_NUMERIC_COLUMNS)}`

These variables describe attacking output, chance volume, passing involvement,
defensive activity, minutes/load, and discipline. They are relevant for PCA
because the objective is to discover latent player profiles rather than predict
a single target.

### Categorical Variables Used

`season` and `position_group` were encoded with One-Hot Encoding. PCA can only
operate on numbers, so categories must be converted into numeric dummy columns.
`position_group` adds tactical role context, while `season` controls for source
and competition-period differences.

### Variables Discarded

- Identifiers such as `player_id`, `team_id`, and names are excluded from PCA
  because they are keys, not behavioral features.
- `nationality` is excluded because it has high missingness and would add many
  sparse identity dummies that do not directly describe player performance.
- Raw player-match advanced metrics with 100% missingness are excluded from this
  PCA matrix because they would add no signal.

## Missing Values

Numeric missing values are imputed with the median. Median imputation is robust
to skewed football statistics, where a few elite players can have very high
values. Categorical missing values are imputed with the most frequent category
inside the preprocessing pipeline. No missing values are replaced with invented
football events or source data.

## Feature Engineering

Per-90 rates normalize production by playing time, making bench players and
starters more comparable. Accuracy and conversion rates summarize efficiency.
Discipline and defensive-action rates add behavioral context beyond goals and
assists.

## Scaling

`StandardScaler` is applied before PCA. PCA is variance-based: without scaling,
large-scale variables such as minutes or passes would dominate the principal
components simply because their units are larger. Scaling gives each feature
mean 0 and standard deviation 1, so PCA reflects correlation structure instead
of raw measurement scale.

## PCA Results

- Rows in feature matrix: `{report['rows']}`
- Encoded feature count after One-Hot Encoding: `{report['encoded_feature_count']}`
- Components needed to reach at least 90% cumulative explained variance:
  `{result.optimal_components_90}`
- Cumulative explained variance at that point:
  `{cumulative_90:.4f}`
- Explained variance captured by PC1 + PC2:
  `{first_two_variance:.4f}`

The full cumulative explained variance table is saved to:
`{PCA_VARIANCE_PATH.relative_to(BASE_DIR)}`

## Component Interpretation

Top absolute loadings for PC1:

{markdown_table(pc1_top)}

Top absolute loadings for PC2:

{markdown_table(pc2_top)}

PC1 and PC2 are weighted combinations of the original scaled features. They do
not represent one original statistic; instead, they summarize dominant patterns
such as attacking volume, passing involvement, defensive activity, discipline,
or position/season structure depending on the loadings above.

## 2D Plot Interpretation

The 2D PCA plot is saved to `{PCA_PLOT_PATH.relative_to(BASE_DIR)}`. Points that
are close together represent player-seasons with similar statistical profiles.
Separation by `position_group` suggests that the feature matrix captures
tactical role differences. Overlap between groups is expected in football data,
especially for hybrid roles such as attacking midfielders or wing-backs.

## Why PCA Helps This Dataset

PCA reduces many correlated football statistics into fewer orthogonal
components. This helps downstream clustering, similarity search, ranking, and
recommendation by reducing redundancy and noise while preserving most of the
variance in player-season profiles.
"""
    PCA_REPORT_PATH.write_text(markdown, encoding="utf-8")


def main() -> None:
    raw = load_player_season_dataset()
    engineered = add_engineered_features(raw)
    feature_matrix = build_feature_matrix(engineered)
    result = fit_pca(feature_matrix)
    write_outputs(result)
    print(
        json.dumps(
            {
                "feature_matrix": str(FEATURE_MATRIX_PATH),
                "variance": str(PCA_VARIANCE_PATH),
                "plot": str(PCA_PLOT_PATH),
                "optimal_components_90": result.optimal_components_90,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()