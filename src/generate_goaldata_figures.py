from __future__ import annotations

import shutil
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
ARTIFACTS_DIR = BASE_DIR / "artifacts"

NULL_MARKERS = {"", "NULL", "NAN", "NA", "NONE", "<NA>", "NOT_AVAILABLE_IN_SOURCE"}

DATASETS = {
    "matches": PROCESSED_DIR / "core" / "matches_cleaned",
    "teams": PROCESSED_DIR / "core" / "teams_cleaned",
    "players": PROCESSED_DIR / "core" / "players_cleaned",
    "goals": PROCESSED_DIR / "events" / "goals_events_cleaned",
    "goalkeepers": PROCESSED_DIR / "stats" / "goalkeeper_stats_cleaned",
    "player-match": PROCESSED_DIR / "stats" / "player_match_stats_cleaned",
    "player-season": PROCESSED_DIR / "stats" / "player_season_stats_cleaned",
}


def read_table(stem: Path, nrows: int | None = None) -> pd.DataFrame:
    parquet_path = stem.with_suffix(".parquet")
    csv_path = stem.with_suffix(".csv")
    if nrows is not None and csv_path.exists():
        return pd.read_csv(csv_path, nrows=nrows, keep_default_na=False, low_memory=False)
    if parquet_path.exists():
        frame = pd.read_parquet(parquet_path)
        return frame.head(nrows) if nrows is not None else frame
    if csv_path.exists():
        return pd.read_csv(csv_path, nrows=nrows, keep_default_na=False, low_memory=False)
    raise FileNotFoundError(f"Missing processed table: {stem}.parquet or {stem}.csv")


def present_mask(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip().str.upper()
    return series.notna() & ~text.isin(NULL_MARKERS)


def completeness(frame: pd.DataFrame) -> float:
    if frame.empty or len(frame.columns) == 0:
        return 0.0
    present_cells = sum(int(present_mask(frame[column]).sum()) for column in frame.columns)
    return present_cells / (len(frame) * len(frame.columns))


def save_bar(
    values: pd.Series,
    path: Path,
    title: str,
    xlabel: str,
    ylabel: str,
    color: str = "#315c8a",
    figsize: tuple[float, float] = (10, 5.8),
) -> None:
    fig, ax = plt.subplots(figsize=figsize)
    values.plot(kind="bar", ax=ax, color=color, edgecolor="#1f2933", linewidth=0.5)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.25)
    ax.set_axisbelow(True)
    plt.xticks(rotation=35, ha="right")
    plt.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def plot_table_completeness(tables: dict[str, pd.DataFrame]) -> None:
    values = pd.Series({name: completeness(df) * 100 for name, df in tables.items()}).sort_values()
    fig, ax = plt.subplots(figsize=(10, 5.8))
    bars = ax.barh(values.index, values.values, color="#2f6f73", edgecolor="#1f2933", linewidth=0.5)
    ax.set_title("Completeness by Processed Table")
    ax.set_xlabel("Completeness (%)")
    ax.set_xlim(0, 100)
    ax.grid(axis="x", alpha=0.25)
    ax.set_axisbelow(True)
    for bar in bars:
        width = bar.get_width()
        ax.text(min(width + 1, 98), bar.get_y() + bar.get_height() / 2, f"{width:.1f}%", va="center", fontsize=9)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "table_completeness.png", dpi=170)
    plt.close(fig)


def plot_possession_distribution(matches: pd.DataFrame) -> None:
    values = []
    for column in ["possession_home", "possession_away"]:
        if column in matches.columns:
            values.append(pd.to_numeric(matches[column], errors="coerce"))
    possession = pd.concat(values, ignore_index=True).dropna()
    possession = possession[possession.between(0, 1)]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.hist(possession * 100, bins=24, color="#7f5539", edgecolor="#fff8f0", linewidth=0.7)
    ax.axvline((possession * 100).mean(), color="#1f2933", linestyle="--", linewidth=1.2, label="Mean")
    ax.set_title("Distribution of Team Possession")
    ax.set_xlabel("Possession (%)")
    ax.set_ylabel("Team-match observations")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(frameon=False)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "possession_distribution.png", dpi=170)
    plt.close(fig)


def plot_matches_by_season(matches: pd.DataFrame) -> None:
    counts = matches["season"].astype(str).value_counts().sort_index()
    save_bar(
        counts,
        FIGURES_DIR / "matches_by_season.png",
        "Matches by Season",
        "Season",
        "Matches",
        color="#476a2a",
        figsize=(12, 5.8),
    )


def plot_match_field_coverage(matches: pd.DataFrame) -> None:
    fields = [
        "stadium",
        "city",
        "country",
        "referee",
        "home_score",
        "away_score",
        "possession_home",
        "possession_away",
        "total_goals",
        "home_team_id",
        "away_team_id",
    ]
    values = {}
    for field in fields:
        if field in matches.columns:
            values[field] = present_mask(matches[field]).mean() * 100
    series = pd.Series(values).sort_values()
    fig, ax = plt.subplots(figsize=(10, 5.6))
    bars = ax.barh(series.index, series.values, color="#4f6d7a", edgecolor="#1f2933", linewidth=0.5)
    ax.set_title("Coverage of Selected Match Fields")
    ax.set_xlabel("Coverage (%)")
    ax.set_xlim(0, 100)
    ax.grid(axis="x", alpha=0.25)
    for bar in bars:
        width = bar.get_width()
        ax.text(min(width + 1, 98), bar.get_y() + bar.get_height() / 2, f"{width:.1f}%", va="center", fontsize=9)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "match_field_coverage.png", dpi=170)
    plt.close(fig)


def ensure_pca_figures() -> None:
    source_scatter = ARTIFACTS_DIR / "pca_player_season_2d.png"
    source_variance = ARTIFACTS_DIR / "pca_cumulative_variance.png"
    if not source_scatter.exists() or not source_variance.exists():
        from src.build_pca_feature_matrix import main as build_pca

        build_pca()

    shutil.copy2(source_scatter, FIGURES_DIR / "pca_player_season_generated.png")
    shutil.copy2(source_variance, FIGURES_DIR / "pca_cumulative_variance_generated.png")


def team_label_lookup(teams: pd.DataFrame) -> dict[str, str]:
    if {"team_id", "team_name"}.issubset(teams.columns):
        return teams.drop_duplicates("team_id").set_index("team_id")["team_name"].astype(str).to_dict()
    return {}


def plot_top_network_teams(matches: pd.DataFrame, teams: pd.DataFrame) -> None:
    lookup = team_label_lookup(teams)
    home = matches["home_team_id"].astype(str) if "home_team_id" in matches.columns else pd.Series(dtype=str)
    away = matches["away_team_id"].astype(str) if "away_team_id" in matches.columns else pd.Series(dtype=str)
    counts = pd.concat([home, away], ignore_index=True)
    counts = counts[~counts.str.upper().isin(NULL_MARKERS)]
    top = counts.value_counts().head(15)
    top.index = [lookup.get(team_id, team_id) for team_id in top.index]
    top = top.sort_values()

    fig, ax = plt.subplots(figsize=(10, 6.2))
    bars = ax.barh(top.index, top.values, color="#8a5a44", edgecolor="#1f2933", linewidth=0.5)
    ax.set_title("Top Teams by Match Participation")
    ax.set_xlabel("Appearances as home or away team")
    ax.grid(axis="x", alpha=0.25)
    for bar in bars:
        width = bar.get_width()
        ax.text(width + max(top.values) * 0.01, bar.get_y() + bar.get_height() / 2, f"{int(width):,}", va="center", fontsize=9)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "top_network_teams.png", dpi=170)
    plt.close(fig)


def main() -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    # Completeness is estimated from a deterministic prefix sample for very
    # large tables so this report script stays fast on classroom hardware.
    completeness_tables = {name: read_table(stem, nrows=100_000) for name, stem in DATASETS.items()}
    matches = read_table(DATASETS["matches"])
    teams = read_table(DATASETS["teams"])

    plot_table_completeness(completeness_tables)
    plot_possession_distribution(matches)
    plot_matches_by_season(matches)
    plot_match_field_coverage(matches)
    ensure_pca_figures()
    plot_top_network_teams(matches, teams)

    for path in sorted(FIGURES_DIR.glob("*.png")):
        print(path.relative_to(BASE_DIR))


if __name__ == "__main__":
    main()
