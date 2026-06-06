"""Documented formulas for advanced football metrics.

The project uses this module as a single source of truth for derived columns.
Entries marked as ``requires_external_model`` are intentionally not computed
from aggregate CSVs because the cited methods require event locations, action
states, tracking data, or fitted model coefficients.
"""

ADVANCED_METRICS = {
    "expected_threat_total": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "(passes_completed * 0.05) + (dribbles * 0.07)",
        "required_columns": ["passes_completed", "dribbles"],
        "source": "Course-provided proxy formula; true xT needs event start/end locations.",
        "reference": "Singh, K. (2018). Expected Threat; Soccermatics xT notes.",
        "url": "https://soccermatics.readthedocs.io/en/latest/lesson4/xTPos.html",
        "requires_external_model": False,
    },
    "vaep_rating": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "(passes_completed + dribbles + tackles_won + interceptions) - fouls_committed",
        "required_columns": [
            "passes_completed",
            "dribbles",
            "tackles_won",
            "interceptions",
            "fouls_committed",
        ],
        "source": "Course-provided proxy formula; true VAEP estimates scoring and conceding probabilities from action states.",
        "reference": "Decroos et al. (2019). Actions Speak Louder than Goals.",
        "url": "https://socceraction.readthedocs.io/en/stable/api/generated/socceraction.vaep.VAEP.html",
        "requires_external_model": False,
    },
    "expected_assists": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "passes_attempted * pass_accuracy * 0.1",
        "required_columns": ["passes_attempted", "pass_accuracy"],
        "source": "Course-provided proxy formula; event-level xA normally needs pass recipient, location, and shot outcome context.",
        "reference": "Bransen and Van Haaren style pass valuation; Decroos et al. discuss action-value context.",
        "url": "https://www.kdd.org/kdd2019/accepted-papers/view/actions-speak-louder-than-goals-valuing-player-actions-in-soccer",
        "requires_external_model": False,
    },
    "progressive_pass_dist": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "distance_covered * (passes_completed / passes_attempted)",
        "required_columns": ["distance_covered", "passes_completed", "passes_attempted"],
        "source": "Course-provided proxy formula; true progressive-pass definitions need pass start/end coordinates.",
        "reference": "StatsBomb-style progressive passing concepts require spatial event data.",
        "url": "https://statsbomb.com/soccer-metrics/",
        "requires_external_model": False,
    },
    "defensive_action_height": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "(tackles + interceptions + clearances) / distance_covered",
        "required_columns": ["tackles", "interceptions", "clearances", "distance_covered"],
        "source": "Course-provided proxy formula; actual defensive height is spatial and needs action locations.",
        "reference": "Style-of-play clustering literature uses spatial defensive-action information.",
        "url": "",
        "requires_external_model": False,
    },
    "pressing_intensity": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "opponent_possession_rate / (tackles + interceptions + fouls_committed)",
        "required_columns": ["match_id", "team_id", "tackles", "interceptions", "fouls_committed"],
        "source": "Course-provided PPDA-inspired proxy using possession rate where opponent pass counts are unavailable.",
        "reference": "Pressing metrics such as PPDA divide opponent possession actions by defensive actions.",
        "url": "https://statsbomb.com/articles/soccer/defensive-metrics-measuring-the-intensity-of-a-high-press/",
        "requires_external_model": False,
    },
    "acwr_index": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "distance_covered_match / average_distance_covered_season",
        "required_columns": ["player_id", "match_id", "distance_covered"],
        "source": "ACWR ratio concept is supported, but this repo lacks non-null match distance for the processed match table.",
        "reference": "Rossi et al. (2018). Effective injury forecasting in soccer with GPS training data.",
        "url": "https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0201264",
        "requires_external_model": False,
    },
    "metabolic_power": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "(15.48 * top_speed) * (distance_covered / (minutes_played * 60))",
        "required_columns": ["top_speed", "distance_covered", "minutes_played"],
        "source": "Course-provided proxy formula; Osgnach metabolic power depends on locomotor energetics and tracking-derived movement.",
        "reference": "Osgnach et al. (2010). Energy cost and metabolic power in elite soccer.",
        "url": "https://pubmed.ncbi.nlm.nih.gov/20010116/",
        "requires_external_model": False,
    },
    "foul_severity_index": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "(fouls_committed * 1) + (yellow_cards * 3) + (red_cards * 6)",
        "required_columns": ["fouls_committed", "yellow_cards", "red_cards"],
        "source": "Course-provided weighted disciplinary index.",
        "reference": "Football disciplinary models commonly use fouls and card counts as risk features.",
        "url": "",
        "requires_external_model": False,
    },
    "possession_involvement": {
        "target_file": "data/processed/stats/player_match_stats_cleaned.csv",
        "formula": "(passes_attempted + dribbles) / (team_possession_rate * minutes_played)",
        "required_columns": ["match_id", "team_id", "passes_attempted", "dribbles", "minutes_played"],
        "source": "Course-provided possession-normalized involvement proxy.",
        "reference": "Pappalardo et al. (2019). PlayeRank / data-driven player evaluation.",
        "url": "https://doi.org/10.1145/3343172",
        "requires_external_model": False,
    },
    "xg_probability": {
        "target_file": "data/processed/events/goals_events_cleaned.csv",
        "formula": "logistic(B0 + beta * shot features)",
        "required_columns": ["shot_location", "angle", "distance", "body_part"],
        "source": "Not computed: processed goal events contain goals only, not the full shot population or spatial shot features.",
        "reference": "Anzer and Bauer (2021). A Goal Scoring Probability Model for Shots.",
        "url": "https://www.frontiersin.org/articles/10.3389/fspor.2021.624475/full",
        "requires_external_model": True,
    },
    "shot_quality_index": {
        "target_file": "data/processed/events/goals_events_cleaned.csv",
        "formula": "goals / xg_probability",
        "required_columns": ["xg_probability"],
        "source": "Not computed unless xg_probability is available from a calibrated xG model.",
        "reference": "xG performance comparison requires calibrated expected-goals probabilities.",
        "url": "https://www.frontiersin.org/articles/10.3389/fspor.2021.624475/full",
        "requires_external_model": True,
    },
}


def metric_rows():
    """Return formula metadata as row dictionaries for CSV/JSON reporting."""
    return [{"metric": name, **info} for name, info in ADVANCED_METRICS.items()]
