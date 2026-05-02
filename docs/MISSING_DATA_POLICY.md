# Missing Data Policy

The project uses missing-value imputation only as a last step after source
cross-reference. The goal is not to make the tables look complete; the goal is
to make them true enough for analysis.

## References Used

- Little and Rubin, *Statistical Analysis with Missing Data*.
- van Buuren, *Flexible Imputation of Missing Data*.
- Khan and Hoque, "SICE: an improved missing data imputation technique",
  *Journal of Big Data*, 2020.
- Brini and van den Heuvel, "Missing Data Imputation with High-Dimensional
  Data", *The American Statistician*, 2024.
- Kontos and Karlis, "Football analytics based on player tracking data using
  interpolation techniques for the prediction of missing coordinates", 2023.
- Butera et al., "Hot Deck Multiple Imputation for Handling Missing
  Accelerometer Data", 2019.
- Decroos et al., "Actions Speak Louder than Goals", 2019, for football
  action normalization and per-90 reasoning.

## Rules

1. Never overwrite observed non-empty values.
2. Never create extra rows to satisfy the 1.5M-record gate.
3. Treat `NULL`, empty strings, `NA`, `NAN`, `NONE`, and invalid failed parses
   as missing.
4. Reclassify impossible football values as missing, for example both teams
   having `0.0` possession in a completed match.
5. Use formulas only when all formula inputs are observed or already justified.
6. Prefer medians over means for skewed counting stats.
7. Group imputations by football context where possible: player, season, team,
   position, competition phase, and minutes played.
8. Record the method, affected rows, columns, and justification in logs.

## Formula Checks

- `pass_accuracy = passes_completed / passes_attempted`
- `shots_off_target = shots - shots_on_target - shots_blocked`
- `tackles_lost = tackles - tackles_won`
- `possession_home + possession_away ~= 1.0`
- completed passes cannot exceed attempted passes;
- shots on target cannot exceed total shots;
- red/yellow cards, goals, assists, fouls, and offsides cannot be negative.

## Large-Gap Rule

If a column has more than 0.25% missing values, the pipeline must first try new
source coverage. Statistical inference can be used only after the report makes
the limitation visible and cites the method used.
