import pandas as pd
import json
import glob
import os

all_matches = []

# Carpeta data contiene subcarpetas como 2011-12, 2012-13...
# Llegar hasta estas carpetas
print(glob.glob("../data/*/*"))

for folder in glob.glob("../data/*/"):
    file = os.path.join(folder, "uefa.cl.json")
    if os.path.exists(file):
        season = os.path.basename(os.path.normpath(folder))  # nombre de la carpeta como temporada
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "matches" in data:
                for m in data["matches"]:
                    match = {
                        "season": season,
                        "round": m.get("round"),
                        "date": m.get("date"),
                        "time": m.get("time"),
                        "team1": m.get("team1"),
                        "team2": m.get("team2"),
                        "score_ht1": m.get("score", {}).get("ht", [None, None])[0],
                        "score_ht2": m.get("score", {}).get("ht", [None, None])[1],
                        "score_ft1": m.get("score", {}).get("ft", [None, None])[0],
                        "score_ft2": m.get("score", {}).get("ft", [None, None])[1],
                    }
                    all_matches.append(match)

# Convertir a DataFrame
df = pd.DataFrame(all_matches)

# Guardar CSV maestro
df.to_csv("champions_league_2011_2025.csv", index=False)