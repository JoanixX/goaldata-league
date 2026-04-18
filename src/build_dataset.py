import pandas as pd

# CONFIG
RAW_PATH = "champions_league_2011_2025.csv"
PROCESSED_PATH = "data/processed/cleaned_data.csv"
DICT_PATH = "data/processed/data_dictionary_v1.csv"

# CLEANING FUNCTION
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    print("Checking null values...")
    null_ratio = df.isnull().mean().sort_values(ascending=False)
    print(null_ratio)

    # 1. Delete columns that exceed the 90% threeshold test of NULL values
    cols_to_drop = null_ratio[null_ratio > 0.9].index
    df = df.drop(columns=cols_to_drop)

    # 2. Delete not important and dispensable columnas
    for col in ["hora_inicio", "hora_fin"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # 4. Convert fecha to Date
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    # 5. Processing marcador to numeric column
    if "marcador" in df.columns:
        df["marcador"] = df["marcador"].str.replace(r'\(.*\)', '', regex=True)

        goles_split = df["marcador"].str.extract(r'(\d+)\D+(\d+)')
        df["home_goals"] = pd.to_numeric(goles_split[0], errors="coerce")
        df["away_goals"] = pd.to_numeric(goles_split[1], errors="coerce")

    return df


# Data Dictionary functional
def create_data_dictionary(df: pd.DataFrame) -> pd.DataFrame:

    descriptions = {
        "season": "Season of the competition",
        "fase": "Competition phase",
        "instancia": "Match round",
        "fecha": "Match date",
        "local": "Home team",
        "visitante": "Away team",
        "marcador": "Match score",
        "home_goals": "Goals scored by home team",
        "away_goals": "Goals scored by away team",
        "global": "Aggregate score in knockout rounds",
        "estadio": "Stadium name",
        "ciudad": "City",
        "pais": "Country",
        "entrenador_local": "Home coach",
        "entrenador_visitante": "Away coach",
        "goles": "Goal details"
    }

    data_dict = pd.DataFrame({
        "column": df.columns,
        "dtype": df.dtypes.astype(str),
        "missing": df.isnull().sum(),
        "missing_%": (df.isnull().mean() * 100).round(2),
        "description": [descriptions.get(col, "") for col in df.columns],
        "example": [
            str(df[col].dropna().iloc[0]) if df[col].dropna().shape[0] > 0 else ""
            for col in df.columns
        ]
    })

    return data_dict


# MAIN PIPELINE
def main():
    print("Loading dataset...")
    df = pd.read_csv(RAW_PATH)

    print("Cleaning dataset...")
    df_clean = clean_dataframe(df)

    print("Saving processed dataset...")
    df_clean.to_csv(PROCESSED_PATH, index=False)

    print("Creating data dictionary...")
    data_dict = create_data_dictionary(df_clean)
    data_dict.to_csv(DICT_PATH, index=False)

    print("Final shape:", df_clean.shape)
    print("Well Done!")


if __name__ == "__main__":
    main()