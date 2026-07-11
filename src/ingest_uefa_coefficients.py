"""
UEFA country-coefficient ingestion (league competitiveness per season)
======================================================================

Downloads the official UEFA 5-year country coefficients for every season
covered by the pipeline (2004-2005 .. 2025-2026). The coefficient method is
defined by UEFA (https://www.uefa.com/nationalassociations/uefarankings/country/about/);
historical per-season tables are read from Bert Kassies' long-running archive
(https://kassiesa.net/uefa/), which reproduces the official UEFA calculation.

Why: raw per-90 stats are not comparable across leagues — 50 goals in the
Primeira Liga are not worth 50 goals in the Premier League. The 5-year country
coefficient is the official, season-specific measure of league strength and is
used by `src/league_strength.py` to weight decision-layer ratings.

Output: data/raw/uefa_country_coefficients.csv
        (season, country, coefficient_5yr, rank, teams)

Run:  python -m src.ingest_uefa_coefficients
"""
from __future__ import annotations

import time
import urllib.request
from io import StringIO
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
OUT = BASE / "data" / "raw" / "uefa_country_coefficients.csv"

# kassiesa.net archives each UEFA calculation era under a method directory.
# Year = season ending year (crank2012.html = season 2011-2012).
def _method(year: int) -> str:
    if year <= 2003:
        return "method2"
    if year <= 2008:
        return "method3"
    if year <= 2017:
        return "method4"
    return "method5"


YEARS = range(2005, 2027)  # seasons 2004-2005 .. 2025-2026
URL = "https://kassiesa.net/uefa/data/{method}/crank{year}.html"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def fetch_year(year: int) -> pd.DataFrame:
    url = URL.format(method=_method(year), year=year)
    req = urllib.request.Request(url, headers=HEADERS)
    html = urllib.request.urlopen(req, timeout=60).read().decode("utf-8", errors="replace")
    table = max(pd.read_html(StringIO(html)), key=len)
    cols = {c.lower(): c for c in table.columns.astype(str)}
    df = pd.DataFrame({
        "season": f"{year - 1}-{year}",
        "country": table[cols["country"]].astype(str).str.strip(),
        "coefficient_5yr": pd.to_numeric(table[cols["ranking"]], errors="coerce"),
        "rank": pd.to_numeric(table[cols["#"]], errors="coerce"),
        "teams": pd.to_numeric(table.get(cols.get("teams")), errors="coerce"),
    })
    return df.dropna(subset=["coefficient_5yr"])


def main() -> None:
    have: set[str] = set()
    frames: list[pd.DataFrame] = []
    if OUT.exists():
        existing = pd.read_csv(OUT)
        have = set(existing["season"].astype(str).unique())
        frames.append(existing)
    for year in YEARS:
        season = f"{year - 1}-{year}"
        if season in have:
            continue
        try:
            df = fetch_year(year)
            frames.append(df)
            print(f"  {season}: {len(df)} countries (top: "
                  f"{df.sort_values('rank').iloc[0]['country']})", flush=True)
        except Exception as exc:
            print(f"  {season}: FAILED ({type(exc).__name__}: {str(exc)[:80]})", flush=True)
        time.sleep(2)  # polite crawl delay
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["season", "country"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False, encoding="utf-8")
    print(f"Saved {len(out):,} (season, country) coefficients -> {OUT}", flush=True)


if __name__ == "__main__":
    from src.logging_utils import run_logged
    run_logged("ingest_uefa_coefficients", main)
