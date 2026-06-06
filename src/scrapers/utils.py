import unicodedata
import re
import logging
from pathlib import Path
import pandas as pd
from formatter import soft_norm as norm_text

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

BASE_DIR = Path(__file__).resolve().parents[2]
LOGS_DIR = BASE_DIR / "logs"


def get_scraper_logger(name: str) -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(f"goaldata.scrapers.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.FileHandler(LOGS_DIR / f"{name}.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    return logger

# norm_text now uses the centralized soft_norm from formatter.py
def teams_match(a: str, b: str) -> bool:
    na, nb = norm_text(a), norm_text(b)
    if na == nb or na in nb or nb in na:
        return True
    stop = {'fc', 'cf', 'ac', 'sc', 'rb', 'bv', 'sl', 'de', 'as', 'us', 'ss', 'sb', 'cl'}
    wa = set(na.split()) - stop
    wb = set(nb.split()) - stop
    return bool(wa & wb)

def is_null(val) -> bool:
    if val is None:
        return True
    try:
        import numpy as np
        if isinstance(val, (float, int)) and pd.isna(val):
            return True
    except Exception:
        pass
    return str(val).strip() in ('NULL', '', 'nan', 'NaN', 'none', 'None')

def safe_pct(v) -> str:
    try:
        return f"{round(float(v))}%"
    except Exception:
        return 'NULL'

def sum_int(*vals) -> str:
    t, ok = 0, False
    for v in vals:
        try:
            t += int(float(v)); ok = True
        except Exception:
            pass
    return str(t) if ok else 'NULL'

def date_to_api(date_str: str) -> str:
    """'DD-MM-YYYY' -> 'YYYYMMDD'"""
    try:
        p = date_str.split('-')
        return f"{p[2]}{p[1]}{p[0]}"
    except Exception:
        return ''

def date_to_iso(date_str: str) -> str:
    """'DD-MM-YYYY' -> 'YYYY-MM-DD'"""
    try:
        p = date_str.split('-')
        return f"{p[2]}-{p[1]}-{p[0]}"
    except Exception:
        return ''
