import unicodedata
import re
import pandas as pd

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def norm_text(s: str) -> str:
    if not s: return ""
    s = s.lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    return ' '.join(s.split())

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
