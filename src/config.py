import os
import json

_ALIASES: dict[str, list[str]] = {
    'psg': ['paris'],
    'paris saint germain': ['paris'],
    'inter milan': ['inter'],
    'atletico': ['atleti', 'atletico', 'atletico madrid'],
    'atletico madrid': ['atleti', 'atletico'],
    'bayer munich': ['baviera', 'munchen', 'munich'],
    'stade brestois': ['brest'],
    'as monaco': ['monaco'],
    'sl benfica': ['benfica'],
    'borussia dortmund': ['dortmund'],
    'rb leipzig': ['leipzig'],
    'losc lille': ['lille'],
}

def expand_aliases(name: str) -> list[str]:
    from .scrapers.utils import norm_text
    n = norm_text(name)
    result = {n}
    for alias_key, alias_vals in _ALIASES.items():
        if alias_key in n or n in alias_key:
            result.update(alias_vals)
    return list(result)

def load_espn_overrides():
    path = os.path.join(os.path.dirname(__file__), 'espn_id_overrides.json')
    if not os.path.exists(path): return {}
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data.get('overrides', {})
