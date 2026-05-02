from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from bs4 import BeautifulSoup


SKIP_EXTENSIONS = {
    ".aac",
    ".avi",
    ".flac",
    ".m4a",
    ".mkv",
    ".mov",
    ".mp3",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ogg",
    ".wav",
    ".webm",
    ".wmv",
}


@dataclass(frozen=True)
class IngestedTable:
    source_path: str
    source_type: str
    table_name: str
    data: pd.DataFrame


def iter_supported_files(path: str | Path) -> Iterable[Path]:
    path = Path(path)
    if path.is_dir():
        for child in path.rglob("*"):
            if child.is_file() and child.suffix.lower() not in SKIP_EXTENSIONS:
                yield child
    elif path.is_file() and path.suffix.lower() not in SKIP_EXTENSIONS:
        yield path


def read_source(path: str | Path, extract_dir: str | Path | None = None) -> list[IngestedTable]:
    """Read structured files, directories, or zip archives into data frames.

    Supported source types: CSV/TSV, Excel, JSON/JSONL, HTML tables, Parquet,
    plain text, directories, and ZIP archives containing any supported file.
    Audio and video formats are intentionally skipped.
    """
    path = Path(path)
    if path.is_dir():
        tables: list[IngestedTable] = []
        for file_path in iter_supported_files(path):
            tables.extend(read_source(file_path, extract_dir=extract_dir))
        return tables

    suffix = path.suffix.lower()
    if suffix in SKIP_EXTENSIONS:
        return []
    if suffix == ".zip":
        target = Path(extract_dir) if extract_dir else path.with_suffix("")
        target.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path) as archive:
            archive.extractall(target)
        return read_source(target, extract_dir=target)
    if suffix == ".csv":
        return [_table(path, "csv", path.stem, pd.read_csv(path, keep_default_na=False))]
    if suffix == ".tsv":
        return [_table(path, "tsv", path.stem, pd.read_csv(path, sep="\t", keep_default_na=False))]
    if suffix in {".xlsx", ".xls"}:
        sheets = pd.read_excel(path, sheet_name=None, keep_default_na=False)
        return [_table(path, "excel", sheet_name, df) for sheet_name, df in sheets.items()]
    if suffix == ".jsonl":
        return [_table(path, "jsonl", path.stem, pd.read_json(path, lines=True))]
    if suffix == ".json":
        return _read_json(path)
    if suffix in {".html", ".htm"}:
        return _read_html(path)
    if suffix == ".parquet":
        return [_table(path, "parquet", path.stem, pd.read_parquet(path))]
    return [_table(path, "text", path.stem, _read_text(path))]


def _table(path: Path, source_type: str, table_name: str, df: pd.DataFrame) -> IngestedTable:
    df = df.copy()
    df["__source_path"] = str(path)
    df["__source_type"] = source_type
    df["__source_table"] = table_name
    return IngestedTable(str(path), source_type, table_name, df)


def _read_json(path: Path) -> list[IngestedTable]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        return [_table(path, "json", path.stem, pd.json_normalize(payload))]
    if isinstance(payload, dict):
        tables = []
        tabular_items = {k: v for k, v in payload.items() if isinstance(v, list)}
        if tabular_items:
            for name, rows in tabular_items.items():
                tables.append(_table(path, "json", name, pd.json_normalize(rows)))
            return tables
        return [_table(path, "json", path.stem, pd.json_normalize(payload))]
    return [_table(path, "json", path.stem, pd.DataFrame({"value": [payload]}))]


def _read_html(path: Path) -> list[IngestedTable]:
    tables = pd.read_html(path)
    if tables:
        return [_table(path, "html", f"{path.stem}_{idx}", df) for idx, df in enumerate(tables)]
    text = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(text, "html.parser")
    return [_table(path, "html_text", path.stem, pd.DataFrame({"text": [soup.get_text(" ", strip=True)]}))]


def _read_text(path: Path) -> pd.DataFrame:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return pd.DataFrame({"line_number": range(1, len(text.splitlines()) + 1), "text": text.splitlines()})
