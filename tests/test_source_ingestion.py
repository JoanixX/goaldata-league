import json
import zipfile
from pathlib import Path

import pandas as pd

from src.source_ingestion import read_source


def test_read_source_reads_directory_and_skips_audio_video(tmp_path: Path):
    pd.DataFrame([{"a": 1}]).to_csv(tmp_path / "sample.csv", index=False)
    (tmp_path / "skip.mp4").write_bytes(b"video")

    tables = read_source(tmp_path)

    assert len(tables) == 1
    assert tables[0].source_type == "csv"
    assert tables[0].data.loc[0, "a"] == 1


def test_read_source_reads_json_lists_and_zip_archives(tmp_path: Path):
    payload = tmp_path / "payload.json"
    payload.write_text(json.dumps({"matches": [{"id": 1}], "players": [{"id": 2}]}), encoding="utf-8")
    archive = tmp_path / "payload.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(payload, arcname="payload.json")

    tables = read_source(archive, extract_dir=tmp_path / "extract")

    assert {table.table_name for table in tables} == {"matches", "players"}
    assert {table.source_type for table in tables} == {"json"}
