"""
Centralised logging for the whole pipeline
==========================================

Every entry point wraps its ``main()`` in :func:`run_logged`, which:

* configures a combined append log ``logs/pipeline.log`` plus a per-component log
  ``logs/<component>.log`` (timestamps, level, component, message);
* **tees stdout/stderr into the log**, so every existing ``print(...)`` (progress,
  counts, parameters) is captured on disk without changing the scripts;
* logs a START line (with argv), an END line with elapsed seconds, and any
  uncaught exception with full traceback.

The ``logs/`` folder is git-ignored. Nothing here changes program behaviour; it
only records what runs, when, and how it ends.
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

LOGS_DIR = Path(__file__).resolve().parents[1] / "logs"
_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def setup_logging(component: str, level: int = logging.INFO) -> logging.Logger:
    """Configure root logging to console + logs/pipeline.log + logs/<component>.log."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)
    # Avoid duplicate handlers if called more than once in one process.
    tags = {getattr(h, "_pipeline_tag", None) for h in root.handlers}
    formatter = logging.Formatter(_FMT)
    targets = {
        "console": logging.StreamHandler(sys.__stderr__),
        "pipeline": logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8"),
        f"component:{component}": logging.FileHandler(LOGS_DIR / f"{component}.log", encoding="utf-8"),
    }
    for tag, handler in targets.items():
        if tag in tags:
            continue
        handler.setFormatter(formatter)
        handler._pipeline_tag = tag  # type: ignore[attr-defined]
        root.addHandler(handler)
    return logging.getLogger(component)


class _Tee:
    """Duplicate a stream to the original console and the log file."""

    def __init__(self, original, file_handle):
        self._original = original
        self._file = file_handle

    def write(self, data):
        self._original.write(data)
        self._file.write(data)
        self._file.flush()

    def flush(self):
        self._original.flush()
        self._file.flush()


def run_logged(component: str, func, *args, **kwargs):
    """Run ``func`` with full logging of start/end/errors and tee'd stdout/stderr."""
    log = setup_logging(component)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    run_file = open(LOGS_DIR / f"{component}_{datetime.now():%Y%m%d_%H%M%S}.log", "w", encoding="utf-8")
    old_out, old_err = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = _Tee(old_out, run_file), _Tee(old_err, run_file)
    start = time.time()
    log.info("START %s | argv=%s", component, " ".join(sys.argv[1:]) or "(none)")
    try:
        result = func(*args, **kwargs)
        log.info("END %s | OK | %.1fs", component, time.time() - start)
        return result
    except SystemExit:
        raise
    except BaseException:
        log.exception("FAILED %s after %.1fs", component, time.time() - start)
        raise
    finally:
        sys.stdout, sys.stderr = old_out, old_err
        run_file.close()
