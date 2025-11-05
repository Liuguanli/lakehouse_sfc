"""Utility helpers for loading input datasets with basic format inference."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, Optional
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession

# Known suffix to Spark format mapping (lower-cased).
_SUFFIX_FORMAT = {
    ".csv": "csv",
    ".tsv": "csv",
    ".parquet": "parquet",
    ".json": "json",
}

# Default reader options applied per format; user options override these.
_FORMAT_DEFAULT_OPTIONS: Dict[str, Dict[str, str]] = {
    "csv": {
        "header": "true",
        "inferSchema": "true",
    },
}


def _iter_dir_samples(directory: Path) -> Iterable[Path]:
    """Yield a few representative files from a directory for suffix inference."""
    try:
        entries = sorted(directory.iterdir())
    except FileNotFoundError:
        return []

    emitted = 0
    for entry in entries:
        name = entry.name
        if name.startswith("_") or name.startswith("."):
            continue
        if entry.is_file():
            yield entry
            emitted += 1
        elif entry.is_dir():
            # Peek one level deeper to cope with partitioned directories.
            for nested in sorted(entry.iterdir()):
                nested_name = nested.name
                if nested_name.startswith("_") or nested_name.startswith("."):
                    continue
                if nested.is_file():
                    yield nested
                    emitted += 1
                    break
        if emitted >= 8:
            break


def _local_path(path: str) -> Optional[Path]:
    """Convert a possibly-URI path to a local Path if applicable."""
    parsed = urlparse(path)
    if parsed.scheme in ("", "file"):
        target = parsed.path if parsed.scheme == "file" else path
        return Path(os.path.expanduser(target))
    return None


def guess_input_format(path: str) -> str:
    """
    Guess the Spark reader format based on the provided path.
    Defaults to "parquet" if detection fails.
    """
    if not path:
        return "parquet"

    parsed = urlparse(path)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in _SUFFIX_FORMAT:
        return _SUFFIX_FORMAT[suffix]

    local = _local_path(path)
    if local:
        if local.is_file():
            suffix = local.suffix.lower()
            if suffix in _SUFFIX_FORMAT:
                return _SUFFIX_FORMAT[suffix]
        elif local.is_dir():
            for sample in _iter_dir_samples(local):
                suffix = sample.suffix.lower()
                if suffix in _SUFFIX_FORMAT:
                    return _SUFFIX_FORMAT[suffix]

    return "parquet"


def parse_kv_options(pairs: Optional[Iterable[str]]) -> Dict[str, str]:
    """Parse KEY=VALUE strings into a dictionary."""
    options: Dict[str, str] = {}
    if not pairs:
        return options
    for item in pairs:
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Expected KEY=VALUE for --input-option, got: {item}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Empty key in --input-option: {item}")
        options[key] = value.strip()
    return options


def load_input_df(
    spark: SparkSession,
    path: str,
    fmt: str = "auto",
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Load a DataFrame from ``path`` using Spark with optional format inference.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    path : str
        Input file or directory.
    fmt : str
        Spark format (csv/parquet/json/...), or "auto" to infer from path.
    options : dict
        Additional reader options. These override any defaults.
    """
    format_name = (fmt or "auto").lower()
    if format_name == "auto":
        format_name = guess_input_format(path)

    reader = spark.read.format(format_name)

    effective_options: Dict[str, str] = {}
    if format_name in _FORMAT_DEFAULT_OPTIONS:
        effective_options.update(_FORMAT_DEFAULT_OPTIONS[format_name])
    if options:
        effective_options.update(options)

    if effective_options:
        reader = reader.options(**effective_options)

    return reader.load(path)
