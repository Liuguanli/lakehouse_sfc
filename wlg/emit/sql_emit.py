"""Emit SQL workload files to a directory."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List


def write_sql_dir(
    directory: str | Path,
    queries: Iterable[Dict[str, object]],
    prefix: str = "q_",
) -> None:
    """
    Write queries to sequentially numbered ``.sql`` files.

    Parameters within the SQL text are assumed to use ``:name`` placeholders
    compatible with SparkSQL parameter binding conventions.
    """

    output_dir = Path(directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    for idx, entry in enumerate(queries, start=1):
        sql_text = entry.get("sql", "")
        filename = output_dir / f"{prefix}{idx:03d}.sql"
        with filename.open("w", encoding="utf-8") as handle:
            handle.write(str(sql_text))

