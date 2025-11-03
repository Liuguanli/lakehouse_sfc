"""PostgreSQL-backed data source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import psycopg2
from psycopg2 import sql

from .base import DataSource


@dataclass
class _QualifiedTable:
    schema: str
    name: str


def _parse_table(identifier: str) -> _QualifiedTable:
    if "." in identifier:
        schema, name = identifier.split(".", 1)
    else:
        schema, name = "public", identifier
    return _QualifiedTable(schema=schema, name=name)


class PostgresDataSource(DataSource):
    """Stream data from PostgreSQL using server-side cursors."""

    def __init__(
        self,
        dsn: str,
        table: str,
        columns: Optional[Iterable[str]] = None,
        sample_rows: Optional[int] = None,
        batch_size: int = 262_144,
    ) -> None:
        self._dsn = dsn
        self._table = _parse_table(table)
        self._columns = list(columns) if columns is not None else None
        self._sample_rows = sample_rows
        self._batch_size = batch_size

    def schema(self) -> Dict[str, str]:
        """Read column metadata from ``information_schema``."""

        query = sql.SQL(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
        )
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (self._table.schema, self._table.name))
                rows = cursor.fetchall()
        return {row[0]: row[1] for row in rows}

    def scan_batches(self) -> Iterable[pd.DataFrame]:
        """Yield ``DataFrame`` batches fetched incrementally."""

        select_list = (
            sql.SQL(", ").join(sql.Identifier(col) for col in self._columns)
            if self._columns is not None
            else sql.SQL("*")
        )
        table_ident = sql.Identifier(self._table.schema, self._table.name)
        query = sql.SQL("SELECT {cols} FROM {table}").format(
            cols=select_list, table=table_ident
        )

        yielded = 0
        with psycopg2.connect(self._dsn) as conn:
            conn.autocommit = True
            with conn.cursor(name="wlg_cursor") as cursor:
                cursor.itersize = self._batch_size
                cursor.execute(query)
                colnames = [desc[0] for desc in cursor.description]
                while True:
                    rows = cursor.fetchmany(self._batch_size)
                    if not rows:
                        break
                    frame = pd.DataFrame(rows, columns=colnames)
                    if self._sample_rows is None:
                        yielded += len(frame)
                        yield frame
                        continue

                    remaining = self._sample_rows - yielded
                    if remaining <= 0:
                        break
                    if len(frame) > remaining:
                        yield frame.iloc[:remaining].copy()
                        yielded += remaining
                        break
                    yielded += len(frame)
                    yield frame

