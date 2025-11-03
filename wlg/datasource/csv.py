"""CSV-backed data source."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

from .base import DataSource


class CSVDataSource(DataSource):
    """Stream data from CSV files using chunked reads."""

    def __init__(
        self,
        path: str | Path,
        columns: Optional[Iterable[str]] = None,
        sample_rows: Optional[int] = None,
        batch_size: int = 262_144,
        **read_csv_kwargs: object,
    ) -> None:
        self._path = Path(path)
        self._columns = list(columns) if columns is not None else None
        self._sample_rows = sample_rows
        self._batch_size = batch_size
        self._read_csv_kwargs = read_csv_kwargs

    def schema(self) -> Dict[str, str]:
        """Infer schema via a single-row read."""

        df = pd.read_csv(self._path, nrows=1, **self._read_csv_kwargs)
        return {col: str(dtype) for col, dtype in df.dtypes.items()}

    def scan_batches(self) -> Iterable[pd.DataFrame]:
        """Yield chunked ``DataFrame`` objects."""

        kwargs = dict(self._read_csv_kwargs)
        if self._columns is not None:
            kwargs["usecols"] = self._columns
        iterator = pd.read_csv(
            self._path,
            chunksize=self._batch_size,
            iterator=True,
            **kwargs,
        )
        yielded = 0
        for frame in iterator:
            if self._sample_rows is None:
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
