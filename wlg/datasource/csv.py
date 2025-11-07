"""CSV-backed data source."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional, List

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
        self._date_parse_explicit: List[str] = []
        self._date_parse_infer: bool = True

    def configure_date_parsing(self, explicit: List[str], infer: bool):
        """Store date parsing preferences provided by the CLI."""
        self._date_parse_explicit = explicit or []
        self._date_parse_infer = infer

    def _apply_date_parsing(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Parse/normalize date columns in-place based on config."""
        if frame.empty:
            return frame

        # Parse explicitly requested columns
        for col in self._date_parse_explicit:
            if col in frame.columns:
                parsed = pd.to_datetime(frame[col], errors="coerce")
                frame[col] = parsed.dt.normalize()

        # Optionally infer string/object columns that look like datetimes
        if self._date_parse_infer:
            for col in frame.columns:
                if pd.api.types.is_datetime64_any_dtype(frame[col]):
                    continue
                if not (pd.api.types.is_object_dtype(frame[col]) or pd.api.types.is_string_dtype(frame[col])):
                    continue
                parsed = pd.to_datetime(frame[col], errors="coerce", infer_datetime_format=True)
                if len(parsed) == 0:
                    continue
                # Require high success rate to avoid corrupting categorical columns
                if parsed.notna().mean() >= 0.90:
                    frame[col] = parsed.dt.normalize()

        return frame

    def schema(self) -> Dict[str, str]:
        """Infer schema via a single-row read."""

        df = pd.read_csv(self._path, nrows=min(self._batch_size, 1024), **self._read_csv_kwargs)
        df = self._apply_date_parsing(df)
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
            frame = self._apply_date_parsing(frame)

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
