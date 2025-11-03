"""JSONL-backed data source."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .base import DataSource


class JSONLDataSource(DataSource):
    """Stream newline-delimited JSON records into ``DataFrame`` batches."""

    def __init__(
        self,
        path: str | Path,
        columns: Optional[Iterable[str]] = None,
        sample_rows: Optional[int] = None,
        batch_size: int = 262_144,
    ) -> None:
        self._path = Path(path)
        self._columns = list(columns) if columns is not None else None
        self._sample_rows = sample_rows
        self._batch_size = batch_size

    def schema(self) -> Dict[str, str]:
        """Infer schema using a small sample of records."""

        rows: List[Dict[str, object]] = []
        with self._path.open("r", encoding="utf-8") as handle:
            for _ in range(100):
                line = handle.readline()
                if not line:
                    break
                rows.append(json.loads(line))
        if not rows:
            return {}
        df = pd.DataFrame(rows)
        return {col: str(dtype) for col, dtype in df.dtypes.items()}

    def scan_batches(self) -> Iterable[pd.DataFrame]:
        """Yield ``DataFrame`` batches assembled from JSON records."""

        buffer: List[Dict[str, object]] = []
        yielded = 0
        with self._path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if self._sample_rows is not None and yielded >= self._sample_rows:
                    break
                record = json.loads(line)
                if self._columns is not None:
                    record = {k: record.get(k) for k in self._columns}
                buffer.append(record)
                if len(buffer) >= self._batch_size:
                    frame = pd.DataFrame(buffer)
                    buffer.clear()
                    frame, consumed = self._prepare_frame(frame, yielded)
                    if consumed == 0 and frame.empty:
                        return
                    yielded += consumed
                    yield frame
                    if self._sample_rows is not None and yielded >= self._sample_rows:
                        return
            if buffer and (self._sample_rows is None or yielded < self._sample_rows):
                frame = pd.DataFrame(buffer)
                buffer.clear()
                frame, consumed = self._prepare_frame(frame, yielded)
                if consumed == 0 and frame.empty:
                    return
                yielded += consumed
                yield frame

    def _prepare_frame(
        self, frame: pd.DataFrame, yielded: int
    ) -> tuple[pd.DataFrame, int]:
        """Trim the frame when ``sample_rows`` is active."""

        if self._sample_rows is None:
            return frame, len(frame)

        remaining = self._sample_rows - yielded
        if remaining <= 0:
            return frame.iloc[0:0].copy(), 0
        if len(frame) > remaining:
            return frame.iloc[:remaining].copy(), remaining
        return frame, len(frame)
