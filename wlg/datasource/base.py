"""Abstract data source definitions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Iterable

import pandas as pd


class DataSource(ABC):
    """Common interface for streaming tabular data in batches."""

    @abstractmethod
    def schema(self) -> Dict[str, str]:
        """Return a column name to logical type mapping."""

    @abstractmethod
    def scan_batches(self) -> Iterable[pd.DataFrame]:
        """Yield successive ``DataFrame`` batches for profiling."""

