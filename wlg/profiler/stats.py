"""Column statistics and profiling logic."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from pandas.api import types as ptypes


@dataclass
class ColumnStats:
    """Summary statistics for a single column."""

    count: int
    nulls: int
    kind: str
    min: float | str | None
    max: float | str | None
    quantiles: Dict[float, float]
    hist: Tuple[List[int], List[float]] | None
    cardinality: int | None
    topk: List[Tuple[str, int]]

    def to_dict(self) -> Dict[str, object]:
        """Serialize the statistics into built-in Python types."""

        return {
            "count": int(self.count),
            "nulls": int(self.nulls),
            "kind": self.kind,
            "min": self.min,
            "max": self.max,
            "quantiles": {float(k): float(v) for k, v in self.quantiles.items()},
            "hist": (
                [int(c) for c in self.hist[0]],
                [float(edge) for edge in self.hist[1]],
            )
            if self.hist is not None
            else None,
            "cardinality": None if self.cardinality is None else int(self.cardinality),
            "topk": [(str(value), int(freq)) for value, freq in self.topk],
        }

    @classmethod
    def from_dict(cls, payload: MutableMapping[str, object]) -> "ColumnStats":
        """Rehydrate ``ColumnStats`` from serialized content."""

        quantiles = payload.get("quantiles", {})
        hist = payload.get("hist")
        topk = payload.get("topk", [])
        hist_tuple: Tuple[List[int], List[float]] | None
        if hist is None:
            hist_tuple = None
        else:
            counts, edges = hist
            hist_tuple = ([int(x) for x in counts], [float(x) for x in edges])
        return cls(
            count=int(payload.get("count", 0)),
            nulls=int(payload.get("nulls", 0)),
            kind=str(payload.get("kind", "unknown")),
            min=payload.get("min"),
            max=payload.get("max"),
            quantiles={float(k): float(v) for k, v in quantiles.items()},
            hist=hist_tuple,
            cardinality=(None if payload.get("cardinality") is None else int(payload["cardinality"])),
            topk=[(str(v), int(c)) for v, c in topk],
        )


@dataclass
class _ColumnState:
    """Incremental accumulator for column profiling."""

    kind: str = "unknown"
    count: int = 0
    nulls: int = 0
    min_value: float | str | None = None
    max_value: float | str | None = None
    numeric_samples: List[float] = field(default_factory=list)
    topk_counter: Counter[str] = field(default_factory=Counter)
    unique_tracker: set = field(default_factory=set)
    datetime_origin: str | None = None


class Profiler:
    """Incremental profiler that aggregates per-column statistics."""

    def __init__(
        self,
        num_bins: int = 64,
        qs: Sequence[float] = (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99),
        sample_cap: int = 20_000,
        topk_size: int = 50,
        corr_cap: int = 50_000,
        seed: Optional[int] = None,
    ) -> None:
        self.num_bins = num_bins
        self.qs = tuple(sorted(set(float(q) for q in qs)))
        self.sample_cap = sample_cap
        self.topk_size = topk_size
        self.corr_cap = corr_cap
        self._columns: Dict[str, _ColumnState] = {}
        self._corr_sample: Optional[pd.DataFrame] = None
        self._correlations: Dict[str, object] = {}

    @property
    def correlations(self) -> Dict[str, object]:
        """Return cached correlation metadata."""

        return self._correlations

    def update(self, frame: pd.DataFrame) -> None:
        """Ingest a ``DataFrame`` batch and update running statistics."""

        if frame.empty:
            return

        for column_name in frame.columns:
            series = frame[column_name]
            state = self._columns.setdefault(column_name, _ColumnState())
            non_null = series.dropna()

            # Kind inference performed only once.
            if state.kind == "unknown":
                state.kind = self._infer_kind(series)

            state.count += len(series)
            state.nulls += len(series) - len(non_null)

            if state.kind in {"numeric", "datetime"} and len(non_null) > 0:
                numeric = self._numeric_view(non_null, state.kind == "datetime")
                self._extend_samples(state.numeric_samples, numeric)
                min_value = float(np.nanmin(numeric))
                max_value = float(np.nanmax(numeric))
                state.min_value = (
                    min_value
                    if state.min_value is None
                    else float(min(min_value, state.min_value))
                )
                state.max_value = (
                    max_value
                    if state.max_value is None
                    else float(max(max_value, state.max_value))
                )
                self._extend_uniques(state.unique_tracker, numeric)
            elif state.kind in {"categorical", "boolean"} and len(non_null) > 0:
                values = non_null.astype("string")
                state.topk_counter.update(values)
                self._extend_uniques(state.unique_tracker, values)

        self._update_correlation_sample(frame)

    def finalize(self) -> Dict[str, ColumnStats]:
        """Compute final statistics from accumulated state."""

        stats: Dict[str, ColumnStats] = {}
        for name, state in self._columns.items():
            quantiles: Dict[float, float] = {}
            hist: Tuple[List[int], List[float]] | None = None
            cardinality: int | None = None
            topk: List[Tuple[str, int]] = []
            min_value = state.min_value
            max_value = state.max_value

            if state.kind in {"numeric", "datetime"} and state.numeric_samples:
                sample = np.asarray(state.numeric_samples, dtype=float)
                if sample.size > 0:
                    quantiles = {
                        q: float(np.quantile(sample, q, method="linear"))
                        for q in self.qs
                    }
                    if sample.size > 1:
                        bins = min(self.num_bins, max(1, int(np.sqrt(sample.size))))
                        counts, edges = np.histogram(sample, bins=bins)
                        hist = (counts.tolist(), edges.tolist())
                    cardinality = int(len(set(sample)))
            elif state.kind in {"categorical", "boolean"}:
                topk = state.topk_counter.most_common(self.topk_size)
                cardinality = int(len(state.unique_tracker))

            stats[name] = ColumnStats(
                count=state.count,
                nulls=state.nulls,
                kind=state.kind,
                min=min_value,
                max=max_value,
                quantiles=quantiles,
                hist=hist,
                cardinality=cardinality,
                topk=topk,
            )

        self._compute_correlations()
        return stats

    # ------------------------------------------------------------------ helpers
    def _infer_kind(self, series: pd.Series) -> str:
        """Map pandas dtypes into coarse-grained kinds."""

        if ptypes.is_datetime64_any_dtype(series):
            return "datetime"
        if ptypes.is_numeric_dtype(series) and not ptypes.is_bool_dtype(series):
            return "numeric"
        if ptypes.is_bool_dtype(series):
            return "boolean"
        return "categorical"

    def _numeric_view(self, series: pd.Series, is_datetime: bool) -> np.ndarray:
        """Return a numeric numpy view for numeric or datetime series."""

        if is_datetime:
            timestamps = pd.to_datetime(series, errors="coerce").dropna()
            values = timestamps.astype("int64").to_numpy(dtype=float, copy=True)
            return values / 1_000_000.0
        numeric_series = pd.to_numeric(series, errors="coerce").dropna()
        return numeric_series.to_numpy(dtype=float, copy=True)

    def _extend_samples(self, store: List[float], values: Iterable[float]) -> None:
        """Append values to a bounded sample store."""

        for value in values:
            if len(store) >= self.sample_cap:
                break
            store.append(float(value))

    def _extend_uniques(self, bucket: set, values: Iterable[object]) -> None:
        """Track unique values up to the configured cap."""

        for value in values:
            if len(bucket) >= self.sample_cap:
                break
            bucket.add(self._hashable_value(value))

    def _hashable_value(self, value: object) -> object:
        """Ensure that tracked unique values are hashable."""

        if isinstance(value, (list, tuple, set)):
            return tuple(value)
        return value

    def _update_correlation_sample(self, frame: pd.DataFrame) -> None:
        """Maintain a bounded sample for correlation analysis."""

        numeric_cols = [
            col
            for col, state in self._columns.items()
            if state.kind in {"numeric", "datetime"}
        ]
        if not numeric_cols or self.corr_cap <= 0:
            return
        slice_df = frame[numeric_cols].dropna()
        if slice_df.empty:
            return

        normalized = slice_df.copy()
        for col in numeric_cols:
            state = self._columns[col]
            if state.kind == "datetime":
                normalized[col] = (
                    pd.to_datetime(normalized[col], errors="coerce").astype("int64")
                    / 1_000_000.0
                )
        if self._corr_sample is None:
            take = min(len(normalized), self.corr_cap)
            self._corr_sample = normalized.iloc[:take].reset_index(drop=True)
            return

        remaining = self.corr_cap - len(self._corr_sample)
        if remaining <= 0:
            return
        take = min(len(normalized), remaining)
        if take <= 0:
            return
        append_df = normalized.iloc[:take]
        self._corr_sample = pd.concat(
            [self._corr_sample, append_df], ignore_index=True
        )

    def _compute_correlations(self) -> None:
        """Compute Pearson and Spearman correlations on the sample."""

        if self._corr_sample is None or self._corr_sample.empty:
            self._correlations = {}
            return
        pearson = self._corr_sample.corr(method="pearson").fillna(0.0)
        spearman = self._corr_sample.corr(method="spearman").fillna(0.0)

        top_pairs: List[Tuple[str, str, float]] = []
        for i, col_i in enumerate(pearson.columns):
            for j, col_j in enumerate(pearson.columns):
                if j <= i:
                    continue
                strength = (
                    abs(float(pearson.iloc[i, j])) + abs(float(spearman.iloc[i, j]))
                ) / 2.0
                top_pairs.append((col_i, col_j, strength))
        top_pairs.sort(key=lambda item: item[2], reverse=True)
        limited_pairs = [
            {"columns": [a, b], "score": score}
            for a, b, score in top_pairs[:20]
        ]
        self._correlations = {
            "pearson": pearson.to_dict(),
            "spearman": spearman.to_dict(),
            "top_pairs": limited_pairs,
        }
