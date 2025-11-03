"""Distribution store utilities backed by histogram data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple

import numpy as np
import yaml

from .stats import ColumnStats


class UniDist:
    """Univariate inverse CDF constructed from histogram counts and edges."""

    def __init__(self, counts: Iterable[int], edges: Iterable[float]) -> None:
        self.counts = np.asarray(list(counts), dtype=float)
        self.edges = np.asarray(list(edges), dtype=float)
        if len(self.edges) != len(self.counts) + 1:
            raise ValueError("Histogram edges must have length counts + 1")
        self.total = self.counts.sum()
        if self.total <= 0:
            self.cdf = np.zeros_like(self.counts, dtype=float)
        else:
            self.cdf = np.cumsum(self.counts) / self.total

    def inv_cdf(self, p: float) -> float:
        """Return the value whose cumulative probability equals ``p``."""

        if not 0.0 <= p <= 1.0:
            raise ValueError("p must be within [0, 1]")
        if self.total <= 0:
            return float(self.edges[0])
        if p == 1.0:
            return float(self.edges[-1])
        idx = int(np.searchsorted(self.cdf, p, side="left"))
        idx = min(idx, len(self.counts) - 1)
        cdf_prev = 0.0 if idx == 0 else float(self.cdf[idx - 1])
        bin_prob = float(self.counts[idx] / self.total)
        if bin_prob <= 0.0:
            return float(self.edges[idx])
        fraction = (p - cdf_prev) / bin_prob
        lower = float(self.edges[idx])
        upper = float(self.edges[idx + 1])
        return lower + fraction * (upper - lower)


def build_uni_dists(stats: Mapping[str, ColumnStats]) -> Dict[str, UniDist]:
    """Construct ``UniDist`` models for columns that expose histogram data."""

    dists: Dict[str, UniDist] = {}
    for name, column in stats.items():
        if column.hist is None:
            continue
        counts, edges = column.hist
        if counts and edges:
            dists[name] = UniDist(counts, edges)
    return dists


def save_yaml(
    stats: Mapping[str, ColumnStats],
    path: str | Path,
    metadata: Optional[Mapping[str, object]] = None,
) -> None:
    """Persist column statistics and optional metadata to YAML."""

    payload: Dict[str, object] = {
        "columns": {name: stat.to_dict() for name, stat in stats.items()}
    }
    if metadata:
        payload["metadata"] = dict(metadata)
    with Path(path).open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=True)


def load_yaml(path: str | Path) -> Tuple[Dict[str, ColumnStats], Dict[str, object]]:
    """Read statistics and metadata from YAML."""

    with Path(path).open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    columns = payload.get("columns", {})
    stats = {
        name: ColumnStats.from_dict(data)
        for name, data in columns.items()
    }
    metadata = payload.get("metadata", {}) or {}
    return stats, metadata

