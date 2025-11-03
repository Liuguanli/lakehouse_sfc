"""Correlation utilities for guided predicate sampling."""

from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

import pandas as pd
from pandas.api import types as ptypes

from wlg.datasource.base import DataSource


def build_sample(source: DataSource, cols: Sequence[str], cap: int = 50_000) -> pd.DataFrame:
    """Collect up to ``cap`` non-null rows for the requested columns."""

    if cap <= 0:
        return pd.DataFrame(columns=list(cols))

    collected: List[pd.DataFrame] = []
    remaining = cap
    for batch in source.scan_batches():
        if remaining <= 0:
            break
        if not set(cols).issubset(batch.columns):
            continue
        subset = batch[list(cols)].dropna()
        if subset.empty:
            continue
        normalized = _normalize(subset)
        take = min(len(normalized), remaining)
        collected.append(normalized.iloc[:take])
        remaining -= take
    if not collected:
        return pd.DataFrame(columns=list(cols))
    return pd.concat(collected, ignore_index=True)


def pair_corr(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute Pearson and Spearman correlation matrices."""

    if df.empty:
        columns = df.columns.tolist()
        return (
            pd.DataFrame(0.0, index=columns, columns=columns),
            pd.DataFrame(0.0, index=columns, columns=columns),
        )
    pear = df.corr(method="pearson").fillna(0.0)
    spear = df.corr(method="spearman").fillna(0.0)
    return pear, spear


def top_pairs(
    pear: pd.DataFrame,
    spear: pd.DataFrame,
    k: int = 10,
) -> List[Tuple[str, str, float]]:
    """Return the ``k`` strongest column pairs ranked by average absolute correlation."""

    if pear.empty or spear.empty:
        return []
    pairs: List[Tuple[str, str, float]] = []
    for i, col_i in enumerate(pear.columns):
        for j, col_j in enumerate(pear.columns):
            if j <= i:
                continue
            score = (
                abs(float(pear.iloc[i, j])) + abs(float(spear.iloc[i, j]))
            ) / 2.0
            pairs.append((col_i, col_j, score))
    pairs.sort(key=lambda item: item[2], reverse=True)
    return pairs[:k]


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to numeric epoch milliseconds for correlation."""

    normalized = df.copy()
    for column in normalized.columns:
        series = normalized[column]
        if ptypes.is_datetime64_any_dtype(series):
            normalized[column] = pd.to_datetime(series, errors="coerce").astype("int64") / 1_000_000.0
        elif ptypes.is_object_dtype(series):
            coerced = pd.to_numeric(series, errors="coerce")
            if coerced.notna().any():
                normalized[column] = coerced
    return normalized

