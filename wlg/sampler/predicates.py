"""Predicate sampling helpers backed by inverse CDFs."""

from __future__ import annotations

import math
from typing import Iterable, List, Sequence, Tuple

from wlg.profiler.dist_store import UniDist


def sample_between(
    dist: UniDist,
    target_sel: float,
    rng,
) -> Tuple[float, float]:
    """Sample a ``BETWEEN`` predicate that matches the target selectivity."""

    span = max(0.0, min(1.0, target_sel))
    start = rng.uniform(0.0, max(0.0, 1.0 - span))
    end = min(1.0, start + span)
    lo = dist.inv_cdf(start)
    hi = dist.inv_cdf(end)
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


def sample_eq_from_topk(
    topk: Sequence[Tuple[str, int]],
    rng,
) -> str:
    """Sample an equality predicate value using Top-K frequencies."""

    if not topk:
        raise ValueError("Top-K list may not be empty for equality sampling")
    total = sum(freq for _, freq in topk)
    if total <= 0:
        return topk[0][0]
    needle = rng.uniform(0, total)
    cumulative = 0.0
    for value, freq in topk:
        cumulative += freq
        if needle <= cumulative:
            return value
    return topk[-1][0]


def sample_copula(
    dists: Sequence[UniDist],
    target_sel: float,
    rho: float = 0.4,
    rng=None,
) -> List[Tuple[float, float]]:
    """Sample multi-dimensional ranges using a simplified Gaussian copula."""

    if rng is None:
        raise ValueError("A random number generator must be provided")
    dimension = len(dists)
    if dimension == 0:
        return []
    rho = max(-0.99, min(0.99, float(rho)))
    cov = [[rho if i != j else 1.0 for j in range(dimension)] for i in range(dimension)]
    # Cholesky decomposition
    L = _cholesky(cov)
    z = [rng.gauss(0.0, 1.0) for _ in range(dimension)]
    correlated = [sum(L_row[k] * z[k] for k in range(dimension)) for L_row in L]
    uniforms = [_normal_cdf(value) for value in correlated]

    marginal_sel = max(1e-6, min(1.0, target_sel) ** (1.0 / dimension))
    half = min(0.5, marginal_sel / 2.0)

    ranges: List[Tuple[float, float]] = []
    for dist, u in zip(dists, uniforms):
        lo_p = max(0.0, u - half)
        hi_p = min(1.0, u + half)
        lo = dist.inv_cdf(lo_p)
        hi = dist.inv_cdf(hi_p)
        if lo > hi:
            lo, hi = hi, lo
        ranges.append((lo, hi))
    return ranges


def _normal_cdf(x: float) -> float:
    """Standard normal cumulative distribution."""

    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _cholesky(matrix: Sequence[Sequence[float]]) -> List[List[float]]:
    """Compute a basic Cholesky factorisation for symmetric positive matrices."""

    n = len(matrix)
    L = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1):
            s = sum(L[i][k] * L[j][k] for k in range(j))
            if i == j:
                value = matrix[i][i] - s
                value = value if value > 0 else 1e-9
                L[i][j] = math.sqrt(value)
            else:
                if L[j][j] == 0:
                    L[i][j] = 0.0
                else:
                    L[i][j] = (matrix[i][j] - s) / L[j][j]
    return L

