# viz_results_toolkit.py
# -*- coding: utf-8 -*-
"""
Reusable plotting helpers for lakehouse query results.
- Pure Matplotlib (no seaborn)
- Return matplotlib.figure.Figure so caller controls display/saving
- Save PNG+PDF via savefig_multi()

Expected CSV columns (at least the metric you choose must exist):
    engine, query, bytesRead, elapsedTime_s, executorRunTime_s, executorCpuTime_s

We are forgiving about headers:
- If a CSV has a header, we read it.
- If header is missing, pass assume_headerless=True or let the loader auto-detect by row 0.
- If there are "junk header rows" repeated in the body, we drop them automatically.

Filename pattern support (engine/layout inference):
    results_<engine>_<layout>_*.csv
Example:
    results_hudi_zorder_tpch_16_Q1.csv -> engine=hudi, layout=zorder

Author: you :)
"""

from __future__ import annotations
import re
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple, Optional, Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# add near imports
from typing import Union
import matplotlib.axes as mpl_axes


# ------------------------------- IO helpers -------------------------------

DEFAULT_COLS = [
    "engine", "query", "bytesRead", "bytesRead_ev",
    "elapsedTime_s", "executorRunTime_s", "executorCpuTime_s"
]

FILENAME_RE = re.compile(r"results_(?P<engine>[^_]+)_(?P<layout>[^_]+)_.+\.csv$", re.IGNORECASE)


def _infer_engine_layout_from_name(csv_path: Path) -> Tuple[Optional[str], Optional[str]]:
    """
    Infer engine/layout from filename like: results_<engine>_<layout>_*.csv
    Return (engine, layout) or (None, None) if not matched.
    """
    m = FILENAME_RE.search(csv_path.name)
    if not m:
        return None, None
    return m.group("engine"), m.group("layout")


def _read_one_csv(
    csv_path: Path,
    assume_headerless: bool | None = None,
    names_if_headerless: Sequence[str] = DEFAULT_COLS,
) -> pd.DataFrame:
    """
    Read one CSV robustly:
    - If assume_headerless is True, read with provided column names.
    - If False, read normally with header.
    - If None, auto-detect:
        * peek first line; if it starts with 'engine,' treat as header
        * else treat as headerless
    - Drop duplicated header rows inside the data if present.
    """
    text = csv_path.read_text(errors="ignore")
    first_line = text.splitlines()[0].strip() if text.splitlines() else ""

    if assume_headerless is None:
        has_header = first_line.lower().startswith("engine,") or first_line.lower().startswith(",".join([c.lower() for c in DEFAULT_COLS[:3]]))
    else:
        has_header = not assume_headerless

    if has_header:
        df = pd.read_csv(csv_path)
    else:
        df = pd.read_csv(csv_path, header=None, names=list(names_if_headerless))

    # Drop repeated header rows accidentally included later
    hdr_lower = [c.lower() for c in df.columns]
    mask_junk = np.zeros(len(df), dtype=bool)
    for i, row in df.iterrows():
        # Heuristic: if row string-joins to something starting with 'engine,'
        try:
            s = ",".join(str(x) for x in row.values).strip().lower()
            if s.startswith("engine,"):
                mask_junk[i] = True
        except Exception:
            pass
    if mask_junk.any():
        df = df.loc[~mask_junk].reset_index(drop=True)

    # Ensure expected columns exist if we inferred headerless with fewer cols
    for c in DEFAULT_COLS:
        if c not in df.columns:
            df[c] = np.nan

    # Try to fill engine/layout from filename if missing
    if ("engine" in df.columns) and df["engine"].isna().all():
        eng, lay = _infer_engine_layout_from_name(csv_path)
        if eng:
            df["engine"] = eng
        if lay:
            if "layout" not in df.columns:
                df.insert(1, "layout", lay)
            else:
                df["layout"] = lay
    elif "layout" not in df.columns:
        # Create layout column if absent; try filename fallback
        _, lay = _infer_engine_layout_from_name(csv_path)
        df.insert(1, "layout", lay if lay else "baseline")

    return df


def load_results_dir(
    results_dir: Path | str,
    assume_headerless: bool | None = None,
) -> pd.DataFrame:
    """
    Recursively read all result CSVs under results_dir.
    Returns a concatenated DataFrame with at least:
        engine, layout, query, bytesRead, elapsedTime_s, executorRunTime_s, executorCpuTime_s
    """
    results_dir = Path(results_dir)
    csvs = sorted(results_dir.rglob("*.csv"))
    frames: List[pd.DataFrame] = []
    for p in csvs:
        try:
            frames.append(_read_one_csv(p, assume_headerless=assume_headerless))
        except Exception as ex:
            print(f"[load_results_dir] skip {p.name}: {ex}")
    if not frames:
        raise FileNotFoundError(f"No CSVs found under: {results_dir}")
    df = pd.concat(frames, ignore_index=True)

    # Coerce numeric columns when possible
    for col in ["bytesRead", "elapsedTime_s", "executorRunTime_s", "executorCpuTime_s"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure engine/layout columns exist
    if "engine" not in df.columns:
        df["engine"] = "unknown"
    if "layout" not in df.columns:
        df["layout"] = "baseline"

    # Basic clean-up: drop fully NA engine rows
    df = df[~df["engine"].isna()].reset_index(drop=True)
    return df


# ------------------------------- group helpers -------------------------------

def make_group_labels(df: pd.DataFrame, group_cols: Sequence[str] = ("engine", "layout")) -> Tuple[pd.DataFrame, List[str]]:
    """
    Combine columns in `group_cols` to a single `group` string and return (df2, order).
    """
    df2 = df.copy()
    df2["group"] = df2.apply(lambda r: ":".join(str(r[c]) for c in group_cols), axis=1)
    order = sorted(df2["group"].dropna().unique().tolist())
    return df2, order


# ------------------------------- plotting helpers -------------------------------

def _ensure_metric(df: pd.DataFrame, metric: str) -> pd.Series:
    if metric not in df.columns:
        raise KeyError(f"metric '{metric}' not in DataFrame columns: {list(df.columns)}")
    return pd.to_numeric(df[metric], errors="coerce")


def savefig_multi(fig: plt.Figure, stem: Path | str, fmts: Sequence[str] = ("png", "pdf"), dpi: int = 160) -> None:
    """
    Save figure to multiple formats without touching rcParams/colors.
    """
    stem = Path(stem)
    stem.parent.mkdir(parents=True, exist_ok=True)
    for f in fmts:
        fig.savefig(stem.with_suffix("." + f), dpi=dpi, bbox_inches="tight")


# def plot_violin(
#     df: pd.DataFrame,
#     metric: str,
#     group_cols: Sequence[str] = ("engine", "layout"),
#     title: Optional[str] = None,
#     ax: Optional[mpl_axes.Axes] = None,          # <- NEW
#     figsize: Tuple[int, int] = (9, 5),           # <- used only when ax is None
# ) -> plt.Figure:
#     """
#     Violin plots by group (engine:layout). Show distribution + mean (red dash).
#     If `ax` is provided, draw into that axes and return its parent figure.
#     """
#     metric_series = _ensure_metric(df, metric)
#     df2, order = make_group_labels(df, group_cols)

#     created_fig = False
#     if ax is None:
#         fig, ax = plt.subplots(figsize=figsize)
#         created_fig = True
#     else:
#         fig = ax.figure

#     data = [metric_series[df2["group"] == g].dropna().values for g in order]
#     parts = ax.violinplot(data, showmeans=False, showmedians=False, showextrema=False)

#     # Mean indicator for each violin
#     for i, vals in enumerate(data, start=1):
#         if len(vals) == 0:
#             continue
#         m = float(np.mean(vals))
#         ax.hlines(m, i - 0.3, i + 0.3, linewidth=2.5, color="red")

#     ax.set_xticks(range(1, len(order) + 1))
#     ax.set_xticklabels(order, rotation=25, ha="right")
#     ax.set_ylabel(metric)
#     ax.set_title(title or f"{metric}: distribution across ({', '.join(group_cols)})")
#     ax.grid(True, axis="y", linestyle="--", alpha=0.3)

#     if created_fig:
#         fig.tight_layout()
#     return fig


# def plot_box(
#     df: pd.DataFrame,
#     metric: str,
#     group_cols: Sequence[str] = ("engine", "layout"),
#     title: Optional[str] = None,
#     ax: Optional[mpl_axes.Axes] = None,          # <- NEW
#     figsize: Tuple[int, int] = (9, 5),           # <- used only when ax is None
# ) -> plt.Figure:
#     """
#     Box plots by group; overlay mean as black dot.
#     If `ax` is provided, draw into that axes and return its parent figure.
#     """
#     metric_series = _ensure_metric(df, metric)
#     df2, order = make_group_labels(df, group_cols)

#     created_fig = False
#     if ax is None:
#         fig, ax = plt.subplots(figsize=figsize)
#         created_fig = True
#     else:
#         fig = ax.figure

#     data = [metric_series[df2["group"] == g].dropna().values for g in order]
#     ax.boxplot(data, showfliers=False)

#     # mean dots
#     for i, vals in enumerate(data, start=1):
#         if len(vals) == 0:
#             continue
#         m = float(np.mean(vals))
#         ax.plot(i, m, marker="o")

#     ax.set_xticks(range(1, len(order) + 1))
#     ax.set_xticklabels(order, rotation=25, ha="right")
#     ax.set_ylabel(metric)
#     ax.set_title(title or f"{metric}: box plot across ({', '.join(group_cols)})")
#     ax.grid(True, axis="y", linestyle="--", alpha=0.3)

#     if created_fig:
#         fig.tight_layout()
#     return fig

def _prep_blocked_data(metric_series, df2, group_cols, block_col=None, blocks_order=None, block_gap=0.6):
    """
    Prepare grouped data for plotting with optional block separation (e.g., delta/hudi/iceberg).
    Returns:
        data: list of arrays (metric values for each group)
        xticklabels: labels for each group
        block_spans: (start, end) indices for each block
        positions: x positions for violin/boxplot
    """
    _, group_order = make_group_labels(df2, group_cols)

    if block_col is None:
        # Single block (default behavior)
        data = [metric_series[df2["group"] == g].dropna().values for g in group_order]
        positions = list(range(1, len(group_order) + 1))
        block_spans = [(1, len(group_order))]
        xticklabels = group_order
        return data, xticklabels, block_spans, positions

    # Multiple blocks
    blocks = list(df2[block_col].dropna().unique())
    if blocks_order:
        blocks = [b for b in blocks_order if b in blocks]

    data, xticklabels, positions, block_spans = [], [], [], []
    pos = 1
    for b in blocks:
        start = pos
        dfb = df2[df2[block_col] == b]
        # Keep same group order but only include groups existing in this block
        present_groups = [g for g in group_order if ((dfb["group"] == g).any())]
        for g in present_groups:
            vals = metric_series[(df2[block_col] == b) & (df2["group"] == g)].dropna().values
            data.append(vals)
            xticklabels.append(f"{g}")
            positions.append(pos)
            pos += 1
        end = pos - 1
        block_spans.append((start, end))
        # Add spacing between blocks
        pos += block_gap
    return data, xticklabels, block_spans, positions


def plot_violin(
    df: pd.DataFrame,
    metric: str,
    group_cols: Sequence[str] = ("engine", "layout"),
    title: Optional[str] = None,
    ax: Optional[mpl_axes.Axes] = None,
    figsize: Tuple[int, int] = (9, 5),
    block_col: Optional[str] = None,              # e.g., "format"
    blocks_order: Optional[Sequence[str]] = None, # e.g., ("delta","hudi","iceberg")
    block_gap: float = 0.6,                       # visual gap between blocks
) -> plt.Figure:
    """
    Violin plot by group (engine:layout) with optional block separation.
    If `block_col` is provided, visually splits the plot into multiple blocks
    (e.g., delta / hudi / iceberg) with dashed separators.
    """
    metric_series = _ensure_metric(df, metric)
    df2 = df.copy()
    df2, _ = make_group_labels(df2, group_cols)

    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True
    else:
        fig = ax.figure

    data, xticklabels, block_spans, positions = _prep_blocked_data(
        metric_series, df2, group_cols, block_col, blocks_order, block_gap
    )

    parts = ax.violinplot(data, positions=positions, showmeans=False, showmedians=False, showextrema=False)

    # Draw mean line for each violin
    for x, vals in zip(positions, data):
        if len(vals):
            m = float(np.mean(vals))
            ax.hlines(m, x - 0.3, x + 0.3, linewidth=2.5, color="red")

    ax.set_xticks(positions)
    ax.set_xticklabels(xticklabels, rotation=25, ha="right")
    ax.set_ylabel(metric)

    # Add block separators and labels
    if block_col is not None and len(block_spans) > 1:
        for (s, e) in block_spans[:-1]:
            ax.axvline(e + block_gap / 2, linestyle="--", alpha=0.4)
        if blocks_order:
            for (s, e), name in zip(block_spans, blocks_order[:len(block_spans)]):
                ax.text((s + e) / 2, ax.get_ylim()[1], name, ha="center", va="bottom", fontsize=10)

    ax.set_title(title or (
        f"{metric}: distribution across ({', '.join(group_cols)})"
        + (f" by {block_col}" if block_col else "")
    ), pad=16)
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)

    if created_fig:
        fig.tight_layout()
    return fig


def plot_box(
    df: pd.DataFrame,
    metric: str,
    group_cols: Sequence[str] = ("engine", "layout"),
    title: Optional[str] = None,
    ax: Optional[mpl_axes.Axes] = None,
    figsize: Tuple[int, int] = (9, 5),
    block_col: Optional[str] = None,
    blocks_order: Optional[Sequence[str]] = None,
    block_gap: float = 0.6,
) -> plt.Figure:
    """
    Box plot by group; overlay mean as black dot.
    If `block_col` is provided, visually splits the plot into multiple blocks
    (e.g., delta / hudi / iceberg) with dashed separators.
    """
    metric_series = _ensure_metric(df, metric)
    df2 = df.copy()
    df2, _ = make_group_labels(df2, group_cols)

    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True
    else:
        fig = ax.figure

    data, xticklabels, block_spans, positions = _prep_blocked_data(
        metric_series, df2, group_cols, block_col, blocks_order, block_gap
    )

    ax.boxplot(data, positions=positions, showfliers=False)

    # Draw mean dots
    for x, vals in zip(positions, data):
        if len(vals):
            ax.plot(x, float(np.mean(vals)), marker="o")

    ax.set_xticks(positions)
    ax.set_xticklabels(xticklabels, rotation=25, ha="right")
    ax.set_ylabel(metric)

    # Add block separators and labels
    if block_col is not None and len(block_spans) > 1:
        for (s, e) in block_spans[:-1]:
            ax.axvline(e + block_gap / 2, linestyle="--", alpha=0.4)
        if blocks_order:
            for (s, e), name in zip(block_spans, blocks_order[:len(block_spans)]):
                ax.text((s + e) / 2, ax.get_ylim()[1], name, ha="center", va="bottom", fontsize=10)

    ax.set_title(title or (
        f"{metric}: box plot across ({', '.join(group_cols)})"
        + (f" by {block_col}" if block_col else "")
    ), pad=16)
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)

    if created_fig:
        fig.tight_layout()
    return fig



def plot_strip(
    df: pd.DataFrame,
    metric: str,
    group_cols: Sequence[str] = ("engine", "layout"),
    jitter: float = 0.12,
    title: Optional[str] = None,
) -> plt.Figure:
    """
    Jittered scatter (strip) by group; shows every query sample.
    """
    metric_series = _ensure_metric(df, metric)
    df2, order = make_group_labels(df, group_cols)
    fig, ax = plt.subplots(figsize=(9, 5))

    for i, g in enumerate(order, start=1):
        vals = metric_series[df2["group"] == g].dropna().values
        if len(vals) == 0:
            continue
        x = i + (np.random.rand(len(vals)) - 0.5) * 2 * jitter
        ax.scatter(x, vals, s=10, alpha=0.7)

        # Mean line
        ax.hlines(float(np.mean(vals)), i - 0.3, i + 0.3, linewidth=2)

    ax.set_xticks(range(1, len(order) + 1))
    ax.set_xticklabels(order, rotation=25, ha="right")
    ax.set_ylabel(metric)
    ax.set_title(title or f"{metric}: per-query strip across ({', '.join(group_cols)})")
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


def plot_hist_overlaid(
    df: pd.DataFrame,
    metric: str,
    group_cols: Sequence[str] = ("engine", "layout"),
    bins: int = 30,
    density: bool = True,
    title: Optional[str] = None,
) -> plt.Figure:
    """
    Overlaid histograms (same global bins) to compare distributions across groups.
    """
    metric_series = _ensure_metric(df, metric)
    df2, order = make_group_labels(df, group_cols)

    # global bins across all data for fair comparison
    all_vals = metric_series.dropna().values
    if all_vals.size == 0:
        raise ValueError("No numeric data to plot.")
    vmin, vmax = np.min(all_vals), np.max(all_vals)
    if vmin == vmax:
        vmax = vmin + 1e-9
    edges = np.linspace(vmin, vmax, bins + 1)

    fig, ax = plt.subplots(figsize=(9, 5))

    for g in order:
        vals = metric_series[df2["group"] == g].dropna().values
        if vals.size == 0:
            continue
        # step-style histogram using same edges; weights/density handled by numpy.histogram
        h, _ = np.histogram(vals, bins=edges, density=density)
        centers = 0.5 * (edges[:-1] + edges[1:])
        width = (edges[1] - edges[0]) * 0.9
        ax.bar(centers, h, width=width, alpha=0.35, align="center", label=g, edgecolor="black", linewidth=0.5)

    ax.set_xlabel(metric)
    ax.set_ylabel("density" if density else "count")
    ax.set_title(title or f"{metric}: overlaid histograms across ({', '.join(group_cols)})")
    ax.legend(loc="best", frameon=False)
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


# ------------------------------- minimal demo -------------------------------

if __name__ == "__main__":
    # Minimal usage example: adjust `results_dir` and `out_dir` then run:
    #   python viz_results_toolkit.py
    results_dir = Path("results/tpch_16_Q1/20251022_142553")
    out_dir = Path("viz_out_results/demo")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load
    df = load_results_dir(results_dir, assume_headerless=None)

    # Choose any metric column you have in CSVs
    for metric in ["elapsedTime_s", "bytesRead", "executorRunTime_s", "executorCpuTime_s"]:
        if metric not in df.columns:
            continue

        fig = plot_violin(df, metric=metric, group_cols=("engine", "layout"))
        savefig_multi(fig, out_dir / f"violin_{metric}")

        fig = plot_box(df, metric=metric, group_cols=("engine", "layout"))
        savefig_multi(fig, out_dir / f"box_{metric}")

        fig = plot_strip(df, metric=metric, group_cols=("engine", "layout"))
        savefig_multi(fig, out_dir / f"strip_{metric}")

        fig = plot_hist_overlaid(df, metric=metric, group_cols=("engine", "layout"), bins=30, density=True)
        savefig_multi(fig, out_dir / f"hist_{metric}")

    print(f"[ok] Wrote figures to: {out_dir.resolve()}")
