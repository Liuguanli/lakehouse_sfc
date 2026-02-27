from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd


PROJ_ROOT = Path(__file__).resolve().parents[1]
RESULTS_ROOT = PROJ_ROOT / "results" / "RQ7"
FIGURES_ROOT = PROJ_ROOT / "notebooks" / "figures"

LAYOUTS = ["no_layout"]
BATCHES = list(range(0, 11))
# Notebook/脚本都可直接改成多个 metric；脚本模式会逐个输出。
METRICS = ["elapsedTime_s"]
METRIC = METRICS[0]


def _parse_layout_from_filename(path: Path) -> str | None:
    m = re.search(r"results_hudi_(.+?)_spec_", path.name)
    return m.group(1) if m else None


def _parse_batch_from_parent(path: Path) -> int | None:
    m = re.match(r"batch_(\d+)$", path.parent.name)
    return int(m.group(1)) if m else None


def load_rq7_batch_metrics(
    results_root: Path = RESULTS_ROOT,
    layouts: Iterable[str] | None = None,
    metric: str = METRIC,
) -> pd.DataFrame:
    """Load all RQ7 batch CSVs and aggregate one point per (spec, layout, batch)."""
    layout_filter = {x.strip() for x in (layouts or []) if str(x).strip()}
    rows: list[dict] = []

    for csv_path in sorted(results_root.glob("*/batch_*/*.csv")):
        spec_name = csv_path.parent.parent.name
        batch = _parse_batch_from_parent(csv_path)
        layout = _parse_layout_from_filename(csv_path)
        if batch is None or layout is None:
            continue
        if layout_filter and layout not in layout_filter:
            continue

        df = pd.read_csv(csv_path)
        if metric not in df.columns:
            raise ValueError(f"Metric '{metric}' not found in {csv_path}")

        qmatch = re.search(r"(RQ7_Q\d+)", spec_name)
        query_family = qmatch.group(1) if qmatch else "RQ7"

        rows.append(
            {
                "spec": spec_name,
                "query_family": query_family,
                "layout": layout,
                "batch": batch,
                "n_runs": len(df),
                "metric_mean": float(df[metric].mean()),
                "metric_median": float(df[metric].median()),
                "metric_std": float(df[metric].std(ddof=0)),
                "csv_path": str(csv_path),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(["query_family", "spec", "layout", "batch"]).reset_index(drop=True)


def _spec_title(spec: str) -> str:
    return spec.replace("spec_tpch_", "")


def plot_rq7_per_query_lines(
    summary_df: pd.DataFrame,
    metric: str = METRIC,
    batches: Iterable[int] = BATCHES,
    out_root: Path = FIGURES_ROOT,
    save_individual: bool = False,
    save_grid: bool = False,
    close_fig: bool = False,
) -> tuple[plt.Figure, list[list[plt.Axes]], dict[str, Path]]:
    """Plot one line chart per query spec; supports multiple layouts as multiple lines."""
    if summary_df.empty:
        raise ValueError("summary_df is empty; no RQ7 batch CSVs found.")

    batches = list(batches)
    layouts = list(summary_df["layout"].dropna().unique())
    specs = list(summary_df["spec"].dropna().unique())
    n_specs = len(specs)
    ncols = 4
    nrows = math.ceil(n_specs / ncols)

    metric_tag = metric.replace("/", "_")
    per_query_dir = out_root / f"rq7_tpch_{metric_tag}_per_query"
    if save_individual:
        per_query_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(4.6 * ncols, 3.2 * nrows),
        constrained_layout=True,
        squeeze=False,
    )

    for i, spec in enumerate(specs):
        ax = axes[i // ncols][i % ncols]
        spec_df = summary_df[summary_df["spec"] == spec].copy()

        for layout in layouts:
            layout_df = spec_df[spec_df["layout"] == layout][["batch", "metric_mean"]].drop_duplicates()
            layout_df = layout_df.set_index("batch").reindex(batches)
            ax.plot(
                batches,
                layout_df["metric_mean"].values,
                marker="o",
                linewidth=1.8,
                markersize=4,
                label=layout,
            )

        ax.set_title(_spec_title(spec), fontsize=10)
        ax.set_xticks(batches)
        ax.grid(True, alpha=0.25)
        ax.set_xlabel("Batch")
        ax.set_ylabel(metric)

        if save_individual:
            single_fig, single_ax = plt.subplots(figsize=(7.4, 4.2), constrained_layout=True)
            for layout in layouts:
                layout_df = spec_df[spec_df["layout"] == layout][["batch", "metric_mean"]].drop_duplicates()
                layout_df = layout_df.set_index("batch").reindex(batches)
                single_ax.plot(
                    batches,
                    layout_df["metric_mean"].values,
                    marker="o",
                    linewidth=2.0,
                    markersize=4.5,
                    label=layout,
                )
            single_ax.set_title(f"{_spec_title(spec)} ({metric})")
            single_ax.set_xticks(batches)
            single_ax.set_xlabel("Batch (0 = baseline, 10 = full TPCH_4 inserts)")
            single_ax.set_ylabel(metric)
            single_ax.grid(True, alpha=0.3)
            if len(layouts) > 1:
                single_ax.legend(title="layout")
            single_out = per_query_dir / f"{spec}_{metric_tag}.png"
            single_fig.savefig(single_out, dpi=180, bbox_inches="tight")
            plt.close(single_fig)

    for j in range(n_specs, nrows * ncols):
        axes[j // ncols][j % ncols].axis("off")

    handles, labels = axes[0][0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=max(1, len(labels)), title="layout")
    fig.suptitle(f"RQ7 TPCH: {metric} vs batch (one subplot per query)", y=1.02, fontsize=14)

    outputs: dict[str, Path] = {}
    if save_grid:
        out_root.mkdir(parents=True, exist_ok=True)
        grid_png = out_root / f"rq7_tpch_{metric_tag}_by_batch_grid.png"
        grid_pdf = out_root / f"rq7_tpch_{metric_tag}_by_batch_grid.pdf"
        fig.savefig(grid_png, dpi=180, bbox_inches="tight")
        fig.savefig(grid_pdf, bbox_inches="tight")
        outputs["grid_png"] = grid_png
        outputs["grid_pdf"] = grid_pdf
    if save_individual:
        outputs["per_query_dir"] = per_query_dir
    if close_fig:
        plt.close(fig)

    return fig, axes.tolist(), outputs


def build_and_plot(
    layouts: Iterable[str] | None = None,
    metric: str = METRIC,
    results_root: Path = RESULTS_ROOT,
    out_root: Path = FIGURES_ROOT,
    save_summary_csv: bool = True,
    save_grid: bool = True,
    save_individual: bool = True,
    close_fig: bool = True,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    summary = load_rq7_batch_metrics(results_root=results_root, layouts=layouts, metric=metric)
    if summary.empty:
        raise RuntimeError(f"No RQ7 batch CSVs found under {results_root}")

    outputs: dict[str, Path] = {}
    if save_summary_csv:
        summary_csv = out_root / f"rq7_tpch_summary_{metric}.csv"
        out_root.mkdir(parents=True, exist_ok=True)
        summary.to_csv(summary_csv, index=False)
        outputs["summary_csv"] = summary_csv

    _, _, plot_outputs = plot_rq7_per_query_lines(
        summary_df=summary,
        metric=metric,
        out_root=out_root,
        save_individual=save_individual,
        save_grid=save_grid,
        close_fig=close_fig,
    )
    outputs.update(plot_outputs)
    return summary, outputs


def build_and_plot_many(
    layouts: Iterable[str] | None = None,
    metrics: Iterable[str] | None = None,
    results_root: Path = RESULTS_ROOT,
    out_root: Path = FIGURES_ROOT,
    save_summary_csv: bool = True,
    save_grid: bool = True,
    save_individual: bool = True,
    close_fig: bool = True,
) -> dict[str, tuple[pd.DataFrame, dict[str, Path]]]:
    """Run build_and_plot for multiple metrics and return a metric->(summary, outputs) map."""
    metric_list = [m for m in (metrics or METRICS) if str(m).strip()]
    if not metric_list:
        raise ValueError("No metrics provided.")

    results: dict[str, tuple[pd.DataFrame, dict[str, Path]]] = {}
    for metric in metric_list:
        results[metric] = build_and_plot(
            layouts=layouts,
            metric=metric,
            results_root=results_root,
            out_root=out_root,
            save_summary_csv=save_summary_csv,
            save_grid=save_grid,
            save_individual=save_individual,
            close_fig=close_fig,
        )
    return results


if __name__ == "__main__":
    all_results = build_and_plot_many(
        layouts=LAYOUTS,
        metrics=METRICS,
        save_summary_csv=True,
        save_grid=True,
        save_individual=True,
        close_fig=True,
    )
    for metric, (summary_df, output_paths) in all_results.items():
        print(f"[RQ7] metric={metric} summary rows: {len(summary_df)}")
        print("[RQ7] layouts:", sorted(summary_df["layout"].unique().tolist()))
        print("[RQ7] specs:", summary_df["spec"].nunique())
        for k, v in output_paths.items():
            print(f"[RQ7] {metric} {k}: {v}")
