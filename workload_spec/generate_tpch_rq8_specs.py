#!/usr/bin/env python3
"""Generate TPCH RQ8 specs for 2D range queries with selectivity + aspect-ratio controls."""

from __future__ import annotations

import argparse
import math
import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Tuple

import yaml


ROOT = Path(__file__).resolve().parents[1]
STATS_PATH = ROOT / "workloads" / "stats" / "tpch_16_stats.yaml"
OUTPUT_DIR = ROOT / "workload_spec" / "tpch_rq8"
DEFAULT_FILLED_YAML_DIR = ROOT / "workloads" / "rq8_plot" / "yaml"
DEFAULT_FILLED_SQL_DIR = ROOT / "workloads" / "rq8_plot" / "sql"


class LiteralStr(str):
    """Tell PyYAML to emit literal block scalars for SQL strings."""


def _literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(LiteralStr, _literal_representer)
yaml.SafeDumper.add_representer(LiteralStr, _literal_representer)

# Target 2D selectivity bands (area ratio over normalized domain).
SELECTIVITY_BANDS: Dict[str, Tuple[float, float]] = {
    "S1": (0.0001, 0.001),
    # "S2": (0.001, 0.01),
    # "S3": (0.01, 0.03),
    # "S4": (0.03, 0.05),
}

# Shape factors for (width(col1), width(col2)).
# The pair is normalized internally to keep factor1*factor2=1, so selectivity
# (area) is preserved while changing only shape.
ASPECT_RATIO_FACTORS: Dict[str, Tuple[float, float]] = {
    # A1_1 intentionally omitted: equivalent to RQ1-style symmetric windows.
    "A4_1": (2.0, 0.5),
    "A16_1": (4.0, 0.25),
    "A64_1": (8.0, 0.125),
    "A1_4": (0.5, 2.0),
    "A1_16": (0.25, 4.0),
    "A1_64": (0.125, 8.0),
}

# If both dimensions are date columns, enforce correlated windows instead of
# sampling two independent date ranges.
CORRELATE_DATE_DATE_WINDOWS = True


QUERY_DEFS = [
    # {"id": "Q8_N2_1", "columns": ["l_shipdate", "l_receiptdate"]},
    {"id": "Q8_N2_2", "columns": ["l_commitdate", "l_suppkey"]},
    # {"id": "Q8_N2_3", "columns": ["l_extendedprice", "l_quantity"]},
    {"id": "Q8_N2_4", "columns": ["l_extendedprice", "l_shipdate"]},
    # {"id": "Q8_N2_5", "columns": ["l_quantity", "l_receiptdate"]},
]


def load_column_stats(path: Path) -> Dict[str, Dict[str, str]]:
    data = yaml.safe_load(path.read_text())
    schema = {k: (v or "").lower() for k, v in (data.get("schema") or {}).items()}

    def normalize(raw: str) -> str:
        raw = (raw or "").lower()
        if "date" in raw:
            return "date"
        if any(k in raw for k in ["int", "long"]):
            return "int"
        if any(k in raw for k in ["float", "double", "decimal", "numeric"]):
            return "number"
        return raw or "string"

    result = {}
    for column, meta in data.get("columns", {}).items():
        dtype = normalize(schema.get(column) or meta.get("kind"))
        result[column] = {"type": dtype}
    return result


def sql_literal(col_type: str, placeholder: str) -> str:
    return f"DATE ':{placeholder}'" if col_type == "date" else f":{placeholder}"


def _normalize_shape_factors(shape_factors: tuple[float, float]) -> tuple[float, float]:
    f1, f2 = float(shape_factors[0]), float(shape_factors[1])
    if f1 <= 0.0 or f2 <= 0.0:
        raise ValueError("shape factors must be > 0")
    g = math.sqrt(f1 * f2)
    return f1 / g, f2 / g


def _project_axis_ratio_ranges(
    selectivity_range: tuple[float, float],
    aspect_shape_factors: tuple[float, float],
) -> tuple[tuple[float, float], tuple[float, float]]:
    s_lo, s_hi = sorted((float(selectivity_range[0]), float(selectivity_range[1])))
    f1, f2 = _normalize_shape_factors(aspect_shape_factors)

    if s_lo <= 0.0:
        raise ValueError("selectivity must be > 0")

    w1_vals = [min(1.0, math.sqrt(s) * f1) for s in (s_lo, s_hi)]
    w2_vals = [min(1.0, math.sqrt(s) * f2) for s in (s_lo, s_hi)]

    dim1 = (min(w1_vals), max(w1_vals))
    dim2 = (min(w2_vals), max(w2_vals))
    return dim1, dim2


def _effective_axis_ratio_ranges(
    columns: List[str],
    column_meta: dict,
    selectivity_range: tuple[float, float],
    aspect_shape_factors: tuple[float, float],
) -> tuple[tuple[float, float], tuple[float, float], bool]:
    dim1_ratio, dim2_ratio = _project_axis_ratio_ranges(selectivity_range, aspect_shape_factors)
    both_date = all(column_meta[col]["type"] == "date" for col in columns)
    if both_date and CORRELATE_DATE_DATE_WINDOWS:
        dim2_ratio = dim1_ratio
        return dim1_ratio, dim2_ratio, True
    return dim1_ratio, dim2_ratio, False


def build_range_template(
    query_id: str,
    columns: List[str],
    selectivity_range: tuple[float, float],
    aspect_shape_factors: tuple[float, float],
    column_meta,
):
    if len(columns) != 2:
        raise ValueError(f"RQ8 expects exactly 2 columns, got {columns}")

    dim1_ratio, dim2_ratio, date_corr_applied = _effective_axis_ratio_ranges(
        columns, column_meta, selectivity_range, aspect_shape_factors
    )
    norm_f1, norm_f2 = _normalize_shape_factors(aspect_shape_factors)
    aspect_ratio = norm_f1 / norm_f2

    params = {}
    interval_rules = []
    conditions = []

    for col, ratio_range in zip(columns, [dim1_ratio, dim2_ratio]):
        dtype = column_meta[col]["type"]
        lo_param = f"{col}_lo"
        hi_param = f"{col}_hi"
        params[lo_param] = {"type": dtype}
        params[hi_param] = {"type": dtype, "constraint": f"{hi_param} >= {lo_param}"}
        interval_rules.append(
            {
                "column": col,
                "lo": lo_param,
                "hi": hi_param,
                "type": dtype,
                "ratio_range": [float(ratio_range[0]), float(ratio_range[1])],
                "clip_to_domain": True,
            }
        )
        conditions.append(
            f"{col} BETWEEN {sql_literal(dtype, lo_param)} AND {sql_literal(dtype, hi_param)}"
        )

    if date_corr_applied:
        # Tie the 2nd date interval to the 1st one, so two date dimensions are
        # sampled as one correlated window instead of two independent windows.
        interval_rules[1]["align_with"] = {
            "lo": interval_rules[0]["lo"],
            "hi": interval_rules[0]["hi"],
        }
        interval_rules[1]["lag_days"] = [0, 0]

    sql = dedent(
        f"""
        SELECT l_orderkey FROM {{{{tbl}}}}
        WHERE {conditions[0]}
        AND {conditions[1]}
        """
    )
    return {
        "id": query_id,
        "sql": LiteralStr(sql.strip()),
        "params": params,
        "interval_rules": interval_rules,
        "rq8_notes": {
            "target_selectivity_area_range": [
                float(selectivity_range[0]),
                float(selectivity_range[1]),
            ],
            "target_aspect_ratio_range": [
                float(aspect_ratio),
                float(aspect_ratio),
            ],
            "target_aspect_shape_factors": [
                float(aspect_shape_factors[0]),
                float(aspect_shape_factors[1]),
            ],
            "normalized_aspect_shape_factors": [
                float(norm_f1),
                float(norm_f2),
            ],
            "effective_axis_ratio_ranges": {
                columns[0]: [float(dim1_ratio[0]), float(dim1_ratio[1])],
                columns[1]: [float(dim2_ratio[0]), float(dim2_ratio[1])],
            },
            "date_date_correlation_applied": bool(date_corr_applied),
        },
    }


def build_spec(
    meta_query: dict,
    selectivity_label: str,
    selectivity_range: tuple[float, float],
    aspect_ratio_label: str,
    aspect_shape_factors: tuple[float, float],
    column_meta: dict,
):
    norm_f1, norm_f2 = _normalize_shape_factors(aspect_shape_factors)
    aspect_ratio = norm_f1 / norm_f2
    dim1_ratio, dim2_ratio, date_corr_applied = _effective_axis_ratio_ranges(
        meta_query["columns"], column_meta, selectivity_range, aspect_shape_factors
    )
    template = build_range_template(
        meta_query["id"],
        meta_query["columns"],
        selectivity_range,
        aspect_shape_factors,
        column_meta,
    )

    return {
        "meta": {
            "rq": "RQ8",
            "query": meta_query["id"],
            "query_columns": meta_query["columns"],
            "selectivity": {
                "label": selectivity_label,
                "area_ratio_range": [float(selectivity_range[0]), float(selectivity_range[1])],
            },
            "aspect_ratio": {
                "label": aspect_ratio_label,
                "ratio_range": [float(aspect_ratio), float(aspect_ratio)],
                "shape_factors": [
                    float(aspect_shape_factors[0]),
                    float(aspect_shape_factors[1]),
                ],
                "normalized_shape_factors": [float(norm_f1), float(norm_f2)],
            },
            "axis_ratio_ranges": {
                meta_query["columns"][0]: [float(dim1_ratio[0]), float(dim1_ratio[1])],
                meta_query["columns"][1]: [float(dim2_ratio[0]), float(dim2_ratio[1])],
            },
            "date_date_correlation_applied": bool(date_corr_applied),
        },
        "generation": {"n": 10, "mode": "random", "seed": 42},
        "templates": [template],
    }


def fill_specs(
    specs: list[Path],
    stats_path: Path,
    yaml_out_dir: Path,
    sql_out_dir: Path,
    force: bool = False,
):
    yaml_out_dir.mkdir(parents=True, exist_ok=True)
    sql_out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    skipped = 0

    for spec_path in specs:
        stem = spec_path.stem
        out_yaml = yaml_out_dir / f"{stem}.yaml"
        out_sql = sql_out_dir / stem
        has_sql = out_sql.exists() and any(out_sql.glob("*.sql"))

        if (not force) and out_yaml.exists() and has_sql:
            skipped += 1
            continue

        out_sql.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            "-m",
            "wlg.cli",
            "fill",
            "--spec",
            str(spec_path),
            "--stats",
            str(stats_path),
            "--out",
            str(out_yaml),
            "--sql-dir",
            str(out_sql),
        ]
        subprocess.run(cmd, check=True)
        total += 1

    print(
        f"Filled {total} specs into workloads under {yaml_out_dir} and {sql_out_dir} "
        f"(skipped {skipped} existing)"
    )


def main(
    overwrite: bool = True,
    queries: list[str] | None = None,
    selectivities: list[str] | None = None,
    aspect_ratios: list[str] | None = None,
    fill: bool = False,
    fill_force: bool = False,
    fill_yaml_dir: Path = DEFAULT_FILLED_YAML_DIR,
    fill_sql_dir: Path = DEFAULT_FILLED_SQL_DIR,
):
    column_meta = load_column_stats(STATS_PATH)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    query_filter = set(queries) if queries else None
    selectivity_filter = set(selectivities) if selectivities else None
    aspect_ratio_filter = set(aspect_ratios) if aspect_ratios else None

    total = 0
    selected_paths: list[Path] = []
    for query in QUERY_DEFS:
        if query_filter and query["id"] not in query_filter:
            continue

        for sel_label, sel_range in SELECTIVITY_BANDS.items():
            if selectivity_filter and sel_label not in selectivity_filter:
                continue

            for ar_label, ar_factors in ASPECT_RATIO_FACTORS.items():
                if aspect_ratio_filter and ar_label not in aspect_ratio_filter:
                    continue

                spec = build_spec(
                    meta_query=query,
                    selectivity_label=sel_label,
                    selectivity_range=sel_range,
                    aspect_ratio_label=ar_label,
                    aspect_shape_factors=ar_factors,
                    column_meta=column_meta,
                )
                stem = f"spec_tpch_RQ8_{query['id']}_{sel_label}_{ar_label}.yaml"
                out_path = OUTPUT_DIR / stem
                selected_paths.append(out_path)
                if out_path.exists() and not overwrite:
                    continue
                out_path.write_text(yaml.safe_dump(spec, sort_keys=False))
                total += 1

    print(f"Generated {total} spec files under {OUTPUT_DIR}")
    if fill:
        dedup_paths = list(dict.fromkeys(selected_paths))
        existing_specs = [p for p in dedup_paths if p.exists()]
        fill_specs(
            specs=existing_specs,
            stats_path=STATS_PATH,
            yaml_out_dir=fill_yaml_dir,
            sql_out_dir=fill_sql_dir,
            force=fill_force,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="skip writing specs that already exist",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        default=None,
        help="limit generation to specific query ids (repeatable)",
    )
    parser.add_argument(
        "--selectivity",
        action="append",
        dest="selectivities",
        default=None,
        help="limit generation to selectivity labels like S1, S2, ... (repeatable)",
    )
    parser.add_argument(
        "--aspect-ratio",
        action="append",
        dest="aspect_ratios",
        default=None,
        help="limit generation to aspect ratio labels like A4_1, A16_1, A1_4 (repeatable)",
    )
    parser.add_argument(
        "--column-config",
        action="append",
        dest="column_configs",
        default=None,
        help="deprecated for RQ8; accepted for backward compatibility and ignored",
    )
    parser.add_argument(
        "--fill",
        action="store_true",
        help="after generating specs, also run `wlg.cli fill` for selected specs",
    )
    parser.add_argument(
        "--fill-force",
        action="store_true",
        help="when used with --fill, regenerate workloads even if yaml/sql already exist",
    )
    parser.add_argument(
        "--fill-yaml-dir",
        default=str(DEFAULT_FILLED_YAML_DIR),
        help="output directory for filled workload yaml files",
    )
    parser.add_argument(
        "--fill-sql-dir",
        default=str(DEFAULT_FILLED_SQL_DIR),
        help="output directory root for filled sql directories",
    )
    args = parser.parse_args()
    if args.column_configs:
        print("[warn] --column-config is deprecated in RQ8 and will be ignored.")
    main(
        overwrite=not args.no_overwrite,
        queries=args.queries,
        selectivities=args.selectivities,
        aspect_ratios=args.aspect_ratios,
        fill=args.fill,
        fill_force=args.fill_force,
        fill_yaml_dir=Path(args.fill_yaml_dir),
        fill_sql_dir=Path(args.fill_sql_dir),
    )

# python workload_spec/generate_tpch_rq8_specs.py --no-overwrite
# python workload_spec/generate_tpch_rq8_specs.py --selectivity S1 --fill
# python workload_spec/generate_tpch_rq8_specs.py --query Q8_N2_4 --selectivity S1 --aspect-ratio A4_1 --fill --fill-force
