#!/usr/bin/env python3
"""Generate TPCH grouped specs (RQ1 + RQ2 variants) without column-config suffix.

Adds GROUP BY / ORDER BY / LIMIT clauses to the templates. Output filenames default
to `spec_tpch_<RQ_LABEL>_<G|o|l tags>_<QUERY>_<SELECTIVITY>.yaml` (RQ_LABEL defaults to RQ4).
Example tag: G1o0l1000 => group on, order off, limit=1000.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from textwrap import dedent
from typing import Dict, Iterable, List, Tuple

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATS_PATH = ROOT / "workloads" / "stats" / "tpch_16_stats.yaml"
DEFAULT_OUTPUT_DIR = ROOT / "workload_spec" / "tpch_rq4"

# Toggle defaults for group/order/limit variants; override via CLI if needed.
DEFAULT_GROUP_BY_VARIANTS = ["on"]           # use --include-no-group to add "off"
DEFAULT_ORDER_VARIANTS = ["on", "off"]       # on/off
DEFAULT_LIMIT_VARIANTS = ["on", "off"]       # on/off
DEFAULT_LIMIT_VALUE = 1000                   # LIMIT n when enabled
ORDER_BY_METRIC = "cnt DESC"                 # applied when group_by=true and order_by=true
# Default selectivity filter; include S4 (range) and S0 (points). Override with --selectivity.
DEFAULT_SELECTIVITY_FILTER = ["S4", "S0"]


class LiteralStr(str):
    """Tell PyYAML to emit literal block scalars for SQL strings."""


def _literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(LiteralStr, _literal_representer)
yaml.SafeDumper.add_representer(LiteralStr, _literal_representer)


# Selectivity bands reused from RQ1 (can be filtered via CLI)
SELECTIVITY_BANDS: Dict[str, Tuple[float, float]] = {
    "S1": (0.0, 0.001),
    "S2": (0.001, 0.01),
    "S3": (0.01, 0.1),
    "S4": (0.1, 0.2),
}


# RQ1-style queries (range + point)
RQ1_QUERY_DEFS = [
    {"id": "Q1_N1_1", "kind": "range", "columns": ["l_shipdate"]},
    {"id": "Q1_N1_2", "kind": "range", "columns": ["l_receiptdate"]},
    {"id": "Q1_N1_3", "kind": "range", "columns": ["l_commitdate"]},
    {"id": "Q1_N1_4", "kind": "range", "columns": ["l_extendedprice"]},
    {"id": "Q1_N1_5", "kind": "range", "columns": ["l_quantity"]},
    {"id": "Q2_N2_1", "kind": "range", "columns": ["l_shipdate", "l_receiptdate"]},
    {"id": "Q2_N2_2", "kind": "range", "columns": ["l_commitdate", "l_suppkey"]},
    {"id": "Q2_N2_3", "kind": "range", "columns": ["l_extendedprice", "l_quantity"]},
    {"id": "Q2_N2_4", "kind": "range", "columns": ["l_extendedprice", "l_shipdate"]},
    {"id": "Q2_N2_5", "kind": "range", "columns": ["l_quantity", "l_receiptdate"]},
    *[
        {"id": f"Q3_K{k}_{idx}", "kind": "point", "columns": [column], "fanout": k}
        for k in [16]
        for idx, column in enumerate(
            ["l_shipdate", "l_commitdate", "l_orderkey", "l_suppkey", "l_quantity"], start=1
        )
    ],
]


# RQ2-style multi-column range + point
RQ2_QUERY_DEFS = [
    {"id": "Q1_N3_1", "kind": "range", "columns": ["l_shipdate", "l_receiptdate", "l_commitdate"]},
    {"id": "Q1_N3_2", "kind": "range", "columns": ["l_extendedprice", "l_quantity", "l_shipdate"]},
    {"id": "Q2_N4_1", "kind": "range", "columns": ["l_suppkey", "l_shipdate", "l_extendedprice", "l_quantity"]},
    {"id": "Q3_N5_1", "kind": "range", "columns": ["l_shipdate", "l_commitdate", "l_suppkey", "l_extendedprice", "l_quantity"]},
    *[
        {"id": f"Q4_K{k}_{idx}", "kind": "point", "columns": [column], "fanout": k}
        for k in [16]
        for idx, column in enumerate(
            ["l_shipdate", "l_commitdate", "l_orderkey", "l_suppkey", "l_quantity"], start=1
        )
    ],
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
    for column, meta in (data.get("columns") or {}).items():
        dtype = normalize(schema.get(column) or meta.get("kind"))
        bounds = [meta.get("min"), meta.get("max")]
        result[column] = {"type": dtype, "bounds": bounds}
    return result


def sql_literal(col_type: str, placeholder: str) -> str:
    return f"DATE ':{placeholder}'" if col_type == "date" else f":{placeholder}"


def build_range_template(
    query_id: str,
    columns: List[str],
    ratio: tuple[float, float],
    column_meta,
    order_by: bool,
    limit: int | None,
    group_by: bool,
):
    params = {}
    interval_rules = []
    conditions = []
    for col in columns:
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
            "ratio_range": [float(ratio[0]), float(ratio[1])],
            "clip_to_domain": True,
          }
        )
        conditions.append(f"{col} BETWEEN {sql_literal(dtype, lo_param)} AND {sql_literal(dtype, hi_param)}")

    group_cols = ", ".join(columns)
    if group_by:
        sql_lines = [
            f"SELECT {group_cols}, COUNT(*) AS cnt FROM {{tbl}}",
            f"WHERE {conditions[0]}",
        ]
        for cond in conditions[1:]:
            sql_lines.append(f"AND {cond}")
        sql_lines.append(f"GROUP BY {group_cols}")
        if order_by:
            sql_lines.append(f"ORDER BY {ORDER_BY_METRIC}")
    else:
        sql_lines = [
            f"SELECT {group_cols} FROM {{tbl}}",
            f"WHERE {conditions[0]}",
        ]
        for cond in conditions[1:]:
            sql_lines.append(f"AND {cond}")
        if order_by:
            sql_lines.append(f"ORDER BY {group_cols}")

    if limit:
        sql_lines.append(f"LIMIT {limit}")

    return {
        "id": query_id,
        "sql": LiteralStr("\n      ".join(sql_lines)),
        "params": params,
        "interval_rules": interval_rules,
    }


def build_point_template(
    query_id: str,
    column: str,
    fanout: int,
    column_meta,
    order_by: bool,
    limit: int | None,
    group_by: bool,
):
    dtype = column_meta[column]["type"]
    bounds = column_meta[column]["bounds"]
    params = {}
    placeholders = []
    seen = []
    for idx in range(fanout):
        name = f"{column}_v{idx+1}"
        entry = {"type": dtype}
        if all(bounds):
            entry["range"] = bounds
        if seen:
            entry["constraint"] = f"{name} not in {{{', '.join(seen)}}}"
        params[name] = entry
        seen.append(name)
        placeholders.append(sql_literal(dtype, name))

    if group_by:
        sql = dedent(
            f"""
            SELECT {column}, COUNT(*) AS cnt FROM {{tbl}}
            WHERE {column} IN ({', '.join(placeholders)})
            GROUP BY {column}
            {('ORDER BY ' + ORDER_BY_METRIC) if order_by else ''}
            {'LIMIT ' + str(limit) if limit else ''}
            """
        ).strip()
    else:
        sql = dedent(
            f"""
            SELECT {column} FROM {{tbl}}
            WHERE {column} IN ({', '.join(placeholders)})
            {'ORDER BY ' + column if order_by else ''}
            {'LIMIT ' + str(limit) if limit else ''}
            """
        ).strip()

    return {
        "id": query_id,
        "sql": LiteralStr(sql),
        "params": params,
    }


def build_spec(
    meta_query: dict,
    selectivity_label: str,
    column_meta: dict,
    limit: int | None,
    rq_label: str,
    ratio_range=None,
    group_by: bool = True,
    order_by: bool = True,
    tag: str = "",
):
    if meta_query["kind"] == "point":
        template = build_point_template(
            meta_query["id"], meta_query["columns"][0], meta_query["fanout"], column_meta, order_by, limit, group_by
        )
    else:
        assert ratio_range is not None
        template = build_range_template(
            meta_query["id"], meta_query["columns"], ratio_range, column_meta, order_by, limit, group_by
        )

    meta_block = {
        "rq": rq_label,
        "query": meta_query["id"],
        "group_by": meta_query["columns"] if group_by else [],
        "order_by": [ORDER_BY_METRIC] if order_by and group_by else (meta_query["columns"] if order_by else []),
        "limit": limit,
        "variant_tag": tag,
    }
    if ratio_range is not None:
        meta_block["selectivity"] = {
            "label": selectivity_label,
            "ratio_range": [float(ratio_range[0]), float(ratio_range[1])],
        }
    else:
        meta_block["selectivity"] = {"label": selectivity_label}

    return {
        "meta": meta_block,
        "generation": {"n": 10, "mode": "random", "seed": 42},
        "templates": [template],
    }


def emit_specs(
    query_defs: Iterable[dict],
    rq_label: str,
    selectivity_filter: set[str] | None,
    limit: int,
    order_variants: list[bool],
    limit_variants: list[bool],
    include_no_group: bool,
    column_meta: dict,
    output_dir: Path,
    overwrite: bool,
) -> int:
    total = 0
    for query in query_defs:
        if query["kind"] == "point":
            selectivity_items = [("S0", None)]
        else:
            selectivity_items = list(SELECTIVITY_BANDS.items())

        for sel_label, sel_ratio in selectivity_items:
            if selectivity_filter and sel_label not in selectivity_filter:
                continue

            group_opts = [True]
            if include_no_group:
                group_opts.append(False)

            for group_flag in group_opts:
                for order_flag in order_variants:
                    for limit_flag in limit_variants:
                        limit_val = limit if limit_flag else None
                        g_tag = "G1" if group_flag else "G0"
                        o_tag = "o1" if order_flag else "o0"
                        l_tag = f"l{limit_val}" if limit_val else "l0"
                        variant_tag = f"{g_tag}{o_tag}{l_tag}"

                        spec = build_spec(
                            query,
                            sel_label,
                            column_meta,
                            limit_val,
                            rq_label,
                            sel_ratio,
                            group_by=group_flag,
                            order_by=order_flag,
                            tag=variant_tag,
                        )
                        stem = f"spec_tpch_{rq_label}_{variant_tag}_{query['id']}_{sel_label}.yaml"
                        out_path = output_dir / stem
                        if out_path.exists() and not overwrite:
                            continue
                        out_path.write_text(yaml.safe_dump(spec, sort_keys=False))
                        total += 1
    return total


def main():
    parser = argparse.ArgumentParser(description="Generate grouped TPCH specs (RQ1+RQ2) without column-config suffix.")
    parser.add_argument("--stats", default=str(DEFAULT_STATS_PATH), help="Path to stats YAML (default: tpch_16).")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for specs.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT_VALUE, help=f"LIMIT clause value (default: {DEFAULT_LIMIT_VALUE}).")
    parser.add_argument("--rq-label", default="RQ4", help="RQ label used in meta and filename prefix (default: RQ4).")
    parser.add_argument(
        "--order-variants",
        default=",".join(DEFAULT_ORDER_VARIANTS),
        help="Comma list of order variants to emit: on/off",
    )
    parser.add_argument(
        "--limit-variants",
        default=",".join(DEFAULT_LIMIT_VARIANTS),
        help="Comma list of limit variants to emit: on/off",
    )
    parser.add_argument("--include-no-group", action="store_true", help="Also emit variants without GROUP BY")
    parser.add_argument("--no-overwrite", action="store_true", help="Skip files that already exist.")
    parser.add_argument("--rq", choices=["rq1", "rq2", "all"], default="all", help="Which query set to generate.")
    parser.add_argument("--selectivity", action="append", help="Selectivity labels to include (e.g., S1 S2).")
    args = parser.parse_args()

    stats_path = Path(args.stats)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    column_meta = load_column_stats(stats_path)

    sel_filter = set(args.selectivity) if args.selectivity else set(DEFAULT_SELECTIVITY_FILTER)
    overwrite = not args.no_overwrite
    limit = args.limit
    order_variants = [v.strip().lower() == "on" for v in args.order_variants.split(",") if v.strip()]
    limit_variants = [v.strip().lower() == "on" for v in args.limit_variants.split(",") if v.strip()]
    if not order_variants:
        order_variants = [v == "on" for v in DEFAULT_ORDER_VARIANTS]
    if not limit_variants:
        limit_variants = [v == "on" for v in DEFAULT_LIMIT_VARIANTS]

    total = 0
    if args.rq in ("rq1", "all"):
        total += emit_specs(
            RQ1_QUERY_DEFS,
            args.rq_label,
            sel_filter,
            limit,
            order_variants,
            limit_variants,
            args.include_no_group,
            column_meta,
            output_dir,
            overwrite,
        )
    if args.rq in ("rq2", "all"):
        total += emit_specs(
            RQ2_QUERY_DEFS,
            args.rq_label,
            sel_filter,
            limit,
            order_variants,
            limit_variants,
            args.include_no_group,
            column_meta,
            output_dir,
            overwrite,
        )

    print(f"[DONE] Generated {total} specs under {output_dir}")


if __name__ == "__main__":
    main()
