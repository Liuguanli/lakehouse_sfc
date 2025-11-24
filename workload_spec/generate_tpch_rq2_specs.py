#!/usr/bin/env python3
"""Generate spec_tpch_RQ2_* workload specs, with optional filters to narrow generation."""

from __future__ import annotations

import argparse
from pathlib import Path
from textwrap import dedent
from typing import Dict, List

import yaml


ROOT = Path(__file__).resolve().parents[1]
STATS_PATH = ROOT / "workloads" / "stats" / "tpch_16_stats.yaml"
OUTPUT_DIR = ROOT / "workload_spec" / "tpch_rq2"


class LiteralStr(str):
    """Tell PyYAML to emit literal block scalars for SQL strings."""


def _literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(LiteralStr, _literal_representer)
yaml.SafeDumper.add_representer(LiteralStr, _literal_representer)


# SELECTIVITY_BANDS = {
#     "S1": (0.0, 0.001),
#     "S2": (0.001, 0.01),
#     "S3": (0.01, 0.1),
#     "S4": (0.1, 0.2),
# }

SELECTIVITY_BANDS = {
    "S3": (0.01, 0.1),
    "S4": (0.1, 0.2),
}


COLUMN_CONFIGS = {
    "C1_N3_O1": ["l_shipdate", "l_receiptdate", "l_commitdate"],
    "C1_N3_O2": ["l_shipdate", "l_commitdate", "l_receiptdate"],
    "C1_N3_O3": ["l_commitdate", "l_shipdate", "l_receiptdate"],
    "C1_N3_O4": ["l_receiptdate", "l_shipdate", "l_commitdate"],
    "C1_N3_O5": ["l_receiptdate", "l_commitdate", "l_shipdate"],
    "C1_N3_O6": ["l_commitdate", "l_receiptdate", "l_shipdate"],
    "C2_N3_O1": ["l_extendedprice","l_quantity","l_shipdate"],
    "C2_N3_O2": ["l_quantity","l_extendedprice","l_shipdate"],
    "C2_N3_O3": ["l_extendedprice","l_shipdate","l_quantity"],
    "C2_N3_O4": ["l_quantity","l_shipdate","l_extendedprice"],
    "C2_N3_O5": ["l_extendedprice","l_shipdate","l_quantity"],
    "C2_N3_O6": ["l_quantity","l_shipdate","l_extendedprice"],
    "C3_N4_O1": ["l_suppkey","l_shipdate","l_extendedprice","l_quantity"],
    "C3_N4_O2": ["l_shipdate","l_suppkey","l_extendedprice","l_quantity"],
    "C4_N5_O1": ["l_shipdate","l_commitdate","l_suppkey","l_extendedprice","l_quantity"],
}


QUERY_DEFS = [
    {"id": "Q1_N3_1", "kind": "range", "columns": ["l_shipdate", "l_receiptdate", "l_commitdate"]},
    {"id": "Q1_N3_2", "kind": "range", "columns": ["l_extendedprice","l_quantity","l_shipdate"]},
    {"id": "Q2_N4_1", "kind": "range", "columns": ["l_suppkey","l_shipdate","l_extendedprice","l_quantity"]},
    {"id": "Q3_N5_1", "kind": "range", "columns": ["l_shipdate","l_commitdate","l_suppkey","l_extendedprice","l_quantity"]},
    # Point queries with varying fan-out (K=1,2,4,8,16) for the same column sets.
    *[
        {"id": f"Q4_K{k}_{idx}", "kind": "point", "columns": [column], "fanout": k}
        for k in (1, 4, 16)
        for idx, column in enumerate(
            ["l_shipdate", "l_commitdate", "l_orderkey", "l_suppkey", "l_quantity"], start=1
        )
    ],
]


def load_column_stats(path: Path) -> Dict[str, Dict[str, str]]:
    data = yaml.safe_load(path.read_text())
    result = {}
    for column, meta in data.get("columns", {}).items():
        kind = (meta.get("kind") or "").lower()
        if "date" in kind:
            dtype = "date"
        elif any(k in kind for k in ["int", "long"]):
            dtype = "int"
        elif any(k in kind for k in ["float", "double", "decimal"]):
            dtype = "number"
        else:
            dtype = kind or "string"
        bounds = [meta.get("min"), meta.get("max")]
        result[column] = {"type": dtype, "bounds": bounds}
    return result


def sql_literal(col_type: str, placeholder: str) -> str:
    return f"DATE ':{placeholder}'" if col_type == "date" else f":{placeholder}"


def build_range_template(query_id: str, columns: List[str], ratio: tuple[float, float], column_meta):
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
        conditions.append(
            f"{col} BETWEEN {sql_literal(dtype, lo_param)} AND {sql_literal(dtype, hi_param)}"
        )
    sql_lines = ["SELECT l_orderkey FROM {{tbl}}", f"WHERE {conditions[0]}"]
    for cond in conditions[1:]:
        sql_lines.append(f"AND {cond}")
    sql = "\n      ".join(sql_lines)
    return {
        "id": query_id,
        "sql": LiteralStr(dedent(sql)),
        "params": params,
        "interval_rules": interval_rules,
    }


def build_point_template(query_id: str, column: str, fanout: int, column_meta):
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
    sql = dedent(
        f"""
        SELECT l_orderkey FROM {{{{tbl}}}}
        WHERE {column} IN ({', '.join(placeholders)})
        """
    )
    return {
        "id": query_id,
        "sql": LiteralStr(sql.strip()),
        "params": params,
    }


def build_spec(
    meta_query: dict,
    selectivity_label: str,
    column_config: str,
    column_meta: dict,
    ratio_range: tuple[float, float] | None = None,
):
    if meta_query["kind"] == "point":
        template = build_point_template(
            meta_query["id"], meta_query["columns"][0], meta_query["fanout"], column_meta
        )
    else:
        assert ratio_range is not None
        template = build_range_template(meta_query["id"], meta_query["columns"], ratio_range, column_meta)

    meta_block = {
        "rq": "RQ2",
        "query": meta_query["id"],
        "column_config": {
            "label": column_config,
            "columns": COLUMN_CONFIGS[column_config],
        },
    }
    if ratio_range is not None:
        meta_block["selectivity"] = {
            "label": selectivity_label,
            "ratio_range": [float(ratio_range[0]), float(ratio_range[1])],
        }
    else:
        meta_block["selectivity"] = {
            "label": selectivity_label,
        }

    return {
        "meta": meta_block,
        "generation": {"n": 10, "mode": "random", "seed": 42},
        "templates": [template],
    }


def main(
    overwrite: bool = True,
    queries: list[str] | None = None,
    selectivities: list[str] | None = None,
    column_configs: list[str] | None = None,
):
    column_meta = load_column_stats(STATS_PATH)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    query_filter = set(queries) if queries else None
    selectivity_filter = set(selectivities) if selectivities else None
    column_config_filter = set(column_configs) if column_configs else None

    total = 0
    for query in QUERY_DEFS:
        if query_filter and query["id"] not in query_filter:
            continue

        if query["kind"] == "point":
            selectivity_items = [("S0", None)]
        else:
            selectivity_items = [(label, ratio) for label, ratio in SELECTIVITY_BANDS.items()]

        for sel_label, sel_ratio in selectivity_items:
            if selectivity_filter and sel_label not in selectivity_filter:
                continue

            for cfg_label in COLUMN_CONFIGS:
                if column_config_filter and cfg_label not in column_config_filter:
                    continue

                spec = build_spec(query, sel_label, cfg_label, column_meta, sel_ratio)
                stem = f"spec_tpch_RQ2_{query['id']}_{sel_label}_{cfg_label}.yaml"
                out_path = OUTPUT_DIR / stem
                if out_path.exists() and not overwrite:
                    continue
                out_path.write_text(yaml.safe_dump(spec, sort_keys=False))
                total += 1
    print(f"Generated {total} spec files under {OUTPUT_DIR}")


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
        help="limit generation to selectivity labels like S0, S1, ... (repeatable)",
    )
    parser.add_argument(
        "--column-config",
        action="append",
        dest="column_configs",
        default=None,
        help="limit generation to column config labels like C1_N2_O1 (repeatable)",
    )
    args = parser.parse_args()
    main(
        overwrite=not args.no_overwrite,
        queries=args.queries,
        selectivities=args.selectivities,
        column_configs=args.column_configs,
    )

# python workload_spec/generate_tpch_rq2_specs.py --no-overwrite
# python workload_spec/generate_tpch_rq2_specs.py --query Q3_K1_1 --selectivity S0 --column-config C1_N2_O1
# spec_tpch_RQ2_Q1_N1_4_S1_C1_N2_O1.yaml
# = PosixPath('workload_spec/tpch_rq2/spec_tpch_RQ2_Q3_K1_1_S0_C1_N2_O1.yaml') 

# python -m wlg.cli fill \
#   --spec workload_spec/tpch_rq2/spec_tpch_RQ2_Q3_K1_1_S0_C1_N2_O1.yaml \
#   --stats workloads/stats/tpch_16_stats.yaml \
#   --out workloads/yaml/tmp.yaml \
#   --sql-dir workloads/sql_tmp