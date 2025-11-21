#!/usr/bin/env python3
"""Generate amazon RQ1 workload specs, similar to TPCH generator."""

from __future__ import annotations

import argparse
from pathlib import Path
from textwrap import dedent
from typing import Dict, List

import yaml


ROOT = Path(__file__).resolve().parents[1]
STATS_PATH = ROOT / "workloads" / "stats" / "amazon_stats.yaml"
OUTPUT_DIR = ROOT / "workload_spec" / "amazon_rq1"


class LiteralStr(str):
    pass


def _literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(LiteralStr, _literal_representer)
yaml.SafeDumper.add_representer(LiteralStr, _literal_representer)


SELECTIVITY_BANDS = {
    "S1": (0.0, 0.001),
    "S2": (0.001, 0.01),
    "S3": (0.01, 0.1),
    "S4": (0.1, 0.2),
}

COLUMN_CONFIGS = {
    "C1": ["asin", "parent_asin"],
    "C2": ["parent_asin", "asin"],
    "C3": ["asin", "user_id"],
    "C4": ["user_id", "asin"],
    "C5": ["user_id", "record_timestamp"],
    "C6": ["record_timestamp", "user_id"],
}

QUERY_DEFS = [
    {"id": "Q1_N1_1", "kind": "range", "columns": ["asin"]},
    {"id": "Q1_N1_2", "kind": "range", "columns": ["user_id"]},
    {"id": "Q1_N1_3", "kind": "range", "columns": ["record_timestamp"]},
    {"id": "Q1_N1_4", "kind": "range", "columns": ["rating"]},
    {"id": "Q2_N2_1", "kind": "range", "columns": ["asin", "parent_asin"]},
    {"id": "Q2_N2_2", "kind": "range", "columns": ["asin", "user_id"]},
    {"id": "Q2_N2_3", "kind": "range", "columns": ["user_id", "record_timestamp"]},
    {"id": "Q2_N2_4", "kind": "range", "columns": ["record_timestamp", "rating"]},
]

for fanout in (1, 2, 4, 8, 16):
    for col, idx in zip(["asin", "user_id", "record_timestamp"], range(1, 4)):
        QUERY_DEFS.append(
            {"id": f"Q3_K{fanout}_{idx}", "kind": "point", "columns": [col], "fanout": fanout}
        )


def load_column_stats(path: Path) -> Dict[str, Dict[str, str]]:
    data = yaml.safe_load(path.read_text())
    result = {}
    for column, meta in data.get("columns", {}).items():
        kind = (meta.get("kind") or "").lower()
        if "date" in kind or "timestamp" in kind:
            dtype = "date" if "date" in kind else "number"
        elif "int" in kind:
            dtype = "int"
        elif "float" in kind or "double" in kind or "decimal" in kind:
            dtype = "number"
        else:
            dtype = kind or "string"
        result[column] = {
            "type": dtype,
            "bounds": [meta.get("min"), meta.get("max")],
        }
    return result


def sql_literal(col_type: str, placeholder: str) -> str:
    return f"DATE ':{placeholder}'" if col_type == "date" else f":{placeholder}"


def build_range_template(query_id: str, columns: List[str], ratio, column_meta):
    params = {}
    interval_rules = []
    conditions = []
    for col in columns:
        dtype = column_meta[col]["type"]
        lo = f"{col}_lo"
        hi = f"{col}_hi"
        params[lo] = {"type": dtype}
        params[hi] = {"type": dtype, "constraint": f"{hi} >= {lo}"}
        interval_rules.append(
            {
                "column": col,
                "lo": lo,
                "hi": hi,
                "type": dtype,
                "ratio_range": [float(ratio[0]), float(ratio[1])],
                "clip_to_domain": True,
            }
        )
        conditions.append(f"{col} BETWEEN {sql_literal(dtype, lo)} AND {sql_literal(dtype, hi)}")
    sql_lines = ["SELECT user_id FROM {{tbl}}", f"WHERE {conditions[0]}"]
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
        SELECT user_id FROM {{tbl}}
        WHERE {column} IN ({', '.join(placeholders)})
        """
    )
    return {
        "id": query_id,
        "sql": LiteralStr(sql.strip()),
        "params": params,
    }


def build_spec(meta_query, selectivity_label, column_config, column_meta, ratio_range=None):
    if meta_query["kind"] == "point":
        template = build_point_template(
            meta_query["id"], meta_query["columns"][0], meta_query["fanout"], column_meta
        )
    else:
        template = build_range_template(meta_query["id"], meta_query["columns"], ratio_range, column_meta)

    meta = {
        "rq": "RQ1",
        "query": meta_query["id"],
        "column_config": {
            "label": column_config,
            "columns": COLUMN_CONFIGS[column_config],
        },
    }
    if ratio_range is not None:
        meta["selectivity"] = {"label": selectivity_label, "ratio_range": list(map(float, ratio_range))}
    else:
        meta["selectivity"] = {"label": selectivity_label}

    return {
        "meta": meta,
        "generation": {"n": 10, "mode": "random", "seed": 42},
        "templates": [template],
    }


def main(overwrite: bool):
    column_meta = load_column_stats(STATS_PATH)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total = 0
    for query in QUERY_DEFS:
        if query["kind"] == "point":
            selectivity_items = [("S0", None)]
        else:
            selectivity_items = list(SELECTIVITY_BANDS.items())
        for sel_label, sel_range in selectivity_items:
            for cfg_label in COLUMN_CONFIGS:
                spec = build_spec(query, sel_label, cfg_label, column_meta, sel_range)
                stem = f"spec_amazon_RQ1_{query['id']}_{sel_label}_{cfg_label}.yaml"
                out_path = OUTPUT_DIR / stem
                if out_path.exists() and not overwrite:
                    continue
                out_path.write_text(yaml.safe_dump(spec, sort_keys=False))
                total += 1
    print(f"Generated {total} specs under {OUTPUT_DIR}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-overwrite", action="store_true")
    args = ap.parse_args()
    main(overwrite=not args.no_overwrite)

