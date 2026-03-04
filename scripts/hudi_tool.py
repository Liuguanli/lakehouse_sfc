#!/usr/bin/env python3
"""Unified Hudi utility tool.

Subcommands:
- inspect: inspect .hoodie metadata structure/timeline/index
- footer-minmax: read parquet footer min/max stats
- metadata-minmax: read Hudi metadata table (column_stats) min/max
- sparksql: run SparkSQL on Hudi table with min/max + query stats
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid


HFILE_RE = re.compile(
    r"^(?P<file_group>[^_]+)_(?P<write_token>[^_]+)_(?P<instant>\d+)\.hfile$"
)
LOG_RE = re.compile(
    r"^\.(?P<file_group>[^_]+)_(?P<base_instant>\d+)\.log\.(?P<version>\d+)_(?P<write_token>.+)$"
)
DATA_FILE_RE = re.compile(
    r"^(?P<file_id>.+)_(?P<write_token>[^_]+)_(?P<instant>\d+)\.parquet$"
)


def split_csv(s: str | None) -> list[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def parse_sql_file(path: Path) -> list[str]:
    raw = path.read_text(encoding="utf-8")
    lines = []
    for line in raw.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    merged = "\n".join(lines)
    return [x.strip() for x in merged.split(";") if x.strip()]


def to_text(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except Exception:
            return v.hex()
    return str(v)


def less_than(a: Any, b: Any) -> bool:
    try:
        return a < b
    except Exception:
        return to_text(a) < to_text(b)


def greater_than(a: Any, b: Any) -> bool:
    try:
        return a > b
    except Exception:
        return to_text(a) > to_text(b)


def read_properties(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8", errors="ignore"))


def parse_hfile_name(name: str) -> dict[str, str] | None:
    m = HFILE_RE.match(name)
    return m.groupdict() if m else None


def parse_log_name(name: str) -> dict[str, str] | None:
    m = LOG_RE.match(name)
    return m.groupdict() if m else None


def parse_timeline_name(name: str) -> dict[str, str] | None:
    if name.startswith(".") or name.endswith(".crc"):
        return None
    parts = name.split(".")
    if len(parts) == 2:
        instant_part, second = parts
        if second in {"requested", "inflight"}:
            action = ""
            state = second
        else:
            action = second
            state = "completed"
    elif len(parts) == 3:
        instant_part, action, state = parts
    else:
        return None

    if "_" in instant_part:
        requested_instant, completed_instant = instant_part.split("_", 1)
    else:
        requested_instant, completed_instant = instant_part, ""
    return {
        "file": name,
        "requested_instant": requested_instant,
        "completed_instant": completed_instant,
        "action": action,
        "state": state,
    }


def collect_timeline(timeline_dir: Path) -> dict[str, Any]:
    entries: list[dict[str, str]] = []
    if timeline_dir.exists():
        for p in sorted(timeline_dir.iterdir()):
            if not p.is_file():
                continue
            parsed = parse_timeline_name(p.name)
            if parsed:
                entries.append(parsed)

    inferred_action: dict[str, str] = {}
    for item in entries:
        if item["action"]:
            inferred_action[item["requested_instant"]] = item["action"]
    for item in entries:
        if not item["action"]:
            item["action"] = inferred_action.get(item["requested_instant"], "unknown")

    action_counts = Counter((x["action"], x["state"]) for x in entries)
    summary = [
        {"action": action, "state": state, "count": count}
        for (action, state), count in sorted(action_counts.items())
    ]
    return {"entry_count": len(entries), "summary": summary, "entries": entries}


def collect_metadata_partition(partition_dir: Path) -> dict[str, Any]:
    files = [p.name for p in sorted(partition_dir.iterdir()) if p.is_file()]
    visible_files = [f for f in files if not f.endswith(".crc")]
    partition_meta = read_properties(partition_dir / ".hoodie_partition_metadata")

    hfiles: list[dict[str, str]] = []
    logs: list[dict[str, str]] = []
    file_groups: dict[str, dict[str, list[dict[str, str]]]] = defaultdict(
        lambda: {"hfiles": [], "logs": []}
    )

    for name in visible_files:
        h = parse_hfile_name(name)
        if h:
            h["name"] = name
            hfiles.append(h)
            file_groups[h["file_group"]]["hfiles"].append(h)
            continue
        lg = parse_log_name(name)
        if lg:
            lg["name"] = name
            logs.append(lg)
            file_groups[lg["file_group"]]["logs"].append(lg)

    return {
        "partition": partition_dir.name,
        "partition_metadata": partition_meta,
        "hfile_count": len(hfiles),
        "log_count": len(logs),
        "other_files": [
            f
            for f in visible_files
            if f != ".hoodie_partition_metadata"
            and parse_hfile_name(f) is None
            and parse_log_name(f) is None
        ],
        "file_groups": [
            {
                "file_group": fg,
                "hfile_count": len(payload["hfiles"]),
                "log_count": len(payload["logs"]),
                "hfiles": payload["hfiles"],
                "logs": payload["logs"],
            }
            for fg, payload in sorted(file_groups.items())
        ],
    }


def collect_data_partitions(table_root: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for p in sorted(table_root.iterdir()):
        if not p.is_dir() or p.name.startswith("."):
            continue
        parquet_files = [
            c.name
            for c in p.iterdir()
            if c.is_file() and c.name.endswith(".parquet") and not c.name.startswith(".")
        ]
        instants = sorted(
            {
                m.group("instant")
                for fname in parquet_files
                if (m := DATA_FILE_RE.match(fname)) is not None
            }
        )
        rows.append(
            {
                "partition": p.name,
                "partition_metadata": read_properties(p / ".hoodie_partition_metadata"),
                "parquet_file_count": len(parquet_files),
                "instants": instants,
            }
        )
    rows.sort(key=lambda x: x["partition"])
    return {
        "partition_count": len(rows),
        "total_parquet_files": sum(x["parquet_file_count"] for x in rows),
        "partitions": rows,
    }


def try_collect_avro_timeline_samples(timeline_dir: Path, limit: int) -> dict[str, Any]:
    try:
        from fastavro import reader  # type: ignore
    except Exception:
        return {
            "enabled": False,
            "reason": "fastavro not installed; install with `pip install fastavro`",
            "samples": [],
        }

    completed_files = []
    for p in sorted(timeline_dir.iterdir()):
        parsed = parse_timeline_name(p.name)
        if parsed and parsed["state"] == "completed":
            completed_files.append(p)
    completed_files = completed_files[-limit:]

    samples = []
    for p in completed_files:
        try:
            with p.open("rb") as fh:
                av = reader(fh)
                first = next(av, None)
                samples.append(
                    {
                        "file": p.name,
                        "schema_name": av.writer_schema.get("name"),
                        "schema_namespace": av.writer_schema.get("namespace"),
                        "first_record_keys": sorted(first.keys()) if isinstance(first, dict) else [],
                    }
                )
        except Exception as exc:
            samples.append({"file": p.name, "error": str(exc)})
    return {"enabled": True, "samples": samples}


def inspect_hoodie(hoodie_dir: Path, avro_limit: int) -> dict[str, Any]:
    table_root = hoodie_dir.parent
    metadata_root = hoodie_dir / "metadata"
    metadata_hoodie = metadata_root / ".hoodie"

    table_props = read_properties(hoodie_dir / "hoodie.properties")
    metadata_props = read_properties(metadata_hoodie / "hoodie.properties")
    index_defs = read_json_file(hoodie_dir / ".index_defs" / "index.json")

    metadata_partition_names = [
        x.strip()
        for x in table_props.get("hoodie.table.metadata.partitions", "").split(",")
        if x.strip()
    ]
    metadata_partitions = []
    for name in metadata_partition_names:
        part_dir = metadata_root / name
        if part_dir.exists() and part_dir.is_dir():
            metadata_partitions.append(collect_metadata_partition(part_dir))
        else:
            metadata_partitions.append({"partition": name, "error": "missing directory"})

    return {
        "table_root": str(table_root),
        "hoodie_dir": str(hoodie_dir),
        "table_properties": table_props,
        "metadata_table_properties": metadata_props,
        "index_definitions": index_defs,
        "main_timeline": collect_timeline(hoodie_dir / "timeline"),
        "metadata_timeline": collect_timeline(metadata_hoodie / "timeline"),
        "metadata_partitions": metadata_partitions,
        "data_partitions": collect_data_partitions(table_root),
        "avro_samples_main_timeline": try_collect_avro_timeline_samples(
            hoodie_dir / "timeline", limit=avro_limit
        ),
        "avro_samples_metadata_timeline": try_collect_avro_timeline_samples(
            metadata_hoodie / "timeline", limit=avro_limit
        ),
    }


def print_inspect_text(report: dict[str, Any]) -> None:
    tp = report["table_properties"]
    mtp = report["metadata_table_properties"]
    idx = report["index_definitions"].get("indexDefinitions", {})
    data_parts = report["data_partitions"]

    print(f"Table root: {report['table_root']}")
    print(f"Hoodie dir: {report['hoodie_dir']}")
    print("")
    print("Table:")
    print(f"  name={tp.get('hoodie.table.name', '')}")
    print(f"  type={tp.get('hoodie.table.type', '')}")
    print(f"  partition_fields={tp.get('hoodie.table.partition.fields', '')}")
    print(f"  record_keys={tp.get('hoodie.table.recordkey.fields', '')}")
    print(f"  metadata_partitions={tp.get('hoodie.table.metadata.partitions', '')}")
    print("")
    print("Metadata table:")
    print(f"  name={mtp.get('hoodie.table.name', '')}")
    print(f"  type={mtp.get('hoodie.table.type', '')}")
    print(f"  base_file_format={mtp.get('hoodie.table.base.file.format', '')}")
    print("")
    print(f"Index definitions: {len(idx)}")
    for name, payload in sorted(idx.items()):
        fields = payload.get("sourceFields", [])
        print(f"  - {name}: type={payload.get('indexType', '')}, fields={len(fields)}")
    print("")
    print("Metadata partitions:")
    for mp in report["metadata_partitions"]:
        if "error" in mp:
            print(f"  - {mp['partition']}: ERROR {mp['error']}")
            continue
        meta = mp.get("partition_metadata", {})
        print(
            f"  - {mp['partition']}: hfiles={mp['hfile_count']}, logs={mp['log_count']}, "
            f"commitTime={meta.get('commitTime', '')}"
        )
        for fg in mp.get("file_groups", []):
            print(
                f"      file_group={fg['file_group']} hfiles={fg['hfile_count']} logs={fg['log_count']}"
            )
    print("")
    print(
        f"Data partitions: {data_parts['partition_count']}, parquet_files={data_parts['total_parquet_files']}"
    )
    for part in data_parts["partitions"][:20]:
        print(
            f"  - {part['partition']}: parquet={part['parquet_file_count']} "
            f"instants={','.join(part['instants'])}"
        )
    if len(data_parts["partitions"]) > 20:
        print(f"  ... ({len(data_parts['partitions']) - 20} more partitions)")
    print("")
    print("Main timeline summary:")
    for row in report["main_timeline"]["summary"]:
        print(f"  - action={row['action']} state={row['state']} count={row['count']}")
    print("Metadata timeline summary:")
    for row in report["metadata_timeline"]["summary"]:
        print(f"  - action={row['action']} state={row['state']} count={row['count']}")


@dataclass
class ColStat:
    min_v: Any | None = None
    max_v: Any | None = None
    null_count: int = 0
    row_groups: int = 0

    def update(self, min_v: Any, max_v: Any, null_count: int) -> None:
        if self.min_v is None or less_than(min_v, self.min_v):
            self.min_v = min_v
        if self.max_v is None or greater_than(max_v, self.max_v):
            self.max_v = max_v
        self.null_count += int(null_count)
        self.row_groups += 1


def iter_parquet_files(table_root: Path):
    for p in table_root.rglob("*.parquet"):
        if any(part.startswith(".") for part in p.relative_to(table_root).parts):
            continue
        yield p


def collect_file_stats(file_path: Path, only_column: str | None) -> dict[str, ColStat]:
    pf = pq.ParquetFile(file_path)
    out: dict[str, ColStat] = {}
    for rg_idx in range(pf.metadata.num_row_groups):
        rg = pf.metadata.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            name = col.path_in_schema
            if only_column and name != only_column:
                continue
            stats = col.statistics
            if stats is None or not getattr(stats, "has_min_max", False):
                continue
            st = out.setdefault(name, ColStat())
            st.update(stats.min, stats.max, getattr(stats, "null_count", 0) or 0)
    return out


def norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def pick_field(field_names: list[str], candidates: list[str]) -> str | None:
    lookup = {norm_token(x): x for x in field_names}
    for c in candidates:
        if c in lookup:
            return lookup[c]
    return None


def infer_sort_cols(existing_cols: list[str], user_cols: list[str]) -> list[str]:
    if user_cols:
        return [c for c in user_cols if c in existing_cols]
    defaults = ["record_timestamp", "l_shipdate", "l_receiptdate", "event_time", "ts"]
    return [c for c in defaults if c in existing_cols]


def run_cmd_inspect(args: argparse.Namespace) -> int:
    hoodie_dir = Path(args.hoodie_dir).resolve()
    if hoodie_dir.name != ".hoodie":
        print(f"ERROR: expected .hoodie dir, got: {hoodie_dir}")
        return 2
    if not hoodie_dir.exists():
        print(f"ERROR: path does not exist: {hoodie_dir}")
        return 2
    report = inspect_hoodie(hoodie_dir=hoodie_dir, avro_limit=max(1, args.avro_samples_limit))
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_inspect_text(report)
    return 0


def run_cmd_footer_minmax(args: argparse.Namespace) -> int:
    table_root = Path(args.table_root).resolve()
    if not table_root.exists():
        print(f"ERROR: table root not found: {table_root}")
        return 2
    files = sorted(iter_parquet_files(table_root))
    if not files:
        print(f"ERROR: no parquet files found under: {table_root}")
        return 2

    global_stats: dict[str, ColStat] = {}
    file_rows: list[dict[str, Any]] = []
    skipped_files: list[dict[str, str]] = []

    for f in files:
        rel = f.relative_to(table_root)
        part = rel.parts[0] if len(rel.parts) > 1 else ""
        try:
            cstats = collect_file_stats(f, args.column)
        except (ArrowInvalid, OSError, ValueError) as exc:
            skipped_files.append({"file": str(rel), "reason": str(exc)})
            continue
        for col, st in cstats.items():
            gst = global_stats.setdefault(col, ColStat())
            gst.update(st.min_v, st.max_v, st.null_count)
            file_rows.append(
                {
                    "partition": part,
                    "file": str(rel),
                    "column": col,
                    "min": to_text(st.min_v),
                    "max": to_text(st.max_v),
                    "row_groups_with_stats": st.row_groups,
                    "null_count_sum": st.null_count,
                }
            )

    if args.format == "json":
        print(
            json.dumps(
                {
                    "table_root": str(table_root),
                    "file_count": len(files),
                    "skipped_file_count": len(skipped_files),
                    "skipped_files": skipped_files,
                    "global_minmax": {
                        c: {
                            "min": to_text(st.min_v),
                            "max": to_text(st.max_v),
                            "row_groups_with_stats": st.row_groups,
                            "null_count_sum": st.null_count,
                        }
                        for c, st in sorted(global_stats.items())
                    },
                    "file_level": file_rows,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    print(f"table_root={table_root}")
    print(f"parquet_files={len(files)}")
    print(f"skipped_non_parquet_or_invalid={len(skipped_files)}")
    print("")
    print("Global min/max by column:")
    for c, st in sorted(global_stats.items()):
        print(
            f"- {c}: min={to_text(st.min_v)}, max={to_text(st.max_v)}, "
            f"row_groups={st.row_groups}, null_count_sum={st.null_count}"
        )
    print("")
    print(f"File-level stats (first {args.limit_files} rows):")
    for r in file_rows[: args.limit_files]:
        print(
            f"- partition={r['partition']} column={r['column']} min={r['min']} max={r['max']} file={r['file']}"
        )
    if len(file_rows) > args.limit_files:
        print(f"... {len(file_rows) - args.limit_files} more rows")
    if skipped_files:
        print("")
        print("Skipped files (first 20):")
        for row in skipped_files[:20]:
            print(f"- file={row['file']} reason={row['reason']}")
    return 0


def run_cmd_metadata_minmax(args: argparse.Namespace) -> int:
    table_root = Path(args.table_root).resolve()
    metadata_path = (
        Path(args.metadata_path).resolve()
        if args.metadata_path
        else (table_root / ".hoodie" / "metadata").resolve()
    )

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    spark = (
        SparkSession.builder.appName("hudi-tool-metadata-minmax")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", f"/tmp/lakehouse_warehouse_{os.getpid()}")
        .getOrCreate()
    )
    try:
        df = spark.read.format("hudi").load(str(metadata_path))
        print(f"metadata_path={metadata_path}")
        print("schema:")
        df.printSchema()

        cols = df.columns
        if "_hoodie_partition_path" in cols:
            print("metadata partition distribution:")
            (
                df.groupBy("_hoodie_partition_path")
                .count()
                .orderBy(F.desc("count"))
                .show(50, truncate=False)
            )

        scoped = df
        if "_hoodie_partition_path" in cols and not args.include_all_metadata_partitions:
            scoped = scoped.filter(F.col("_hoodie_partition_path") == F.lit("column_stats"))

        targets: list[tuple[str, str, str, str | None, str | None]] = []
        for fld in scoped.schema.fields:
            if not isinstance(fld.dataType, StructType):
                continue
            names = [x.name for x in fld.dataType.fields]
            min_f = pick_field(names, ["minvalue", "minval", "minimumvalue", "minimum"])
            max_f = pick_field(names, ["maxvalue", "maxval", "maximumvalue", "maximum"])
            if not (min_f and max_f):
                continue
            col_f = pick_field(names, ["columnname", "colname", "column"])
            file_f = pick_field(names, ["filename", "file", "fileid"])
            targets.append((fld.name, min_f, max_f, col_f, file_f))

        if not targets:
            print("No struct column containing min/max fields found.")
            return 1

        struct_col, min_f, max_f, col_f, file_f = targets[0]
        print(
            f"using struct={struct_col}, min={min_f}, max={max_f}, "
            f"column_field={col_f}, file_field={file_f}"
        )

        select_exprs = [
            F.col("_hoodie_record_key").alias("record_key")
            if "_hoodie_record_key" in cols
            else F.lit(None).alias("record_key"),
            F.col("_hoodie_partition_path").alias("metadata_partition")
            if "_hoodie_partition_path" in cols
            else F.lit(None).alias("metadata_partition"),
            F.col(f"{struct_col}.{file_f}").cast("string").alias("file_name")
            if file_f
            else F.lit(None).alias("file_name"),
            F.col(f"{struct_col}.{col_f}").cast("string").alias("column_name")
            if col_f
            else F.lit(None).alias("column_name"),
            F.col(f"{struct_col}.{min_f}").alias("min_raw"),
            F.col(f"{struct_col}.{max_f}").alias("max_raw"),
        ]
        result = scoped.select(*select_exprs)
        if args.column:
            result = result.filter(F.col("column_name") == F.lit(args.column))

        if col_f is None:
            print("WARNING: metadata struct does not expose column name field; filtering by --column is unavailable.")

        print("summary min/max:")
        summary = (
            result.groupBy("column_name")
            .agg(
                F.min(F.col("min_raw")).alias("global_min_raw"),
                F.max(F.col("max_raw")).alias("global_max_raw"),
                F.count("*").alias("file_stats_rows"),
            )
            .select(
                F.col("column_name").alias("column"),
                F.col("global_min_raw").cast("string").alias("global_min"),
                F.col("global_max_raw").cast("string").alias("global_max"),
                F.col("file_stats_rows"),
            )
            .orderBy("column")
        )
        summary.show(args.summary_limit, truncate=False)

        if not args.summary_only:
            print("file-level min/max rows:")
            (
                result.select(
                    F.col("file_name").alias("file"),
                    F.col("column_name").alias("column"),
                    F.col("min_raw").cast("string").alias("min"),
                    F.col("max_raw").cast("string").alias("max"),
                )
                .orderBy("column", "file")
                .show(args.limit, truncate=False)
            )
        return 0
    finally:
        spark.stop()


def run_cmd_sparksql(args: argparse.Namespace) -> int:
    table_root = str(Path(args.table_root).resolve())
    os.environ.setdefault("USER", os.environ.get("LOGNAME", "user"))
    os.environ.setdefault("LOGNAME", os.environ.get("USER", "user"))
    os.environ.setdefault("HADOOP_USER_NAME", os.environ["USER"])

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark = (
        SparkSession.builder.appName("hudi-tool-sparksql")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", f"/tmp/lakehouse_warehouse_{os.getpid()}")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.format("hudi").load(table_root)
        df.createOrReplaceTempView(args.view_name)
        print(f"table_root={table_root}")
        print(f"view_name={args.view_name}")
        if args.skip_table_count:
            print("row_count=skipped")
        else:
            print(f"row_count={df.count()}")
        print(f"column_count={len(df.columns)}")
        print("")

        sort_cols = infer_sort_cols(df.columns, split_csv(args.sort_cols))
        if args.skip_sort_minmax:
            print("=== Min/Max (Sort Columns) ===")
            print("skipped (--skip-sort-minmax)")
            print("")
        elif sort_cols:
            agg_exprs = []
            for c in sort_cols:
                col = F.col(c)
                agg_exprs.append(F.min(col).alias(c + "__min"))
                agg_exprs.append(F.max(col).alias(c + "__max"))
                agg_exprs.append(F.sum(col.isNull().cast("long")).alias(c + "__nulls"))
            row = df.agg(*agg_exprs).collect()[0].asDict()
            print("=== Min/Max (Sort Columns) ===")
            for c in sort_cols:
                print(
                    f"- {c}: min={to_text(row.get(c + '__min'))}, "
                    f"max={to_text(row.get(c + '__max'))}, nulls={to_text(row.get(c + '__nulls'))}"
                )
            print("")

        if args.minmax_only:
            return 0

        queries = list(args.query)
        for qf in args.query_file:
            queries.extend(parse_sql_file(Path(qf)))
        if not queries:
            print("No query provided.")
            print(f"Example: --query \"SELECT COUNT(*) FROM {args.view_name}\"")
            return 0

        numeric_types = (
            T.ByteType,
            T.ShortType,
            T.IntegerType,
            T.LongType,
            T.FloatType,
            T.DoubleType,
            T.DecimalType,
        )
        temporal_types = (T.DateType, T.TimestampType)

        for i, sql in enumerate(queries, start=1):
            print(f"=== Query {i} ===")
            print(sql)
            t0 = time.perf_counter()
            qdf = spark.sql(sql)
            if args.cache_query_result:
                qdf = qdf.cache()

            rows: int | None = None
            if not args.skip_count:
                rows = qdf.count()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            rows_text = "skipped" if rows is None else str(rows)
            print(f"rows={rows_text}, columns={len(qdf.columns)}, elapsed_ms={elapsed_ms:.1f}")
            print(f"[Preview top {args.preview_rows}]")
            qdf.show(args.preview_rows, truncate=False)

            if args.skip_column_stats:
                print("[Column Stats]")
                print("skipped (--skip-column-stats)")
            else:
                stat_df = qdf.limit(args.preview_rows) if args.stats_on_preview else qdf
                fields = list(stat_df.schema.fields)[: args.max_stat_cols]
                agg_exprs = []
                kinds: dict[str, str] = {}
                for f in fields:
                    c = f.name
                    col = F.col(c)
                    agg_exprs.append(F.sum(col.isNull().cast("long")).alias(c + "__nulls"))
                    if isinstance(f.dataType, numeric_types):
                        kinds[c] = "numeric"
                        agg_exprs.extend(
                            [
                                F.min(col).alias(c + "__min"),
                                F.max(col).alias(c + "__max"),
                                F.avg(col).alias(c + "__avg"),
                            ]
                        )
                    elif isinstance(f.dataType, temporal_types):
                        kinds[c] = "temporal"
                        agg_exprs.extend([F.min(col).alias(c + "__min"), F.max(col).alias(c + "__max")])
                    else:
                        kinds[c] = "other"
                        agg_exprs.extend(
                            [
                                F.min(col).cast("string").alias(c + "__min"),
                                F.max(col).cast("string").alias(c + "__max"),
                            ]
                        )

                stat_row = stat_df.agg(*agg_exprs).collect()[0].asDict() if agg_exprs else {}
                src = f"preview({args.preview_rows})" if args.stats_on_preview else "full_result"
                print(f"[Column Stats] source={src}")
                for f in fields:
                    c = f.name
                    avg = stat_row.get(c + "__avg")
                    avg_part = f", avg={to_text(avg)}" if avg is not None else ""
                    print(
                        f"- {c} ({kinds.get(c, 'other')}): min={to_text(stat_row.get(c + '__min'))}, "
                        f"max={to_text(stat_row.get(c + '__max'))}, nulls={to_text(stat_row.get(c + '__nulls'))}{avg_part}"
                    )
                if len(stat_df.columns) > args.max_stat_cols:
                    print(f"... only first {args.max_stat_cols} columns were profiled")
            print("")
            if args.cache_query_result:
                qdf.unpersist()
        return 0
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Unified Hudi tool with subcommands.")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ins = sub.add_parser("inspect", help="Inspect .hoodie metadata structure")
    p_ins.add_argument(
        "hoodie_dir",
        nargs="?",
        default="data/amazon/hudi_user_time/hudi_linear/.hoodie",
        help="Path to .hoodie directory",
    )
    p_ins.add_argument("--format", choices=["text", "json"], default="text")
    p_ins.add_argument("--avro-samples-limit", type=int, default=2)
    p_ins.set_defaults(func=run_cmd_inspect)

    p_foot = sub.add_parser("footer-minmax", help="Read parquet footer min/max stats")
    p_foot.add_argument("--table-root", default="data/amazon/hudi_user_time/hudi_linear")
    p_foot.add_argument("--column", default=None)
    p_foot.add_argument("--limit-files", type=int, default=30)
    p_foot.add_argument("--format", choices=["text", "json"], default="text")
    p_foot.set_defaults(func=run_cmd_footer_minmax)

    p_md = sub.add_parser("metadata-minmax", help="Read Hudi metadata table column_stats min/max")
    p_md.add_argument("--table-root", default="data/amazon/hudi_user_time/hudi_linear")
    p_md.add_argument("--metadata-path", default=None)
    p_md.add_argument("--column", default=None)
    p_md.add_argument("--limit", type=int, default=100)
    p_md.add_argument("--summary-limit", type=int, default=200)
    p_md.add_argument("--summary-only", action="store_true")
    p_md.add_argument("--include-all-metadata-partitions", action="store_true")
    p_md.set_defaults(func=run_cmd_metadata_minmax)

    p_sql = sub.add_parser("sparksql", help="Run SparkSQL queries on Hudi table")
    p_sql.add_argument("--table-root", default="data/amazon/hudi_user_time/hudi_linear")
    p_sql.add_argument("--view-name", default="hudi_tbl")
    p_sql.add_argument("--sort-cols", default=None)
    p_sql.add_argument("--query", action="append", default=[])
    p_sql.add_argument("--query-file", action="append", default=[])
    p_sql.add_argument("--preview-rows", type=int, default=20)
    p_sql.add_argument("--max-stat-cols", type=int, default=20)
    p_sql.add_argument("--minmax-only", action="store_true")
    p_sql.add_argument("--cache-query-result", action="store_true")
    p_sql.add_argument("--skip-count", action="store_true")
    p_sql.add_argument("--skip-column-stats", action="store_true")
    p_sql.add_argument("--skip-table-count", action="store_true")
    p_sql.add_argument("--skip-sort-minmax", action="store_true")
    p_sql.add_argument(
        "--stats-on-preview",
        action="store_true",
        help="Compute column stats from preview rows only (faster, approximate).",
    )
    p_sql.set_defaults(func=run_cmd_sparksql)

    return p


def main() -> int:
    args = build_parser().parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
