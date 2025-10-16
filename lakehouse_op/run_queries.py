#!/usr/bin/env python3
import argparse, os, time, json, urllib.request, pathlib, csv, re
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# ---------------- helpers ----------------
def read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def substitute(sql: str, table: str) -> str:
    return sql.replace("{{tbl}}", table)

def get_spark(app_name: str) -> SparkSession:
    """
    Create a SparkSession with a few sane defaults. You can still override via env vars.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", os.getenv("ADAPTIVE", "true"))
        .config("spark.sql.files.maxPartitionBytes", os.getenv("MAX_PART_BYTES", "256m"))
        .config("spark.sql.parquet.enableVectorizedReader", os.getenv("VEC_READER", "true"))
        .getOrCreate()
    )

def get_ui_base_url(spark: SparkSession) -> str | None:
    """
    Return Spark UI base URL if available, otherwise None.
    """
    try:
        return spark._jsc.sc().uiWebUrl().get()
    except Exception:
        return None

def http_json(url: str):
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))

# ---------------- table preparation (key change) ----------------
def _strip_outer_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def _looks_like_path(s: str) -> bool:
    """
    Heuristic: treat absolute paths or file: URIs as 'path-like'.
    """
    ss = s.strip()
    return ss.startswith("/") or ss.startswith("file:")

def prepare_table_view(spark: SparkSession, engine: str, table_arg: str) -> str:
    """
    Normalize --table into something that can be directly used in SQL as {{tbl}}.

    Behaviors by engine:
      - Hudi:
          * If a path (e.g., /abs/path, file:/abs/path, or hudi.'...' / hudi.`...`), read via
            DataFrame API (format("hudi").load(path)) and register a temp view named "_tbl".
            Return "_tbl" so SQL can use FROM {{tbl}} safely.
          * Otherwise, treat as a metastore table name and return as-is.
      - Delta:
          * If already in the form delta.`/path` or delta."...", return as-is.
          * If a path-like string, wrap as delta.`/abs/path` and return.
          * Otherwise, return as a catalog table name.
      - Iceberg:
          * Return as-is (typically a catalog identifier like local.db.table).
    """
    t = _strip_outer_quotes(table_arg)
    eng = engine.lower()

    if eng == "hudi":
        path = None
        # hudi.`/path`
        if t.startswith("hudi.`") and t.endswith("`"):
            path = t[len("hudi.`"):-1]
        # hudi.'...'
        elif t.startswith("hudi.'") and t.endswith("'"):
            path = t[len("hudi.'"):-1]
        # plain path or file: URI
        elif _looks_like_path(t):
            path = t

        if path:
            df = spark.read.format("hudi").load(path)
            view = "_tbl"
            df.createOrReplaceTempView(view)
            return view
        else:
            # assume catalog table
            return t

    if eng == "delta":
        # already delta.`...` or delta."..."
        if re.match(r"^delta\.\s*`.+`\s*$", t) or re.match(r'^delta\.\s*".+"\s*$', t):
            return t
        # path-like -> delta.`/abs/path`
        if _looks_like_path(t):
            return f"delta.`{t}`"
        # catalog table name
        return t

    # iceberg: use as provided (catalog table or identifier)
    return t

# ---------------- metrics via REST ----------------
def collect_metrics_via_rest(ui_base: str, job_group_id: str) -> dict:
    """
    Aggregate stage-level metrics for jobs in the given jobGroup:
      - bytesRead (bytes)
      - executorRunTime_ms (milliseconds, sum over tasks)
      - executorCpuTime_ns (nanoseconds, sum over tasks)
    """
    apps = http_json(f"{ui_base}/api/v1/applications")
    if not apps:
        return {}
    app_id = apps[0]["id"]

    app_jobs = http_json(f"{ui_base}/api/v1/applications/{app_id}/jobs")
    target_jobs = [j for j in app_jobs if j.get("jobGroup") == job_group_id]
    if not target_jobs:
        return {}

    stage_ids = {sid for j in target_jobs for sid in j.get("stageIds", [])}
    stages = http_json(f"{ui_base}/api/v1/applications/{app_id}/stages")

    metrics = {
        "bytesRead": 0,           # bytes
        "executorRunTime_ms": 0,  # ms (sum)
        "executorCpuTime_ns": 0,  # ns (sum)
    }

    for st in stages:
        if st.get("stageId") not in stage_ids:
            continue
        ib = st.get("inputBytes")
        if isinstance(ib, int):
            metrics["bytesRead"] += ib
        if isinstance(st.get("executorRunTime"), int):
            metrics["executorRunTime_ms"] += st["executorRunTime"]
        if isinstance(st.get("executorCpuTime"), int):
            metrics["executorCpuTime_ns"] += st["executorCpuTime"]

    return metrics

# ---------------- cache control ----------------
def maybe_cache_table(spark, table, cache_mode):
    """
    Optionally cache the table:
      - none: do nothing
      - catalog: run 'CACHE TABLE <table>'
      - df: materialize a DF of 'SELECT * FROM <table>' and persist(MEMORY_AND_DISK)
    """
    if cache_mode == "none":
        return None
    if cache_mode == "catalog":
        spark.sql(f"CACHE TABLE {table}")
        return None
    elif cache_mode == "df":
        df = spark.sql(f"SELECT * FROM {table}")
        df.persist(StorageLevel.MEMORY_AND_DISK)
        df.count()  # materialize
        return df

def maybe_uncache_table(spark, table, cache_obj, cache_mode):
    """
    Undo caching per the chosen mode.
    """
    if cache_mode == "none":
        return
    if cache_mode == "catalog":
        spark.sql(f"UNCACHE TABLE {table}")
    elif cache_mode == "df" and cache_obj is not None:
        cache_obj.unpersist()

# ---------------- single query runner ----------------
def run_one_query(spark, sql_text, group_id, action) -> dict:
    """
    Execute a single query under a jobGroup and collect:
      - wall-clock time (seconds)
      - Spark REST metrics (bytesRead, executorRunTime_s, executorCpuTime_s)
    """
    sc = spark.sparkContext
    sc.setJobGroup(group_id, f"query-{group_id}")
    wall_start = time.time()
    try:
        df = spark.sql(sql_text)
        if action == "count":
            df.count()
        elif action == "collect":
            _ = df.collect()
        elif action == "show":
            df.show(5, truncate=False)
        else:
            df.count()
    finally:
        # clear job group regardless of success/failure
        sc.setLocalProperty("spark.jobGroup.id", None)

    wall_s = (time.time() - wall_start)

    # Pull metrics via REST API (if Spark UI is available)
    ui = get_ui_base_url(spark)
    bytes_read = 0
    exec_run_ms = 0
    exec_cpu_ns = 0
    if ui:
        m = collect_metrics_via_rest(ui, group_id) or {}
        bytes_read  = m.get("bytesRead", 0)
        exec_run_ms = m.get("executorRunTime_ms", 0)
        exec_cpu_ns = m.get("executorCpuTime_ns", 0)

    # Convert times to seconds for output CSV
    exec_run_s = exec_run_ms / 1000.0
    exec_cpu_s = exec_cpu_ns / 1_000_000_000.0

    return {
        "bytesRead": bytes_read,          # bytes
        "elapsedTime_s": wall_s,          # wall-clock seconds
        "executorRunTime_s": exec_run_s,  # sum over tasks, seconds
        "executorCpuTime_s": exec_cpu_s,  # sum over tasks, seconds
    }

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser("Unified query runner for Delta/Hudi/Iceberg")
    ap.add_argument("--engine", required=True, choices=["delta", "hudi", "iceberg"])
    ap.add_argument("--table", required=True, help="Table identifier (e.g., local.demo.table, delta.`/path`, or absolute path for Hudi)")
    ap.add_argument("--queries_dir", default="queries", help="Directory containing .sql files with {{tbl}} placeholder")
    ap.add_argument("--warmup", action="store_true", help="Run each query once as warm-up (discard metrics)")
    ap.add_argument("--cache", choices=["none", "catalog", "df"], default="none", help="Caching mode")
    ap.add_argument("--action", choices=["count", "collect", "show"], default="count", help="Spark action to materialize the query")
    ap.add_argument("--output_csv", default="results.csv", help="Path to write metrics CSV")
    ap.add_argument("--broadcast_hint", action="store_true", help="Enable broadcast join via conf")
    args = ap.parse_args()

    spark = get_spark(app_name=f"runner-{args.engine}")

    # Optional engine-specific tweak
    if args.broadcast_hint:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB

    # Normalize the table argument into an SQL-usable identifier or a temp view
    sql_table = prepare_table_view(spark, args.engine, args.table)

    # Optional caching
    cache_obj = maybe_cache_table(spark, sql_table, args.cache)

    # Enumerate queries
    qfiles = sorted(str(p) for p in pathlib.Path(args.queries_dir).glob("*.sql"))
    if not qfiles:
        raise SystemExit(f"No .sql files found in {args.queries_dir}")

    # Run and record metrics
    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["engine","query","bytesRead","elapsedTime_s","executorRunTime_s","executorCpuTime_s"])
        for qf in qfiles:
            raw = read_sql(qf)
            sql_text = substitute(raw, sql_table)

            # warm-up run (discard)
            if args.warmup:
                _ = run_one_query(spark, sql_text, f"warmup-{os.path.basename(qf)}", args.action)

            # measured run
            m = run_one_query(spark, sql_text, f"run-{os.path.basename(qf)}", args.action)
            writer.writerow([
                args.engine,
                os.path.basename(qf),
                m["bytesRead"],
                f"{m['elapsedTime_s']:.3f}",
                f"{m['executorRunTime_s']:.3f}",
                f"{m['executorCpuTime_s']:.3f}",
            ])

    # Cleanup
    maybe_uncache_table(spark, sql_table, cache_obj, args.cache)
    spark.stop()
    print(f"[OK] wrote {args.output_csv}")

if __name__ == "__main__":
    main()
