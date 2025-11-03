# #!/usr/bin/env python3
# import argparse, os, time, json, urllib.request, pathlib, csv, re
# from pyspark.sql import SparkSession
# from pyspark.storagelevel import StorageLevel

# # ---------------- helpers ----------------
# def read_sql(path: str) -> str:
#     with open(path, "r", encoding="utf-8") as f:
#         return f.read()

# def substitute(sql: str, table: str) -> str:
#     return sql.replace("{{tbl}}", table)

# def get_spark(app_name: str) -> SparkSession:
#     """
#     Create a SparkSession with a few sane defaults. You can still override via env vars.
#     """
#     return (
#         SparkSession.builder
#         .appName(app_name)
#         .config("spark.sql.adaptive.enabled", os.getenv("ADAPTIVE", "true"))
#         .config("spark.sql.files.maxPartitionBytes", os.getenv("MAX_PART_BYTES", "256m"))
#         .config("spark.sql.parquet.enableVectorizedReader", os.getenv("VEC_READER", "true"))
#         .getOrCreate()
#     )

# def get_ui_base_url(spark: SparkSession) -> str | None:
#     """
#     Return Spark UI base URL if available, otherwise None.
#     """
#     try:
#         return spark._jsc.sc().uiWebUrl().get()
#     except Exception:
#         return None

# def http_json(url: str):
#     with urllib.request.urlopen(url) as resp:
#         return json.loads(resp.read().decode("utf-8"))

# # ---------------- table preparation (key change) ----------------
# def _strip_outer_quotes(s: str) -> str:
#     s = s.strip()
#     if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
#         return s[1:-1]
#     return s

# def _looks_like_path(s: str) -> bool:
#     """
#     Heuristic: treat absolute paths or file: URIs as 'path-like'.
#     """
#     ss = s.strip()
#     return ss.startswith("/") or ss.startswith("file:")

# def prepare_table_view(spark: SparkSession, engine: str, table_arg: str) -> str:
#     """
#     Normalize --table into something that can be directly used in SQL as {{tbl}}.
#     """
#     t = _strip_outer_quotes(table_arg)
#     eng = engine.lower()

#     if eng == "hudi":
#         path = None
#         # hudi.`/path`
#         if t.startswith("hudi.`") and t.endswith("`"):
#             path = t[len("hudi.`"):-1]
#         # hudi.'...'
#         elif t.startswith("hudi.'") and t.endswith("'"):
#             path = t[len("hudi.'"):-1]
#         # plain path or file: URI
#         elif _looks_like_path(t):
#             path = t

#         if path:
#             df = spark.read.format("hudi").load(path)
#             view = "_tbl"
#             df.createOrReplaceTempView(view)
#             return view
#         else:
#             return t

#     if eng == "delta":
#         # already delta.`...` or delta."..."
#         if re.match(r"^delta\.\s*`.+`\s*$", t) or re.match(r'^delta\.\s*".+"\s*$', t):
#             return t
#         # path-like -> delta.`/abs/path`
#         if _looks_like_path(t):
#             return f"delta.`{t}`"
#         return t

#     # iceberg: use as provided (catalog table or identifier)
#     return t

# # ---------------- app selection ----------------
# def choose_app_id(ui_base: str, spark: SparkSession, retry: int = 3, sleep_s: float = 0.1) -> str | None:
#     """
#     Return the best app id to query from the REST API for this SparkSession.

#     Strategy:
#       1) Prefer spark.sparkContext.applicationId if present in REST apps.
#       2) Match by spark app name.
#       3) Use newest app by attempts startTime.
#       4) Fallback to last entry.
#     """
#     def jget(url: str):
#         with urllib.request.urlopen(url) as r:
#             return json.loads(r.read().decode("utf-8"))

#     apps = []
#     for _ in range(retry):
#         apps = jget(f"{ui_base}/api/v1/applications") or []
#         if apps:
#             break
#         time.sleep(sleep_s)
#     if not apps:
#         return None

#     # 1) by applicationId
#     desired_id = None
#     try:
#         desired_id = spark.sparkContext.applicationId
#     except Exception:
#         pass
#     if desired_id:
#         for a in apps:
#             if a.get("id") == desired_id or (desired_id in a.get("id", "")):
#                 return a.get("id")

#     # 2) by app name
#     desired_name = None
#     try:
#         desired_name = spark.sparkContext.appName
#     except Exception:
#         pass
#     if not desired_name:
#         try:
#             desired_name = spark.conf.get("spark.app.name")
#         except Exception:
#             desired_name = None
#     if desired_name:
#         for a in apps:
#             if str(a.get("name", "")).strip() == str(desired_name).strip():
#                 return a.get("id")

#     # 3) newest by startTime
#     def _start_time(app):
#         atts = app.get("attempts") or []
#         if atts and isinstance(atts[0], dict):
#             return atts[0].get("startTime") or ""
#         return ""
#     try:
#         apps_sorted = sorted(apps, key=_start_time)
#         return apps_sorted[-1].get("id")
#     except Exception:
#         # 4) fallback to last
#         return apps[-1].get("id")

# # ---------------- bytes read: preferred metrics ----------------
# def bytes_sql_execution(spark: SparkSession) -> int | None:
#     """
#     Return inputBytes (or similar) of the last SQL execution (already deduped/aggregated).
#     Call AFTER an action (count/collect/show).
#     """
#     try:
#         jvm = spark._jvm
#         sc = spark._jsparkSession.sparkContext()
#         # Try the common store; some minor versions may differ—add more candidates if needed
#         store = jvm.org.apache.spark.sql.execution.ui.SQLAppStatusStore(sc.statusStore())
#         execs = store.executionsList()
#         if execs is None or execs.isEmpty():
#             return None
#         last = execs.get(execs.size() - 1)
#         metrics = store.executionMetrics(last.executionId())
#         for k in ("inputBytes", "bytesRead", "scanBytes"):
#             if metrics.containsKey(k):
#                 # value may already be numeric; guard with int()
#                 try:
#                     return int(metrics.get(k))
#                 except Exception:
#                     return int(str(metrics.get(k)))
#         return None
#     except Exception:
#         return None

# def bytes_from_plan_scans(df) -> int | None:
#     """
#     Sum 'bytes read' from scan nodes in executed plan.
#     Accepts keys: inputBytes / bytesRead / scanBytes.
#     Call AFTER an action.
#     """
#     try:
#         jvm = df._sc._jvm
#         plan = df._jdf.queryExecution().executedPlan()
#         # Match broader: any node whose name contains 'scan' or 'filesource'
#         IsScan = type(
#             "IsScan",
#             (jvm.scala.runtime.AbstractFunction1,),
#             {"apply": lambda self, p: ("scan" in p.nodeName().lower()) or ("filesource" in p.nodeName().lower())},
#         )
#         scans = plan.collect(IsScan())
#         it = scans.iterator()
#         total = 0
#         found = False
#         while it.hasNext():
#             n = it.next()
#             m = n.metrics()  # scala.collection.Map[String, SQLMetric]
#             for key in ("inputBytes", "bytesRead", "scanBytes"):
#                 if m.contains(key):
#                     total += int(m.apply(key).value())
#                     found = True
#                     break
#         return total if found else None
#     except Exception:
#         return None

# def bytes_sum_input_files(df) -> int:
#     """
#     Sum Hadoop FS lengths of df.inputFiles() as an upper bound (compressed, no pruning).
#     """
#     try:
#         files = list(df.inputFiles())
#         if not files:
#             return 0
#         jvm = df._sc._jvm
#         fs = jvm.org.apache.hadoop.fs.FileSystem.get(df._sc._jsc.hadoopConfiguration())
#         Path = jvm.org.apache.hadoop.fs.Path
#         total = 0
#         for f in files:
#             try:
#                 total += int(fs.getFileStatus(Path(f)).getLen())
#             except Exception:
#                 pass
#         return total
#     except Exception:
#         # As a last resort, fall back to 0
#         return 0

# # ---------------- metrics via REST (task de-dup) ----------------
# def collect_metrics_via_rest(ui_base: str, job_group_id: str, spark: SparkSession | None = None) -> dict:
#     """
#     Collect Spark metrics for a specific jobGroup via the REST API.

#     What it does:
#       - Chooses the right application id (avoid apps[0]) using SparkContext, then sensible fallbacks.
#       - Finds jobs that belong to the given jobGroup (matches jobGroup field, properties, or description).
#       - Gathers stageIds for those jobs.
#       - For each stage, fetches the latest attempt's task list and de-duplicates tasks by (stageId, taskId).
#       - Sums:
#           * bytesRead (from task inputMetrics.bytesRead)           -> bytes
#           * executorRunTime_ms (from task or taskMetrics)          -> milliseconds
#           * executorCpuTime_ns (from task or taskMetrics)          -> nanoseconds
#       - If task-level data isn't available, falls back to stage-level aggregates.

#     Always returns a dict with integer fields.
#     """
#     def jget(url: str):
#         with urllib.request.urlopen(url) as r:
#             return json.loads(r.read().decode("utf-8"))

#     out = {"bytesRead": 0, "executorRunTime_ms": 0, "executorCpuTime_ns": 0}

#     # 1) Pick the correct app id
#     app_id = choose_app_id(ui_base, spark) if spark is not None else None
#     if not app_id:
#         # Last resort: try listing and pick the last one
#         apps = jget(f"{ui_base}/api/v1/applications") or []
#         if not apps:
#             return out
#         app_id = apps[-1].get("id")

#     # 2) List jobs and filter to the target jobGroup (robust across Spark versions)
#     jobs = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs") or []
#     if not jobs:
#         # tiny retry helps immediately-after-action cases
#         for _ in range(3):
#             time.sleep(0.1)
#             jobs = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs") or []
#             if jobs:
#                 break

#     target_jobs = []
#     for j in jobs:
#         ok = (j.get("jobGroup") == job_group_id)
#         props = j.get("properties") or {}
#         ok = ok or (props.get("spark.jobGroup.id") == job_group_id)
#         if (not ok) and job_group_id in (j.get("description") or ""):
#             ok = True
#         if ok:
#             target_jobs.append(j)

#     # Fallback: use most recent job so we still return something
#     if not target_jobs and jobs:
#         target_jobs = [max(jobs, key=lambda x: x.get("jobId", -1))]
#     if not target_jobs:
#         return out

#     # 3) Gather stage IDs (prefer job['stageIds'], else derive from /stages by jobIds)
#     stage_ids = set()
#     for j in target_jobs:
#         for sid in (j.get("stageIds") or []):
#             if isinstance(sid, int):
#                 stage_ids.add(sid)

#     if not stage_ids:
#         all_stages = jget(f"{ui_base}/api/v1/applications/{app_id}/stages") or []
#         target_job_ids = {j.get("jobId") for j in target_jobs if isinstance(j.get("jobId"), int)}
#         for st in all_stages:
#             job_ids = set(st.get("jobIds") or [])
#             if job_ids & target_job_ids:
#                 sid = st.get("stageId")
#                 if isinstance(sid, int):
#                     stage_ids.add(sid)
#     if not stage_ids:
#         return out

#     # 4) Latest attempt's tasks per stage, then de-dup by (stageId, taskId)
#     seen = {}
#     have_tasks = False
#     for sid in stage_ids:
#         sd = jget(f"{ui_base}/api/v1/applications/{app_id}/stages/{sid}")
#         attempts = sd if isinstance(sd, list) else sd.get("attempts", [])
#         if not attempts:
#             attempts = [{"stageId": sid, "attemptId": 0}]
#         attempt_id = max(a.get("attemptId", 0) for a in attempts)

#         tl = jget(f"{ui_base}/api/v1/applications/{app_id}/stages/{sid}/{attempt_id}/taskList?length=100000") or []
#         if tl:
#             have_tasks = True
#         for t in tl:
#             key = (sid, t["taskId"])
#             prev = seen.get(key)
#             if prev is None:
#                 seen[key] = t
#                 continue
#             # Prefer SUCCESS; then higher attempt; then longer duration
#             ps, cs = prev.get("status"), t.get("status")
#             if ps != "SUCCESS" and cs == "SUCCESS":
#                 seen[key] = t
#             elif ps == cs:
#                 pa, ca = prev.get("attempt", 0), t.get("attempt", 0)
#                 if ca > pa:
#                     seen[key] = t
#                 elif ca == pa and (t.get("duration", 0) or 0) > (prev.get("duration", 0) or 0):
#                     seen[key] = t

#     if have_tasks:
#         total_bytes = total_exec_ms = total_cpu_ns = 0
#         for t in seen.values():
#             im = t.get("inputMetrics") or {}
#             total_bytes += int(im.get("bytesRead", 0))

#             tm = t.get("taskMetrics") or {}
#             # executorRunTime (ms)
#             if isinstance(t.get("executorRunTime"), int):
#                 total_exec_ms += t["executorRunTime"]
#             elif isinstance(tm.get("executorRunTime"), int):
#                 total_exec_ms += tm["executorRunTime"]
#             elif isinstance(tm.get("executorRunTimeMs"), int):
#                 total_exec_ms += tm["executorRunTimeMs"]
#             # executorCpuTime (ns)
#             if isinstance(t.get("executorCpuTime"), int):
#                 total_cpu_ns += t["executorCpuTime"]
#             elif isinstance(tm.get("executorCpuTime"), int):
#                 total_cpu_ns += tm["executorCpuTime"]

#         out["bytesRead"] = total_bytes
#         out["executorRunTime_ms"] = total_exec_ms
#         out["executorCpuTime_ns"] = total_cpu_ns
#         return out

#     # 5) Fallback: stage-level aggregates (latest attempt per stageId)
#     stages = jget(f"{ui_base}/api/v1/applications/{app_id}/stages") or []
#     latest = {}
#     for st in stages:
#         sid = st.get("stageId")
#         aid = st.get("attemptId", 0)
#         if sid in stage_ids and isinstance(sid, int):
#             if (sid not in latest) or (aid > latest[sid].get("attemptId", -1)):
#                 latest[sid] = st
#     for st in latest.values():
#         ib = st.get("inputBytes")
#         if isinstance(ib, int):
#             out["bytesRead"] += ib
#         er = st.get("executorRunTime")
#         if isinstance(er, int):
#             out["executorRunTime_ms"] += er
#         ec = st.get("executorCpuTime")
#         if isinstance(ec, int):
#             out["executorCpuTime_ns"] += ec
#     return out

# # ---------------- cache control ----------------
# def maybe_cache_table(spark, table, cache_mode):
#     """
#     Optionally cache the table:
#       - none: do nothing
#       - catalog: run 'CACHE TABLE <table>'
#       - df: materialize a DF of 'SELECT * FROM <table>' and persist(MEMORY_AND_DISK)
#     """
#     if cache_mode == "none":
#         return None
#     if cache_mode == "catalog":
#         spark.sql(f"CACHE TABLE {table}")
#         return None
#     elif cache_mode == "df":
#         df = spark.sql(f"SELECT * FROM {table}")
#         df.persist(StorageLevel.MEMORY_AND_DISK)
#         df.count()  # materialize
#         return df

# def maybe_uncache_table(spark, table, cache_obj, cache_mode):
#     """
#     Undo caching per the chosen mode.
#     """
#     if cache_mode == "none":
#         return
#     if cache_mode == "catalog":
#         spark.sql(f"UNCACHE TABLE {table}")
#     elif cache_mode == "df" and cache_obj is not None:
#         cache_obj.unpersist()

# # ---------------- single query runner ----------------
# def run_one_query(spark, sql_text, group_id, action) -> dict:
#     """
#     Execute a single query under a jobGroup and collect multiple bytes-read views:
#       - bytes_sql_exec: SQL execution inputBytes (preferred)
#       - bytes_plan_scans: sum of scan nodes' inputBytes/bytesRead/scanBytes
#       - bytes_rest_dedup: REST task-de-duped bytesRead
#       - bytes_input_files: sum of input file sizes (upper bound)
#       - elapsedTime_s
#       - executorRunTime_s (sum over tasks)
#       - executorCpuTime_s (sum over tasks)
#     """
#     sc = spark.sparkContext
#     sc.setJobGroup(group_id, f"query-{group_id}")
#     wall_start = time.time()
#     try:
#         df = spark.sql(sql_text)
#         if action == "count":
#             df.count()
#         elif action == "collect":
#             _ = df.collect()
#         elif action == "show":
#             df.show(5, truncate=False)
#         else:
#             df.count()
#     finally:
#         # clear job group regardless of success/failure
#         sc.setLocalProperty("spark.jobGroup.id", None)

#     wall_s = (time.time() - wall_start)

#     # Preferred bytes metrics (AFTER action)
#     b_sql  = bytes_sql_execution(spark)
#     b_plan = bytes_from_plan_scans(df)
#     b_files = bytes_sum_input_files(df)

#     # REST metrics (task dedup), if UI is available
#     ui = get_ui_base_url(spark)
#     bytes_rest = 0
#     exec_run_ms = 0
#     exec_cpu_ns = 0
#     if ui:
#         m = collect_metrics_via_rest(ui, group_id, spark) or {}
#         bytes_rest  = int(m.get("bytesRead", 0))
#         exec_run_ms = int(m.get("executorRunTime_ms", 0))
#         exec_cpu_ns = int(m.get("executorCpuTime_ns", 0))

#     # Convert times to seconds for output CSV
#     exec_run_s = exec_run_ms / 1000.0
#     exec_cpu_s = exec_cpu_ns / 1_000_000_000.0

#     return {
#         "bytes_sql_exec": int(b_sql) if b_sql is not None else -1,
#         "bytes_plan_scans": int(b_plan) if b_plan is not None else -1,
#         "bytes_rest_dedup": int(bytes_rest),
#         "bytes_input_files": int(b_files),
#         "elapsedTime_s": wall_s,
#         "executorRunTime_s": exec_run_s,
#         "executorCpuTime_s": exec_cpu_s,
#     }

# # ---------------- main ----------------
# def main():
#     ap = argparse.ArgumentParser("Unified query runner for Delta/Hudi/Iceberg")
#     ap.add_argument("--engine", required=True, choices=["delta", "hudi", "iceberg"])
#     ap.add_argument("--table", required=True, help="Table identifier (e.g., local.demo.table, delta.`/path`, or absolute path for Hudi)")
#     ap.add_argument("--queries_dir", default="queries", help="Directory containing .sql files with {{tbl}} placeholder")
#     ap.add_argument("--warmup", action="store_true", help="Run each query once as warm-up (discard metrics)")
#     ap.add_argument("--cache", choices=["none", "catalog", "df"], default="none", help="Caching mode")
#     ap.add_argument("--action", choices=["count", "collect", "show"], default="count", help="Spark action to materialize the query")
#     ap.add_argument("--output_csv", default="results.csv", help="Path to write metrics CSV")
#     ap.add_argument("--broadcast_hint", action="store_true", help="Enable broadcast join via conf")
#     args = ap.parse_args()

#     spark = get_spark(app_name=f"runner-{args.engine}")

#     # Optional engine-specific tweak
#     if args.broadcast_hint:
#         spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB

#     # Normalize the table argument into an SQL-usable identifier or a temp view
#     sql_table = prepare_table_view(spark, args.engine, args.table)

#     # Optional caching
#     cache_obj = maybe_cache_table(spark, sql_table, args.cache)

#     # Enumerate queries
#     qfiles = sorted(str(p) for p in pathlib.Path(args.queries_dir).glob("*.sql"))
#     if not qfiles:
#         raise SystemExit(f"No .sql files found in {args.queries_dir}")

#     # Run and record metrics
#     with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             "engine","query",
#             "bytes_sql_exec","bytes_plan_scans","bytes_rest_dedup","bytes_input_files",
#             "elapsedTime_s","executorRunTime_s","executorCpuTime_s"
#         ])
#         for qf in qfiles:
#             raw = read_sql(qf)
#             sql_text = substitute(raw, sql_table)

#             # warm-up run (optional)
#             if args.warmup:
#                 _ = run_one_query(spark, sql_text, f"warmup-{os.path.basename(qf)}", args.action)

#             # measured run
#             m = run_one_query(spark, sql_text, f"run-{os.path.basename(qf)}", args.action)
#             writer.writerow([
#                 args.engine,
#                 os.path.basename(qf),
#                 (m["bytes_sql_exec"] if m["bytes_sql_exec"] is not None else ""),
#                 (m["bytes_plan_scans"] if m["bytes_plan_scans"] is not None else ""),
#                 m["bytes_rest_dedup"],
#                 m["bytes_input_files"],
#                 f"{m['elapsedTime_s']:.3f}",
#                 f"{m['executorRunTime_s']:.3f}",
#                 f"{m['executorCpuTime_s']:.3f}",
#             ])

#     # Cleanup
#     maybe_uncache_table(spark, sql_table, cache_obj, args.cache)
#     spark.stop()
#     print(f"[OK] wrote {args.output_csv}")

# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, time, json, urllib.request, pathlib, csv, re, sys, shutil, platform
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# ==============================
# Basic helpers
# ==============================
def read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def substitute(sql: str, table: str) -> str:
    return sql.replace("{{tbl}}", table)

def write_text(p: pathlib.Path, content: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write(content)

def write_json(p: pathlib.Path, obj: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)

# ==============================
# Spark session + UI
# ==============================
def _default_eventlog_dir_local_path() -> pathlib.Path:
    return (pathlib.Path.cwd() / "spark_eventlogs")

def _default_eventlog_dir() -> str:
    return f"file://{_default_eventlog_dir_local_path().as_posix()}"

def _ensure_eventlog_dir_exists():
    try:
        p = _default_eventlog_dir_local_path()
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

def get_spark(app_name: str) -> SparkSession:
    """
    Build SparkSession with UI + SQL UI + Event Log enabled and sane defaults.
    Retention is increased to avoid losing jobs/stages before REST polling.
    """
    _ensure_eventlog_dir_exists()
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.ui.enabled", os.getenv("SPARK_UI_ENABLED", "true"))
        .config("spark.sql.ui.enabled", os.getenv("SPARK_SQL_UI_ENABLED", "true"))
        .config("spark.eventLog.enabled", os.getenv("SPARK_EVENTLOG_ENABLED", "true"))
        .config("spark.eventLog.dir", os.getenv("SPARK_EVENTLOG_DIR", _default_eventlog_dir()))
        .config("spark.sql.adaptive.enabled", os.getenv("ADAPTIVE", "true"))
        .config("spark.sql.files.maxPartitionBytes", os.getenv("MAX_PART_BYTES", "256m"))
        .config("spark.sql.parquet.enableVectorizedReader", os.getenv("VEC_READER", "true"))
        .config("spark.sql.parquet.cacheMetadata", "false")
        .config("spark.databricks.io.cache.enabled", "false")
        # keep more entries in UI to survive short polling windows
        .config("spark.ui.retainedJobs", "2000")
        .config("spark.ui.retainedStages", "2000")
        .config("spark.sql.ui.retainedExecutions", "2000")
        .getOrCreate()
    )

def get_ui_base_url(spark: SparkSession):
    """Return Spark UI base URL if available, otherwise None."""
    try:
        return spark._jsc.sc().uiWebUrl().get()
    except Exception:
        return None

def http_json(url: str):
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))

# ==============================
# Table prep
# ==============================
def _strip_outer_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def _looks_like_path(s: str) -> bool:
    ss = s.strip()
    return ss.startswith("/") or ss.startswith("file:")

def prepare_table_view(spark: SparkSession, engine: str, table_arg: str) -> str:
    """
    Normalize --table so {{tbl}} is usable across engines.

    Hudi: path => DataFrame.load => temp view "_tbl"
    Delta: path => delta.`/path`; catalog id => keep
    Iceberg: catalog id only (paths not supported)
    """
    t = _strip_outer_quotes(table_arg)
    eng = engine.lower()

    if eng == "hudi":
        path = None
        if t.startswith("hudi.`") and t.endswith("`"):
            path = t[len("hudi.`"):-1]
        elif t.startswith("hudi.'") and t.endswith("'"):
            path = t[len("hudi.'"):-1]
        elif _looks_like_path(t):
            path = t
        if path:
            df = spark.read.format("hudi").load(path)
            view = "_tbl"
            try:
                spark.catalog.dropTempView(view)
            except Exception:
                pass
            df.createOrReplaceTempView(view)
            return view
        else:
            return t

    if eng == "delta":
        if re.match(r"^delta\.\s*`.+`\s*$", t) or re.match(r'^delta\.\s*".+"\s*$', t):
            return t
        if _looks_like_path(t):
            return f"delta.`{t}`"
        return t

    if eng == "iceberg":
        if _looks_like_path(t):
            raise SystemExit("For Iceberg, --table must be a catalog identifier like catalog.db.tbl (paths are not supported).")
        return t

    return t

# ==============================
# REST app selection
# ==============================
def choose_app_id(ui_base: str, spark: SparkSession):
    """
    Choose /api/v1/applications/<appId> that matches this SparkSession.
    Priority: applicationId -> exact match; otherwise last resort: newest by attempts.startTime.
    """
    def jget(url: str):
        with urllib.request.urlopen(url) as r:
            return json.loads(r.read().decode("utf-8"))

    try:
        apps = jget(f"{ui_base}/api/v1/applications") or []
        if not apps:
            return None

        desired_id = getattr(spark.sparkContext, "applicationId", None)
        if desired_id:
            for a in apps:
                if a.get("id") == desired_id or (desired_id in (a.get("id") or "")):
                    return a.get("id")

        # fallback: newest by start time
        def _start_time(app):
            atts = app.get("attempts") or []
            if atts and isinstance(atts[0], dict):
                return atts[0].get("startTime") or ""
            return ""
        try:
            apps_sorted = sorted(apps, key=_start_time)
            return apps_sorted[-1].get("id")
        except Exception:
            return apps[-1].get("id")
    except Exception:
        return None

# ==============================
# Plan scan metrics
# ==============================
def _to_int(x):
    try:
        return int(x)
    except Exception:
        return int(str(x))

def collect_scan_metrics(df) -> dict:
    """
    Aggregate scan-node metrics (executed plan).
    Returns:
      - numFiles (files_scanned)
      - bytesRead (bytes_scanned)
    If not available, returns -1 for the field.
    """
    want = {
        "numFiles": ["numFiles", "filesRead", "numberOfFiles", "number of files read", "number of file splits read"],
        "bytesRead": ["inputBytes", "bytesRead", "scanBytes", "sizeOfFilesRead", "input bytes"]
    }
    agg = {k: 0 for k in want}
    seen_any = {k: False for k in want}
    try:
        jvm = df._sc._jvm
        plan = df._jdf.queryExecution().executedPlan()
        IsScan = type(
            "IsScan",
            (jvm.scala.runtime.AbstractFunction1,),
            {
                "apply": lambda self, p: (
                    "scan" in p.nodeName().lower()
                    or "filesource" in p.nodeName().lower()
                    or "filescan" in p.getClass().getName().lower()
                    or "batchscan" in p.getClass().getName().lower()
                )
            },
        )
        scans = plan.collect(IsScan())
        it = scans.iterator()
        while it.hasNext():
            n = it.next()
            m = n.metrics()
            for out_key, aliases in want.items():
                for a in aliases:
                    if m.contains(a):
                        agg[out_key] += _to_int(m.apply(a).value())
                        seen_any[out_key] = True
                        break
        for k in agg:
            if not seen_any[k]:
                agg[k] = -1
        return agg
    except Exception:
        return {k: -1 for k in want}

# ==============================
# File-size upper bound
# ==============================
def bytes_sum_input_files(df) -> int:
    """
    Upper bound: sum Hadoop FS lengths for df.inputFiles() (compressed + unpruned).
    """
    try:
        files = list(df.inputFiles())
    except Exception:
        files = []
    if not files:
        return 0
    try:
        jvm = df._sc._jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(df._sc._jsc.hadoopConfiguration())
        Path = jvm.org.apache.hadoop.fs.Path
        total = 0
        for f in files:
            try:
                total += int(fs.getFileStatus(Path(f)).getLen())
            except Exception:
                pass
        return total
    except Exception:
        return 0

# ---------------- metrics via REST ----------------
def collect_metrics_via_rest(ui_base: str, spark: SparkSession, job_group_id: str,
                              poll_ms_total: int = 1000, poll_interval_ms: int = 200) -> dict:
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
# # ==============================
# # REST metrics (task-level first, stage fallback) with polling
# # ==============================
# def collect_metrics_via_rest(ui_base: str, spark: SparkSession, job_group_id: str,
#                              poll_ms_total: int = 1000, poll_interval_ms: int = 200) -> dict:
#     """
#     Collect metrics for a given jobGroup:
#       - Sum inputMetrics.bytesRead across deduped tasks (preferred)
#       - Fallback to stage.inputBytes
#       - Also sum executorRunTime (ms) and executorCpuTime (ns)
#     Light polling helps when UI lags right after the action finishes.
#     """
#     def jget(url: str):
#         with urllib.request.urlopen(url) as r:
#             return json.loads(r.read().decode("utf-8"))

#     out = {"bytesRead": 0, "executorRunTime_ms": 0, "executorCpuTime_ns": 0}

#     try:
#         app_id = choose_app_id(ui_base, spark)
#         if not app_id:
#             return out

#         # Poll until our jobGroup appears (or timeout)
#         deadline = time.time() + (poll_ms_total / 1000.0)
#         jobs = []
#         while time.time() < deadline:
#             jobs = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs") or []
#             found = False
#             for j in jobs:
#                 if (j.get("jobGroup") == job_group_id):
#                     found = True; break
#                 props = j.get("properties") or {}
#                 if props.get("spark.jobGroup.id") == job_group_id:
#                     found = True; break
#                 if job_group_id in (j.get("description") or ""):
#                     found = True; break
#             if found:
#                 break
#             time.sleep(poll_interval_ms / 1000.0)
#         if not jobs:
#             jobs = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs") or []

#         # Select all jobs that match our group id
#         target_jobs = []
#         for j in jobs:
#             ok = (j.get("jobGroup") == job_group_id)
#             props = j.get("properties") or {}
#             ok = ok or (props.get("spark.jobGroup.id") == job_group_id)
#             if (not ok) and job_group_id in (j.get("description") or ""):
#                 ok = True
#             if ok:
#                 target_jobs.append(j)
#         if not target_jobs:
#             # best-effort fallback: most recent job
#             if jobs:
#                 target_jobs = [max(jobs, key=lambda x: x.get("jobId", -1))]
#             else:
#                 return out

#         # Collect stage ids
#         stage_ids = set()
#         for j in target_jobs:
#             for sid in (j.get("stageIds") or []):
#                 if isinstance(sid, int):
#                     stage_ids.add(sid)
#         if not stage_ids:
#             # Some Spark builds require fetching details to see stage mapping
#             for j in target_jobs:
#                 jid = j.get("jobId")
#                 if isinstance(jid, int):
#                     jd = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs/{jid}") or {}
#                     for st in (jd.get("stages") or []):
#                         sid = st.get("stageId")
#                         if isinstance(sid, int):
#                             stage_ids.add(sid)
#         if not stage_ids:
#             return out

#         # For each stage: get latest attempt's taskList and dedup by (stageId, taskId)
#         seen, have_tasks = {}, False
#         for sid in stage_ids:
#             sd = jget(f"{ui_base}/api/v1/applications/{app_id}/stages/{sid}")
#             attempts = sd if isinstance(sd, list) else sd.get("attempts", [])
#             if not attempts:
#                 attempts = [{"stageId": sid, "attemptId": 0}]
#             attempt_id = max(a.get("attemptId", 0) for a in attempts)

#             tl = jget(f"{ui_base}/api/v1/applications/{app_id}/stages/{sid}/{attempt_id}/taskList?length=100000") or []
#             if tl:
#                 have_tasks = True
#             for t in tl:
#                 key = (sid, t["taskId"])
#                 prev = seen.get(key)
#                 if prev is None:
#                     seen[key] = t
#                 else:
#                     # prefer SUCCESS; else higher attempt; else longer duration
#                     ps, cs = prev.get("status"), t.get("status")
#                     if ps != "SUCCESS" and cs == "SUCCESS":
#                         seen[key] = t
#                     elif ps == cs:
#                         pa, ca = prev.get("attempt", 0), t.get("attempt", 0)
#                         if ca > pa:
#                             seen[key] = t
#                         elif ca == pa and (t.get("duration", 0) or 0) > (prev.get("duration", 0) or 0):
#                             seen[key] = t

#         if have_tasks:
#             total_bytes = total_exec_ms = total_cpu_ns = 0
#             for t in seen.values():
#                 im = t.get("inputMetrics") or {}
#                 total_bytes += int(im.get("bytesRead", 0))
#                 tm = t.get("taskMetrics") or {}
#                 if isinstance(t.get("executorRunTime"), int):
#                     total_exec_ms += t["executorRunTime"]
#                 elif isinstance(tm.get("executorRunTime"), int):
#                     total_exec_ms += tm["executorRunTime"]
#                 elif isinstance(tm.get("executorRunTimeMs"), int):
#                     total_exec_ms += tm["executorRunTimeMs"]
#                 if isinstance(t.get("executorCpuTime"), int):
#                     total_cpu_ns += t["executorCpuTime"]
#                 elif isinstance(tm.get("executorCpuTime"), int):
#                     total_cpu_ns += tm["executorCpuTime"]
#             out["bytesRead"] = total_bytes
#             out["executorRunTime_ms"] = total_exec_ms
#             out["executorCpuTime_ns"] = total_cpu_ns
#             return out

#         # Stage-aggregate fallback (sum inputBytes/executorRunTime/executorCpuTime on latest attempts)
#         stages = jget(f"{ui_base}/api/v1/applications/{app_id}/stages") or []
#         latest = {}
#         for st in stages:
#             sid = st.get("stageId")
#             aid = st.get("attemptId", 0)
#             if sid in stage_ids and isinstance(sid, int):
#                 if (sid not in latest) or (aid > latest[sid].get("attemptId", -1)):
#                     latest[sid] = st
#         for st in latest.values():
#             ib = st.get("inputBytes")
#             if isinstance(ib, int): out["bytesRead"] += ib
#             er = st.get("executorRunTime")
#             if isinstance(er, int): out["executorRunTime_ms"] += er
#             ec = st.get("executorCpuTime")
#             if isinstance(ec, int): out["executorCpuTime_ns"] += ec
#         return out
#     except Exception:
#         return out

# ==============================
# Cache control
# ==============================
def pre_query_uncache(spark: SparkSession):
    try:
        spark.catalog.clearCache()
    except Exception:
        pass

def maybe_cache_table(spark, table, cache_mode):
    if cache_mode == "none":
        return None
    if cache_mode == "catalog":
        spark.sql(f"CACHE TABLE {table}")
        return None
    elif cache_mode == "df":
        df = spark.sql(f"SELECT * FROM {table}")
        df.persist(StorageLevel.MEMORY_AND_DISK)
        df.count()
        return df

def maybe_uncache_table(spark, table, cache_obj, cache_mode):
    if cache_mode == "none":
        return
    if cache_mode == "catalog":
        spark.sql(f"UNCACHE TABLE {table}")
    elif cache_mode == "df" and cache_obj is not None:
        cache_obj.unpersist()

# ==============================
# Event logs export (optional)
# ==============================
def export_event_logs(spark: SparkSession, logs_root: pathlib.Path):
    try:
        app_id = getattr(spark.sparkContext, "applicationId", None)
        ev_dir_conf = spark.conf.get("spark.eventLog.dir", _default_eventlog_dir())
        ev_dir = pathlib.Path(ev_dir_conf.replace("file://", ""))
        if not ev_dir.exists():
            return
        target = logs_root / "spark_eventlogs"
        target.mkdir(parents=True, exist_ok=True)

        matched = []
        for f in ev_dir.glob("*"):
            name = f.name
            if app_id and (app_id in name or name.endswith(app_id) or name.startswith(app_id)):
                matched.append(f)
        if not matched:
            latest = sorted(ev_dir.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)[:3]
            matched = latest

        for src in matched:
            dst = target / src.name
            try:
                shutil.copy2(src, dst)
            except Exception:
                shutil.copy(src, dst)

        write_json(target / "INDEX.json", {
            "applicationId": app_id or "",
            "eventLogDir": str(ev_dir),
            "copied": [p.name for p in matched],
        })
    except Exception:
        pass

# ==============================
# Single query runner
# ==============================
def run_one_query(
    spark: SparkSession,
    sql_text: str,
    group_id: str,
    action: str,
    q_log_dir: pathlib.Path,
    rest_wait_ms: int,
    rest_poll_ms: int,
) -> dict:
    """
    Execute a single SQL under a jobGroup and return:
      - bytesRead (from REST tasks; fallback to stages)
      - elapsedTime_s
      - executorRunTime_s / executorCpuTime_s (from REST)
      - bytes_input_files (sum of df.inputFiles() lengths)
      - files_scanned / bytes_scanned (from executed plan scan metrics)
    """
    write_text(q_log_dir / "query.sql", sql_text)
    pre_query_uncache(spark)

    sc = spark.sparkContext
    sc.setJobGroup(group_id, f"query-{group_id}")
    sc.setLocalProperty("spark.job.description", f"query-{group_id}")
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
        sc.setLocalProperty("spark.job.description", None)

    wall_s = (time.time() - wall_start)

    # Scan metrics (executed plan)
    scan = collect_scan_metrics(df)
    files_scanned = 0 if scan.get("numFiles", -1) in (-1, None) else int(scan["numFiles"])
    bytes_scanned = 0 if scan.get("bytesRead", -1) in (-1, None) else int(scan["bytesRead"])

    # Upper bound from input files
    bytes_input_files = bytes_sum_input_files(df)

    # REST metrics (task-level first, stage fallback)
    ui = get_ui_base_url(spark)
    rest = {"bytesRead": 0, "executorRunTime_ms": 0, "executorCpuTime_ns": 0}
    if ui:
        rest = collect_metrics_via_rest(ui, spark, group_id,
                                        poll_ms_total=rest_wait_ms,
                                        poll_interval_ms=rest_poll_ms) or rest
        # if the query is ultra-fast, give the UI a very short extra breath
        if rest.get("bytesRead", 0) == 0:
            time.sleep(min(0.3, rest_wait_ms / 1000.0))
            again = collect_metrics_via_rest(ui, spark, group_id,
                                             poll_ms_total=rest_wait_ms // 2,
                                             poll_interval_ms=rest_poll_ms) or {}
            # keep the larger value
            rest["bytesRead"] = max(int(rest.get("bytesRead", 0)), int(again.get("bytesRead", 0)))
            rest["executorRunTime_ms"] += int(again.get("executorRunTime_ms", 0))
            rest["executorCpuTime_ns"] += int(again.get("executorCpuTime_ns", 0))

    # Convert to seconds for CSV
    exec_run_s = (rest.get("executorRunTime_ms", 0) or 0) / 1000.0
    exec_cpu_s = (rest.get("executorCpuTime_ns", 0) or 0) / 1_000_000_000.0

    out = {
        "bytesRead": int(rest.get("bytesRead", 0) or 0),
        "elapsedTime_s": wall_s,
        "executorRunTime_s": exec_run_s,
        "executorCpuTime_s": exec_cpu_s,
        "bytes_input_files": int(bytes_input_files),
        "files_scanned": int(files_scanned),
        "bytes_scanned": int(bytes_scanned),
    }
    write_json(q_log_dir / "metrics.min.json", out)
    pre_query_uncache(spark)
    return out

# ==============================
# Main
# ==============================
def derive_logs_root_from_output(output_csv: str) -> pathlib.Path:
    out_dir = pathlib.Path(output_csv).parent
    parts = list(out_dir.parts)
    if "results" in parts:
        idx = parts.index("results")
        rel_after_results = pathlib.Path(*parts[idx+1:])
        root = pathlib.Path("log") / rel_after_results
    else:
        try:
            rel = out_dir.relative_to(pathlib.Path.cwd())
        except Exception:
            rel = out_dir
        root = pathlib.Path("log") / rel
    root.mkdir(parents=True, exist_ok=True)
    return root

def main():
    ap = argparse.ArgumentParser("Unified query runner for Delta/Hudi/Iceberg (stable bytesRead)")
    ap.add_argument("--engine", required=True, choices=["delta", "hudi", "iceberg"])
    ap.add_argument("--table", required=True, help="Table (catalog id, delta.`/path`, or absolute path for Hudi)")
    ap.add_argument("--queries_dir", default="queries", help="Dir of .sql files with {{tbl}}")
    ap.add_argument("--warmup", action="store_true", help="Warm up each query (discard)")
    ap.add_argument("--repeat", type=int, default=5, help="Repeat each query N times and average metrics")
    ap.add_argument("--cache", choices=["none", "catalog", "df"], default="none", help="Caching mode")
    ap.add_argument("--action", choices=["count", "collect", "show"], default="collect", help="Spark action")
    ap.add_argument("--output_csv", default="results.csv", help="CSV path")
    ap.add_argument("--broadcast_hint", action="store_true", help="Enable broadcast join (50MB)")
    ap.add_argument("--output_log", default="", help="Log root; if empty, mirror results/ under log/")
    ap.add_argument("--rest_wait_ms", type=int, default=1200, help="Total wait for REST polling (ms)")
    ap.add_argument("--rest_poll_ms", type=int, default=200, help="REST poll interval (ms)")
    args = ap.parse_args()

    spark = get_spark(app_name=f"runner-{args.engine}")

    if args.broadcast_hint:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)

    sql_table = prepare_table_view(spark, args.engine, args.table)

    cache_obj = maybe_cache_table(spark, sql_table, args.cache)

    qfiles = sorted(str(p) for p in pathlib.Path(args.queries_dir).glob("*.sql"))
    if not qfiles:
        raise SystemExit(f"No .sql files found in {args.queries_dir}")

    logs_root = pathlib.Path(args.output_log) if args.output_log else derive_logs_root_from_output(args.output_csv)
    logs_root.mkdir(parents=True, exist_ok=True)

    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "engine","query",
            "bytesRead","elapsedTime_s","executorRunTime_s","executorCpuTime_s",
            "bytes_input_files","files_scanned","bytes_scanned",
        ])
        for qf in qfiles:
            raw = read_sql(qf)
            sql_text = substitute(raw, sql_table)

            q_log_dir = logs_root / f"{args.engine}__{os.path.basename(qf)}"
            q_log_dir.mkdir(parents=True, exist_ok=True)

            if args.warmup:
                _ = run_one_query(
                    spark, sql_text, f"warmup-{os.path.basename(qf)}", args.action,
                    q_log_dir, args.rest_wait_ms, args.rest_poll_ms
                )

            keys = [
                "bytesRead", "elapsedTime_s", "executorRunTime_s", "executorCpuTime_s",
                "bytes_input_files", "files_scanned", "bytes_scanned",
            ]
            runs = []

            for i in range(args.repeat):
                rep_dir = q_log_dir / f"rep{i+1}"
                rep_dir.mkdir(parents=True, exist_ok=True)
                gid = f"run-{os.path.basename(qf)}-rep{i+1}"

                # res = run_one_query(
                #     spark, sql_text, gid, args.action,
                #     rep_dir, args.engine, sql_table,
                #     rest_poll_ms_total=args.rest_wait_ms,
                #     rest_poll_interval_ms=args.rest_poll_ms
                # )
                res = run_one_query(spark, sql_text, gid, args.action, q_log_dir, 
                                    args.rest_wait_ms, args.rest_poll_ms)
                runs.append(res)

            avg = {k: (sum(float(r[k]) for r in runs) / len(runs)) for k in keys}

            writer.writerow([
                args.engine,
                os.path.basename(qf),
                int(round(avg["bytesRead"])),           
                f"{avg['elapsedTime_s']:.3f}",
                f"{avg['executorRunTime_s']:.3f}",
                f"{avg['executorCpuTime_s']:.3f}",
                int(round(avg["bytes_input_files"])),
                int(round(avg["files_scanned"])),
                int(round(avg["bytes_scanned"])),
            ])

            # m = run_one_query(
            #     spark, sql_text, f"run-{os.path.basename(qf)}", args.action,
            #     q_log_dir, args.rest_wait_ms, args.rest_poll_ms
            # )
            # writer.writerow([
            #     args.engine,
            #     os.path.basename(qf),
            #     m["bytesRead"],
            #     f"{m['elapsedTime_s']:.3f}",
            #     f"{m['executorRunTime_s']:.3f}",
            #     f"{m['executorCpuTime_s']:.3f}",
            #     m["bytes_input_files"],
            #     m["files_scanned"],
            #     m["bytes_scanned"],
            # ])

    export_event_logs(spark, logs_root)
    maybe_uncache_table(spark, sql_table, cache_obj, args.cache)
    spark.stop()
    print(f"[OK] wrote {args.output_csv}\n[OK] logs at {logs_root}")

if __name__ == "__main__":
    main()
