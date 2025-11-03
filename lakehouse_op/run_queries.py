#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, time, json, urllib.request, pathlib, csv, re, sys, shutil, gzip
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# ---------------- basic io helpers ----------------
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

# ---------------- spark session ----------------
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
    Also retain more entries to survive short REST polling.
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

# ---------------- table normalization ----------------
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
            try: spark.catalog.dropTempView(view)
            except Exception: pass
            df.createOrReplaceTempView(view)
            return view
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

# ---------------- app selection ----------------
def choose_app_id(ui_base: str, spark: SparkSession):
    """
    Pick /api/v1/applications/<appId> for this SparkSession.
    Priority: sparkContext.applicationId -> else newest by attempts.startTime.
    """
    def jget(url: str):
        with urllib.request.urlopen(url) as r:
            return json.loads(r.read().decode("utf-8"))
    try:
        apps = jget(f"{ui_base}/api/v1/applications") or []
        if not apps: return None
        desired_id = getattr(spark.sparkContext, "applicationId", None)
        if desired_id:
            for a in apps:
                if a.get("id") == desired_id or (desired_id in (a.get("id") or "")):
                    return a.get("id")
        # fallback: newest
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

# ---------------- plan scan metrics (best-effort) ----------------
def _to_int(x):
    try:
        return int(x)
    except Exception:
        return int(str(x))

def collect_scan_metrics(df) -> dict:
    """
    Aggregate scan-node metrics from executed plan (may be missing on some sources).
    Returns numFiles (files_scanned) and bytesRead (bytes_scanned). -1 means missing.
    """
    want = {
        "numFiles": ["numFiles", "filesRead", "numberOfFiles", "number of files read", "number of file splits read"],
        "bytesRead": ["inputBytes", "bytesRead", "scanBytes", "sizeOfFilesRead", "input bytes"]
    }
    agg, seen_any = {k:0 for k in want}, {k:False for k in want}
    try:
        jvm = df._sc._jvm
        plan = df._jdf.queryExecution().executedPlan()
        IsScan = type(
            "IsScan",
            (jvm.scala.runtime.AbstractFunction1,),
            {"apply": lambda self, p: ("scan" in p.nodeName().lower()
                                       or "filesource" in p.nodeName().lower()
                                       or "filescan" in p.getClass().getName().lower()
                                       or "batchscan" in p.getClass().getName().lower())},
        )
        scans = plan.collect(IsScan()).iterator()
        while scans.hasNext():
            n = scans.next()
            m = n.metrics()
            for out_key, aliases in want.items():
                for a in aliases:
                    if m.contains(a):
                        agg[out_key] += _to_int(m.apply(a).value())
                        seen_any[out_key] = True
                        break
        for k in agg:
            if not seen_any[k]: agg[k] = -1
        return agg
    except Exception:
        return {k:-1 for k in want}

# ---------------- file upper bound via inputFiles() ----------------
def bytes_sum_input_files(df) -> int:
    """
    Sum Hadoop FS lengths for df.inputFiles(); 0 if source does not expose file paths.
    """
    try:
        files = list(df.inputFiles())
    except Exception:
        files = []
    if not files: return 0
    try:
        jvm = df._sc._jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(df._sc._jsc.hadoopConfiguration())
        Path = jvm.org.apache.hadoop.fs.Path
        total = 0
        for f in files:
            try: total += int(fs.getFileStatus(Path(f)).getLen())
            except Exception: pass
        return total
    except Exception:
        return 0

# ---------------- hard fallback: enumerate actual files via input_file_name() ----------------
def enumerate_scanned_files_and_bytes(spark: SparkSession, sql_text: str) -> tuple[int, int]:
    """
    Robust fallback: re-run a light probe that enumerates actual files used by the query.
    Implementation:
      SELECT DISTINCT input_file_name() AS f FROM ( <original SQL> ) q
    Then sum file sizes via Hadoop FS.
    This guarantees non-empty files/bytes if the original query touched files at all.
    """
    probe_sql = f"SELECT DISTINCT input_file_name() AS f FROM ( {sql_text} ) q"
    try:
        df_files = spark.sql(probe_sql)
        files = [r["f"] for r in df_files.collect() if r["f"]]
        if not files:
            return 0, 0
        jvm = spark._jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        Path = jvm.org.apache.hadoop.fs.Path
        total = 0
        for f in files:
            try: total += int(fs.getFileStatus(Path(f)).getLen())
            except Exception: pass
        return len(files), total
    except Exception:
        return 0, 0

# ---------------- REST metrics with task dedup ----------------
def collect_metrics_via_rest(ui_base: str, spark: SparkSession, job_group_id: str,
                             poll_ms_total: int = 1200, poll_interval_ms: int = 200) -> dict:
    """
    Preferred path: sum inputMetrics.bytesRead across deduped tasks for given jobGroup.
    Fallback: sum stage.inputBytes. Also returns executorRunTime_ms and executorCpuTime_ns.
    """
    def jget(url: str):
        with urllib.request.urlopen(url) as r:
            return json.loads(r.read().decode("utf-8"))
    out = {"bytesRead": 0, "executorRunTime_ms": 0, "executorCpuTime_ns": 0}
    try:
        app_id = choose_app_id(ui_base, spark)
        if not app_id: return out

        # poll until the jobGroup is visible
        deadline = time.time() + (poll_ms_total / 1000.0)
        jobs = []
        while time.time() < deadline:
            jobs = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs") or []
            if any(
                (j.get("jobGroup") == job_group_id)
                or ((j.get("properties") or {}).get("spark.jobGroup.id") == job_group_id)
                or (job_group_id in (j.get("description") or ""))
                for j in jobs
            ):
                break
            time.sleep(poll_interval_ms / 1000.0)
        if not jobs:
            jobs = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs") or []

        # pick jobs of this group
        target_jobs = []
        for j in jobs:
            ok = (j.get("jobGroup") == job_group_id)
            props = j.get("properties") or {}
            ok = ok or (props.get("spark.jobGroup.id") == job_group_id)
            if (not ok) and job_group_id in (j.get("description") or ""):
                ok = True
            if ok: target_jobs.append(j)
        if not target_jobs:
            if jobs: target_jobs = [max(jobs, key=lambda x: x.get("jobId", -1))]
            else: return out

        # collect stage ids
        stage_ids = set()
        for j in target_jobs:
            for sid in (j.get("stageIds") or []):
                if isinstance(sid, int): stage_ids.add(sid)
        if not stage_ids:
            for j in target_jobs:
                jid = j.get("jobId")
                if isinstance(jid, int):
                    jd = jget(f"{ui_base}/api/v1/applications/{app_id}/jobs/{jid}") or {}
                    for st in (jd.get("stages") or []):
                        sid = st.get("stageId")
                        if isinstance(sid, int): stage_ids.add(sid)
        if not stage_ids: return out

        # tasks (dedup by (stageId, taskId))
        seen, have_tasks = {}, False
        for sid in stage_ids:
            sd = jget(f"{ui_base}/api/v1/applications/{app_id}/stages/{sid}")
            attempts = sd if isinstance(sd, list) else sd.get("attempts", [])
            if not attempts: attempts = [{"stageId": sid, "attemptId": 0}]
            attempt_id = max(a.get("attemptId", 0) for a in attempts)
            tl = jget(f"{ui_base}/api/v1/applications/{app_id}/stages/{sid}/{attempt_id}/taskList?length=100000") or []
            if tl: have_tasks = True
            for t in tl:
                key = (sid, t["taskId"])
                prev = seen.get(key)
                if prev is None:
                    seen[key] = t
                else:
                    ps, cs = prev.get("status"), t.get("status")
                    if ps != "SUCCESS" and cs == "SUCCESS":
                        seen[key] = t
                    elif ps == cs:
                        pa, ca = prev.get("attempt", 0), t.get("attempt", 0)
                        if ca > pa:
                            seen[key] = t
                        elif ca == pa and (t.get("duration", 0) or 0) > (prev.get("duration", 0) or 0):
                            seen[key] = t

        if have_tasks:
            total_bytes = total_exec_ms = total_cpu_ns = 0
            for t in seen.values():
                im = t.get("inputMetrics") or {}
                total_bytes += int(im.get("bytesRead", 0))
                tm = t.get("taskMetrics") or {}
                if isinstance(t.get("executorRunTime"), int):
                    total_exec_ms += t["executorRunTime"]
                elif isinstance(tm.get("executorRunTime"), int):
                    total_exec_ms += tm["executorRunTime"]
                elif isinstance(tm.get("executorRunTimeMs"), int):
                    total_exec_ms += tm["executorRunTimeMs"]
                if isinstance(t.get("executorCpuTime"), int):
                    total_cpu_ns += t["executorCpuTime"]
                elif isinstance(tm.get("executorCpuTime"), int):
                    total_cpu_ns += tm["executorCpuTime"]
            out["bytesRead"] = total_bytes
            out["executorRunTime_ms"] = total_exec_ms
            out["executorCpuTime_ns"] = total_cpu_ns
            return out

        # stage fallback
        stages = jget(f"{ui_base}/api/v1/applications/{app_id}/stages") or []
        latest = {}
        for st in stages:
            sid = st.get("stageId"); aid = st.get("attemptId", 0)
            if sid in stage_ids and isinstance(sid, int):
                if (sid not in latest) or (aid > latest[sid].get("attemptId", -1)):
                    latest[sid] = st
        for st in latest.values():
            if isinstance(st.get("inputBytes"), int): out["bytesRead"] += st["inputBytes"]
            if isinstance(st.get("executorRunTime"), int): out["executorRunTime_ms"] += st["executorRunTime"]
            if isinstance(st.get("executorCpuTime"), int): out["executorCpuTime_ns"] += st["executorCpuTime"]
        return out
    except Exception:
        return out

# ---------------- eventlog parsing (standalone) ----------------
def _open_eventlog(path: pathlib.Path):
    """
    Open a Spark event log file transparently (supports .gz).
    """
    name = path.name
    if name.endswith(".gz") or ".gz" in name:
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")

def _find_eventlog_file(spark) -> pathlib.Path | None:
    """
    Locate current application's eventlog file under spark.eventLog.dir.
    """
    app_id = getattr(spark.sparkContext, "applicationId", None)
    try:
        ev_dir_conf = spark.conf.get("spark.eventLog.dir", "")
    except Exception:
        ev_dir_conf = ""
    ev_dir = pathlib.Path(ev_dir_conf.replace("file://", ""))
    if not app_id or not ev_dir.exists():
        return None
    cands = sorted(ev_dir.glob(f"*{app_id}*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

def _acc_val(accs, names: tuple[str, ...]) -> int:
    """
    Read an integer value from task "Accumulables" by any of the given names.
    """
    for a in accs or []:
        nm = a.get("Name", "")
        if nm in names:
            v = a.get("Value")
            try:
                return int(v)
            except Exception:
                try:
                    return int(float(v))
                except Exception:
                    pass
    return 0

def parse_eventlog_for_group(spark, group_id: str, t0_ms: int, t1_ms: int) -> dict:
    """
    Aggregate bytes and counts for a specific query window from SparkListenerTaskEnd.

    Selection:
      - Only consider events whose task finish time is within [t0_ms, t1_ms].
      - Additionally, we pre-collect Stage IDs by scanning JobStart/SQLExecutionStart
        events whose description or jobGroup matches `group_id`. This further narrows
        down tasks to the ones belonging to this query.
      - If no Stage IDs are found (rare), we fall back to time window only.

    Metrics:
      - bytesRead_ev: internal.metrics.input.bytesRead (preferred), else Task Metrics.Input Metrics.Bytes Read
      - files_scanned_ev: "number of files read" or internal.metrics.input.filesRead
      - bytes_scanned_ev: equals bytesRead_ev under file-scan semantics
      - executorRunTime_ms_ev / executorCpuTime_ns_ev: from internal accumulables if present
    """
    f = _find_eventlog_file(spark)
    if not f:
        return {
            "bytesRead_ev": 0,
            "files_scanned_ev": 0,
            "bytes_scanned_ev": 0,
            "executorRunTime_ms_ev": 0,
            "executorCpuTime_ns_ev": 0,
        }

    stage_ids = set()
    gid_pat = re.compile(re.escape(group_id))

    # Pass 1: collect stage IDs for this group within the time window
    with _open_eventlog(f) as fp:
        for line in fp:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            ts = ev.get("Timestamp") or ev.get("EventTime")
            if not isinstance(ts, int) or ts < t0_ms or ts > t1_ms:
                continue
            et = ev.get("Event")

            if et == "SparkListenerJobStart":
                props = ev.get("Properties") or {}
                desc = props.get("spark.job.description", "")
                if (props.get("spark.jobGroup.id") == group_id) or gid_pat.search(desc or ""):
                    for sid in (ev.get("Stage IDs") or []):
                        stage_ids.add(sid)

            elif et in ("SparkListenerSQLExecutionStart",
                        "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"):
                desc = ev.get("Description") or ""
                if gid_pat.search(desc):
                    for sid in (ev.get("Stage IDs") or ev.get("stageIds") or []):
                        stage_ids.add(sid)

    # Pass 2: aggregate TaskEnd in the window (and within collected stageIds if any)
    seen = {}  # (stageId, taskId) -> (attempt, status, duration, event)
    with _open_eventlog(f) as fp:
        for line in fp:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            if ev.get("Event") != "SparkListenerTaskEnd":
                continue

            sid = ev.get("Stage ID")
            if stage_ids and sid not in stage_ids:
                continue

            tinfo = ev.get("Task Info") or {}
            ft = tinfo.get("Finish Time")
            lt = tinfo.get("Launch Time")
            if not isinstance(ft, int) or ft < t0_ms or ft > t1_ms:
                continue

            tid = tinfo.get("Task ID")
            att = tinfo.get("Attempt") or 0
            dur = (ft - (lt or ft)) or 0
            stat = (ev.get("Task End Reason") or {}).get("Reason") or "Success"

            key = (sid, tid)
            prev = seen.get(key)
            if prev is None:
                seen[key] = (att, stat, dur, ev)
            else:
                patt, pstat, pdur, _ = prev
                better = False
                if pstat != "Success" and stat == "Success":
                    better = True
                elif pstat == stat:
                    if att > patt:
                        better = True
                    elif att == patt and dur > pdur:
                        better = True
                if better:
                    seen[key] = (att, stat, dur, ev)

    bytes_read = 0
    files_read = 0
    exec_rt_ms = 0
    exec_cpu_ns = 0

    for _, _, _, ev in seen.values():
        tinfo = ev.get("Task Info") or {}
        accs = tinfo.get("Accumulables") or []

        br = _acc_val(accs, (
            "internal.metrics.input.bytesRead",
            "bytesRead",
        ))
        if br == 0:
            tm = ev.get("Task Metrics") or {}
            im = tm.get("Input Metrics") or {}
            try:
                br = int(im.get("Bytes Read") or 0)
            except Exception:
                br = 0
        bytes_read += br

        files_read += _acc_val(accs, (
            "number of files read",
            "internal.metrics.input.filesRead",
        ))

        exec_rt_ms  += _acc_val(accs, ("internal.metrics.executorRunTime",))
        exec_cpu_ns += _acc_val(accs, ("internal.metrics.executorCpuTime",))

    return {
        "bytesRead_ev": bytes_read,
        "files_scanned_ev": files_read,
        "bytes_scanned_ev": bytes_read,
        "executorRunTime_ms_ev": exec_rt_ms,
        "executorCpuTime_ns_ev": exec_cpu_ns,
    }

# ---------------- cache control ----------------
def pre_query_uncache(spark: SparkSession):
    try: spark.catalog.clearCache()
    except Exception: pass

def maybe_cache_table(spark, table, cache_mode):
    if cache_mode == "none": return None
    if cache_mode == "catalog":
        spark.sql(f"CACHE TABLE {table}"); return None
    if cache_mode == "df":
        df = spark.sql(f"SELECT * FROM {table}")
        df.persist(StorageLevel.MEMORY_AND_DISK); df.count(); return df

def maybe_uncache_table(spark, table, cache_obj, cache_mode):
    if cache_mode == "none": return
    if cache_mode == "catalog": spark.sql(f"UNCACHE TABLE {table}")
    elif cache_mode == "df" and cache_obj is not None: cache_obj.unpersist()

# ---------------- event logs export (optional utility) ----------------
def export_event_logs(spark: SparkSession, logs_root: pathlib.Path):
    """
    Copy current app's eventlog file(s) into logs_root/spark_eventlogs for offline analysis.
    """
    try:
        app_id = getattr(spark.sparkContext, "applicationId", None)
        ev_dir_conf = spark.conf.get("spark.eventLog.dir", _default_eventlog_dir())
        ev_dir = pathlib.Path(ev_dir_conf.replace("file://", ""))
        if not ev_dir.exists(): return
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
            try: shutil.copy2(src, dst)
            except Exception: shutil.copy(src, dst)
        write_json(target / "INDEX.json", {"applicationId": app_id or "", "eventLogDir": str(ev_dir), "copied": [p.name for p in matched]})
    except Exception:
        pass

# ---------------- single query runner ----------------
def run_one_query(
    spark: SparkSession,
    sql_text: str,
    group_id: str,
    action: str,
    rest_wait_ms: int,
    rest_poll_ms: int,
) -> dict:
    """
    Return ONLY the requested fields (REST kept; eventlog added with *_ev suffix):
      REST/UI:
        - bytesRead
        - elapsedTime_s
        - executorRunTime_s, executorCpuTime_s
        - bytes_input_files (df.inputFiles)
        - files_scanned, bytes_scanned (from executed plan; fallback via probe)
      EVENTLOG (SparkListenerTaskEnd, time-windowed for this query):
        - bytesRead_ev, files_scanned_ev, bytes_scanned_ev
        - executorRunTime_s_ev, executorCpuTime_s_ev
    """
    pre_query_uncache(spark)

    sc = spark.sparkContext
    sc.setJobGroup(group_id, f"query-{group_id}")
    sc.setLocalProperty("spark.job.description", f"query-{group_id}")

    # Start time for the per-query eventlog window (milliseconds)
    t0_ms = int(time.time() * 1000)

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
        # End time for the per-query eventlog window (milliseconds)
        t1_ms = int(time.time() * 1000)
        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)

    wall_s = (time.time() - wall_start)

    # 1) plan scan metrics (may be -1 if source doesn't expose)
    scan = collect_scan_metrics(df)
    files_scanned = 0 if scan.get("numFiles", -1) in (-1, None) else int(scan["numFiles"])
    bytes_scanned = 0 if scan.get("bytesRead", -1) in (-1, None) else int(scan["bytesRead"])

    # 2) upper bound from inputFiles()
    bytes_input_files = bytes_sum_input_files(df)

    # 3) REST aggregation (original path)
    ui = get_ui_base_url(spark)
    rest = {"bytesRead": 0, "executorRunTime_ms": 0, "executorCpuTime_ns": 0}
    if ui:
        rest = collect_metrics_via_rest(ui, spark, group_id, poll_ms_total=rest_wait_ms, poll_interval_ms=rest_poll_ms) or rest
        if rest.get("bytesRead", 0) == 0:
            # tiny extra breath if UI is slightly behind
            time.sleep(min(0.3, rest_wait_ms / 1000.0))
            again = collect_metrics_via_rest(ui, spark, group_id, poll_ms_total=rest_wait_ms // 2, poll_interval_ms=rest_poll_ms) or {}
            rest["bytesRead"] = max(int(rest.get("bytesRead", 0)), int(again.get("bytesRead", 0)))
            rest["executorRunTime_ms"] += int(again.get("executorRunTime_ms", 0))
            rest["executorCpuTime_ns"] += int(again.get("executorCpuTime_ns", 0))

    # 4) hard fallback via input_file_name() probe (for REST/plan gaps)
    if rest.get("bytesRead", 0) == 0 or bytes_input_files == 0 or files_scanned == 0:
        probe_files, probe_bytes = enumerate_scanned_files_and_bytes(spark, sql_text)
        if rest.get("bytesRead", 0) == 0:
            rest["bytesRead"] = int(probe_bytes)  # use probe when REST misses
        if bytes_input_files == 0:
            bytes_input_files = int(probe_bytes)
        if files_scanned == 0:
            files_scanned = int(probe_files)
        if bytes_scanned == 0:
            bytes_scanned = int(probe_bytes)

    # 5) EVENTLOG aggregation (independent of REST; distinguished by *_ev suffix)
    ev = parse_eventlog_for_group(spark, group_id=group_id, t0_ms=t0_ms, t1_ms=t1_ms)
    # Convert times to seconds for *_ev
    exec_run_s_ev = (ev.get("executorRunTime_ms_ev", 0) or 0) / 1000.0
    exec_cpu_s_ev = (ev.get("executorCpuTime_ns_ev", 0) or 0) / 1_000_000_000.0

    # convert REST times
    exec_run_s = (rest.get("executorRunTime_ms", 0) or 0) / 1000.0
    exec_cpu_s = (rest.get("executorCpuTime_ns", 0) or 0) / 1_000_000_000.0

    out = {
        # REST/UI path (kept for backward compatibility)
        "bytesRead": int(rest.get("bytesRead", 0) or 0),
        "elapsedTime_s": wall_s,
        "executorRunTime_s": exec_run_s,
        "executorCpuTime_s": exec_cpu_s,
        "bytes_input_files": int(bytes_input_files) / (1024 * 1024.0),
        "files_scanned": int(files_scanned),
        "bytes_scanned": int(bytes_scanned) / (1024 * 1024.0),

        # EVENTLOG path (distinguished with *_ev suffix)
        "bytesRead_ev": int(ev.get("bytesRead_ev", 0) / (1024 * 1024.0) or 0),
        "files_scanned_ev": int(ev.get("files_scanned_ev", 0) or 0),
        "bytes_scanned_ev": int(ev.get("bytes_scanned_ev", 0) / (1024 * 1024.0) or 0),
        "executorRunTime_s_ev": exec_run_s_ev,
        "executorCpuTime_s_ev": exec_cpu_s_ev,
    }
    return out

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser("Unified query runner (REST + EventLog side-by-side)")
    ap.add_argument("--engine", required=True, choices=["delta", "hudi", "iceberg"])
    ap.add_argument("--table", required=True, help="Table (catalog id, delta.`/path`, or absolute path for Hudi)")
    ap.add_argument("--queries_dir", default="queries", help="Dir of .sql files with {{tbl}}")
    ap.add_argument("--warmup", action="store_true", help="Warm up each query (discard)")
    ap.add_argument("--cache", choices=["none", "catalog", "df"], default="none", help="Caching mode")
    ap.add_argument("--action", choices=["count", "collect", "show"], default="collect", help="Spark action")
    ap.add_argument("--output_csv", default="results.csv", help="CSV path")
    ap.add_argument("--broadcast_hint", action="store_true", help="Enable broadcast join (50MB)")
    ap.add_argument("--rest_wait_ms", type=int, default=1500, help="Total wait for REST polling (ms)")
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

    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Keep original REST columns; append *_ev columns for eventlog
        writer.writerow([
            "engine","query",
            "bytesRead","elapsedTime_s","executorRunTime_s","executorCpuTime_s",
            "bytes_input_files","files_scanned","bytes_scanned",
            "bytesRead_ev","files_scanned_ev","bytes_scanned_ev","executorRunTime_s_ev","executorCpuTime_s_ev",
        ])
        for qf in qfiles:
            raw = read_sql(qf)
            sql_text = substitute(raw, sql_table)

            if args.warmup:
                _ = run_one_query(spark, sql_text, f"warmup-{os.path.basename(qf)}",
                                  args.action, args.rest_wait_ms, args.rest_poll_ms)

            m = run_one_query(spark, sql_text, f"run-{os.path.basename(qf)}",
                              args.action, args.rest_wait_ms, args.rest_poll_ms)
            writer.writerow([
                args.engine,
                os.path.basename(qf),
                m["bytesRead"],
                f"{m['elapsedTime_s']:.3f}",
                f"{m['executorRunTime_s']:.3f}",
                f"{m['executorCpuTime_s']:.3f}",
                f"{m['bytes_input_files']:.3f}",
                m['files_scanned'],
                f"{m['bytes_scanned']:.3f}",
                f"{m['bytesRead_ev']:.3f}",
                m["files_scanned_ev"],
                f"{m['bytes_scanned_ev']:.3f}",
                f"{m['executorRunTime_s_ev']:.3f}",
                f"{m['executorCpuTime_s_ev']:.3f}",
            ])

    maybe_uncache_table(spark, sql_table, cache_obj, args.cache)
    spark.stop()
    print(f"[OK] wrote {args.output_csv}")

if __name__ == "__main__":
    main()
