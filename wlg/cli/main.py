"""Typer-based CLI entry points for workload profiling and generation."""

from __future__ import annotations

import itertools
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import datetime as dt
import yaml
import typer

import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")                     # headless backend for servers/CI
import matplotlib.pyplot as plt


# ---- project imports ----
from wlg.datasource.csv import CSVDataSource
from wlg.datasource.jsonl import JSONLDataSource
from wlg.datasource.parquet import ParquetDataSource
from wlg.datasource.postgres import PostgresDataSource
from wlg.emit.sql_emit import write_sql_dir
from wlg.emit.yaml_emit import write_workload
from wlg.profiler.dist_store import build_uni_dists, load_yaml, save_yaml, UniDist
from wlg.profiler.stats import ColumnStats, Profiler
from wlg.sampler.predicates import sample_between, sample_copula, sample_eq_from_topk
from wlg.templates.dialect import format_sql
from wlg.templates.sql import TemplateRenderer, TemplateSpec

# -----------------------------------------------------------------------------
# Typer app
# -----------------------------------------------------------------------------
app = typer.Typer(help="Workload generator CLI.")


# -----------------------------------------------------------------------------
# Utility helpers (shared by commands)
# -----------------------------------------------------------------------------
def _create_source(
    fmt: str,
    input_ref: str,
    table: Optional[str],
    sample_rows: Optional[int],
):
    """Factory for data sources."""
    fmt_lower = fmt.lower()
    if fmt_lower == "parquet":
        return ParquetDataSource(path=input_ref, sample_rows=sample_rows)
    if fmt_lower == "csv":
        return CSVDataSource(path=input_ref, sample_rows=sample_rows)
    if fmt_lower == "jsonl":
        return JSONLDataSource(path=input_ref, sample_rows=sample_rows)
    if fmt_lower == "postgres":
        if not table:
            raise typer.BadParameter("Table name is required for Postgres sources.")
        return PostgresDataSource(dsn=input_ref, table=table, sample_rows=sample_rows)
    raise typer.BadParameter(f"Unsupported format: {fmt}")


def _ensure_parent_dir(path: Path) -> None:
    """Create the parent directory for `path` if needed."""
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# FILL: range-driven template filler (no profiling required; can use stats for domains)
# -----------------------------------------------------------------------------
@app.command(name="fill")
def fill(
    spec: Path = typer.Option(..., help="YAML with templates and parameter ranges."),
    out: Path = typer.Option(Path("workload.yaml"), help="Output workload YAML."),
    sql_dir: Optional[Path] = typer.Option(None, help="Optional dir to emit .sql files."),
    stats: Optional[Path] = typer.Option(None, help="Optional stats.yaml to derive column min/max for ratio rules."),
):
    """
    Fill templates using user-provided ranges + ratio rules.
    Key fix: skip sampling params that are controlled by interval_rules (lo/hi),
    then apply rules, then check constraints.
    Also: resolve domains from stats.yaml (supports datetime epoch-ms to ISO dates).
    """
    typer.echo(f"[fill] spec={spec} out={out} sql_dir={sql_dir} stats={'yes' if stats else 'no'}")

    cfg = yaml.safe_load(spec.read_text())
    gen = (cfg.get("generation") or {})
    n = int(gen.get("n", 10))
    mode = str(gen.get("mode", "random")).lower()  # random | grid | lhs
    seed = gen.get("seed", None)
    rng = random.Random(0 if seed is None else seed)

    # Load stats; accept both {columns:{...}} and flat {col:{...}}
    raw_stats = {}
    if stats and stats.exists():
        raw_stats = yaml.safe_load(stats.read_text()) or {}
    stats_cols = raw_stats.get("columns", raw_stats) or {}

    # ---------- helpers ----------
    def _date_span(lo: str, hi: str) -> int:
        d0, d1 = dt.date.fromisoformat(str(lo)), dt.date.fromisoformat(str(hi))
        return (d1 - d0).days

    float_like = {"float", "number", "numeric"}
    categorical_like = {"string", "categorical"}

    def _cast_value(t: str, x):
        if t == "int": return int(x)
        if t in float_like: return float(x)
        if t == "date": return str(x)  # keep ISO
        return x

    def _cat_values(pname: str, p: dict):
        choices = p.get("choices") or []
        if choices:
            return [str(c) for c in choices]
        # infer column name from param name like "asin_v1" -> "asin"
        col = pname
        if "_v" in pname:
            col = pname.split("_v", 1)[0]
        meta = stats_cols.get(col, {}) if col else {}
        topk = meta.get("topk") or []
        vals = []
        for item in topk:
            if isinstance(item, (list, tuple)) and item:
                vals.append(item[0])
            else:
                vals.append(item)
        return [str(v) for v in vals if v is not None]

    def _sample_one(pname: str, p: dict):
        """Sample one param (ONLY for params not covered by interval_rules)."""
        t = p["type"]; lo, hi = p.get("range", [None, None]); step = p.get("step")
        if t == "date":
            lo, hi = _epochms_to_iso(lo), _epochms_to_iso(hi)
            if lo is None or hi is None:
                raise ValueError("date param requires explicit range [lo, hi]")
            days = max(0, _date_span(lo, hi))
            k = rng.randint(0, days)
            return str(dt.date.fromisoformat(str(lo)) + dt.timedelta(days=k))
        if t == "int":
            if lo is None or hi is None:
                raise ValueError("int param requires explicit range [lo, hi] or ratio rule.")
            if step:
                loi, hii, stepi = int(lo), int(hi), int(step)
                kmax = max(0, (hii - loi) // stepi)
                return loi + rng.randint(0, kmax) * stepi
            return rng.randint(int(lo), int(hi))
        if t in float_like:
            if lo is None or hi is None:
                raise ValueError("float/number param requires explicit range [lo, hi] or ratio rule.")
            if step:
                lof, hif, stepf = float(lo), float(hi), float(step)
                cnt = int(round((hif - lof) / stepf)) + 1
                idx = rng.randint(0, max(0, cnt - 1))
                return round(lof + idx * stepf, 12)
            return rng.uniform(float(lo), float(hi))
        if t in categorical_like:
            vals = _cat_values(pname, p)
            if vals:
                return rng.choice(vals)
            if lo is not None:
                return lo
            raise ValueError(f"categorical param '{pname}' needs values (choices/topk/range)")
        return lo

    def _grid_values(pname: str, p: dict, m: int):
        t = p["type"]; lo, hi = p.get("range", [None, None]); step = p.get("step")
        if t == "date":
            lo, hi = _epochms_to_iso(lo), _epochms_to_iso(hi)
            if lo is None or hi is None: raise ValueError("date param needs range for grid")
            days = max(1, _date_span(lo, hi))
            idxs = [round(i * days / (m - 1)) for i in range(m)] if m > 1 else [0]
            base = dt.date.fromisoformat(str(lo))
            return [str(base + dt.timedelta(days=i)) for i in idxs]
        if t == "int":
            if lo is None or hi is None: raise ValueError("int param needs range for grid")
            if step:
                return list(range(int(lo), int(hi) + 1, int(step)))[:m]
            if m == 1: return [int((int(lo) + int(hi)) // 2)]
            return [int(round(int(lo) + i * (int(hi) - int(lo)) / (m - 1))) for i in range(m)]
        if t in float_like:
            if lo is None or hi is None: raise ValueError("float/number param needs range for grid")
            if step:
                lof, hif, stepf = float(lo), float(hi), float(step)
                cnt = int(round((hif - lof) / stepf)) + 1
                return [round(lof + i * stepf, 12) for i in range(min(cnt, m))]
            if m == 1: return [0.5 * (float(lo) + float(hi))]
            return [float(lo) + i * (float(hi) - float(lo)) / (m - 1) for i in range(m)]
        if t in categorical_like:
            choices = _cat_values(pname, p)
            if not choices:
                raise ValueError(f"categorical param '{pname}' needs values for grid mode")
            if m == 1:
                return [choices[0]]
            if len(choices) >= m:
                return choices[:m]
            # repeat choices to reach m
            res = []
            while len(res) < m:
                res.extend(choices)
            return res[:m]
        return [lo] * m

    def _lhs_values(pname: str, p: dict, m: int):
        t = p["type"]; lo, hi = p.get("range", [None, None])
        if t == "date":
            lo, hi = _epochms_to_iso(lo), _epochms_to_iso(hi)
            if lo is None or hi is None: raise ValueError("date param needs range for lhs")
            days = max(1, _date_span(lo, hi))
            bins = [(i * days // m, (i + 1) * days // m) for i in range(m)]
            picks = [rng.randint(a, max(a, b)) for a, b in bins]; rng.shuffle(picks)
            base = dt.date.fromisoformat(str(lo))
            return [str(base + dt.timedelta(days=i)) for i in picks]
        if t in ("int", *float_like):
            if lo is None or hi is None: raise ValueError(f"{t} param needs range for lhs")
            pts = []
            for i in range(m):
                a = float(lo) + i * (float(hi) - float(lo)) / m
                b = float(lo) + (i + 1) * (float(hi) - float(lo)) / m
                x = rng.uniform(a, b)
                pts.append(int(round(x)) if t == "int" else x)
            rng.shuffle(pts)
            return pts
        if t in categorical_like:
            choices = _cat_values(pname, p)
            if not choices:
                raise ValueError(f"categorical param '{pname}' needs values for lhs mode")
            res = []
            for _ in range(m):
                res.append(rng.choice(choices))
            return res
        return [lo] * m

    def _constraints_ok(row: dict, param_defs: dict) -> bool:
        # Evaluate boolean expressions like "q_hi > q_lo"
        for _, spec in (param_defs or {}).items():
            expr = spec.get("constraint")
            if not expr: continue
            try:
                if not eval(expr, {"__builtins__": {}}, dict(row)):
                    return False
            except Exception:
                return False
        return True

    def _epochms_to_iso(x):
        # Accept float/int epoch-ms → ISO date string
        try:
            if isinstance(x, (int, float)):
                return dt.date.fromtimestamp(float(x) / 1000.0).isoformat()
        except Exception:
            pass
        return x

    def _resolve_domain(rule: dict):
        """
        Domain resolution order:
        1) rule['domain'] if provided
        2) stats.yaml by rule['column'] (supports {columns:{...}} or flat)
        """
        if "domain" in rule:
            lo, hi = rule["domain"]
            return lo, hi
        col = rule.get("column")
        meta = stats_cols.get(col, {}) if col else {}
        lo, hi = meta.get("min"), meta.get("max")
        # If rule type is date, convert epoch-ms → ISO
        if rule.get("type") == "date":
            lo, hi = _epochms_to_iso(lo), _epochms_to_iso(hi)
        return lo, hi

    def _apply_interval_rules(row: dict, rules: list, rng_: random.Random):
        """
        Fill lo/hi params from ratio or ratio_range on top of [min,max] domain.
        Supported types: int | float | date.
        """
        for r in (rules or []):
            lo_k, hi_k, tp = r["lo"], r["hi"], r["type"]
            dom_lo, dom_hi = _resolve_domain(r)
            ratio = r.get("ratio"); ratio_rng = r.get("ratio_range")
            if ratio_rng:
                a, b = float(ratio_rng[0]), float(ratio_rng[1])
                width_ratio = rng_.uniform(min(a, b), max(a, b))
            elif ratio is not None:
                width_ratio = float(ratio)
            else:
                raise ValueError("interval_rule requires 'ratio' or 'ratio_range'.")

            if tp in categorical_like:
                col = r.get("column")
                meta = stats_cols.get(col, {}) if col else {}
                topk = meta.get("topk") or []
                vals = []
                for item in topk:
                    if isinstance(item, (list, tuple)) and item:
                        vals.append(item[0])
                    else:
                        vals.append(item)
                values = sorted({str(v) for v in vals if v is not None})
                if not values:
                    raise ValueError(f"Missing topk values for categorical interval rule (column={col})")
                width = max(1, int(round(width_ratio * len(values))))
                width = min(width, len(values))
                start = rng_.randint(0, max(0, len(values) - width))
                lo_val = values[start]
                hi_val = values[start + width - 1]
                row[lo_k], row[hi_k] = lo_val, hi_val
                continue

            if dom_lo is None or dom_hi is None:
                raise ValueError(f"Missing domain for interval rule (column={r.get('column')})")

            if tp == "int":
                L, H = int(dom_lo), int(dom_hi)
                width = max(1, int(round(width_ratio * (H - L))))
                step = int(r.get("align_step", 1))
                width = max(step, (width // step) * step)
                start_max = max(L, H - width)
                lo_val = L if start_max <= L else rng_.randrange(L, start_max + 1, step)
                lo_val = ((lo_val - L) // step) * step + L
                hi_val = min(lo_val + width, H)
                row[lo_k], row[hi_k] = lo_val, hi_val

            elif tp in float_like:
                L, H = float(dom_lo), float(dom_hi)
                width = max(0.0, width_ratio * (H - L))
                start = rng_.uniform(L, max(L, H - width))
                row[lo_k], row[hi_k] = start, start + width

            elif tp == "date":
                # --- helpers ---
                def _to_date(x):
                    """Accept datetime.date, ISO string, or epoch-ms -> datetime.date."""
                    if isinstance(x, dt.date):
                        return x
                    if isinstance(x, (int, float)):
                        iso = _epochms_to_iso(x)  # defined above in your fill()
                        return dt.date.fromisoformat(str(iso))
                    return dt.date.fromisoformat(str(x))

                # Column domain (from stats or explicit domain)
                d0 = _to_date(dom_lo)
                d1 = _to_date(dom_hi)
                col_span_days = max(1, (d1 - d0).days)

                # Choose width by ratio or ratio_range
                width_days = max(
                    1,
                    int(round(width_ratio * col_span_days))
                )

                align = r.get("align_with")
                if align:
                    # Base window to align with (e.g., ship date window for receipt)
                    base_lo = _to_date(row[align["lo"]])
                    base_hi = _to_date(row[align["hi"]])

                    # Optional lag between starts (rd_lo = sd_lo + lag)
                    lag_lo, lag_hi = r.get("lag_days", [0, 0])
                    lag_lo, lag_hi = int(min(lag_lo, lag_hi)), int(max(lag_lo, lag_hi))
                    lag = rng_.randint(lag_lo, lag_hi)

                    # Start from (base_lo + lag); try to keep the window near the base window
                    start = base_lo + dt.timedelta(days=lag)

                    # Initial aligned window
                    lo_val = start
                    hi_val = lo_val + dt.timedelta(days=width_days)

                    # Hard caps relative to base window (optional)
                    max_start = r.get("max_start_gap_days")
                    if max_start is not None:
                        cap = base_lo + dt.timedelta(days=int(max_start))
                        if lo_val > cap:
                            lo_val = cap
                            hi_val = lo_val + dt.timedelta(days=width_days)

                    max_end = r.get("max_end_gap_days")
                    if max_end is not None:
                        cap = base_hi + dt.timedelta(days=int(max_end))
                        if hi_val > cap:
                            hi_val = cap
                            lo_val = hi_val - dt.timedelta(days=width_days)

                    # Clamp to column domain if requested
                    if r.get("clip_to_domain", False):
                        if lo_val < d0:
                            lo_val = d0
                            hi_val = lo_val + dt.timedelta(days=width_days)
                        if hi_val > d1:
                            hi_val = d1
                            lo_val = hi_val - dt.timedelta(days=width_days)

                    # Ensure non-negative interval
                    if hi_val < lo_val:
                        hi_val = lo_val

                else:
                    # Non-aligned: draw uniformly within the column domain
                    latest_start = max(d0, d1 - dt.timedelta(days=width_days))
                    start = _to_date(
                        d0 + dt.timedelta(days=rng_.randint(0, max(0, (latest_start - d0).days)))
                    )
                    lo_val = start
                    hi_val = lo_val + dt.timedelta(days=width_days)

                # Emit as ISO strings
                row[lo_k], row[hi_k] = lo_val.isoformat(), hi_val.isoformat()


            else:
                raise ValueError(f"Unsupported interval_rule type: {tp}")

    # ---------- generation ----------
    outputs = []
    templates = (cfg.get("templates") or [])
    typer.echo(f"[fill] loaded templates: {len(templates)}")

    for tpl in templates:
        sql = tpl["sql"]
        param_defs = tpl.get("params", {}) or {}
        names = list(param_defs.keys())

        # Determine params covered by interval_rules (lo/hi placeholders)
        rules = tpl.get("interval_rules", []) or []
        covered = set()
        for r in rules:
            covered.add(r["lo"]); covered.add(r["hi"])

        candidates = []

        if mode == "grid":
            # grid only for UNcovered params; covered ones are filled by rules later
            d = max(1, len([nm for nm in names if nm not in covered]))
            k = max(1, math.ceil(n ** (1.0 / d)))
            grids = []
            for nm in names:
                if nm in covered:
                    grids.append([None] * k)
                else:
                    grids.append(_grid_values(nm, param_defs[nm], k))
            for combo in itertools.product(*grids):
                row = {nm: _cast_value(param_defs[nm]["type"], v) for nm, v in zip(names, combo)}
                _apply_interval_rules(row, rules, rng)
                if _constraints_ok(row, param_defs):
                    candidates.append(row)
                if len(candidates) >= n:
                    break

        elif mode == "lhs":
            sets = []
            for nm in names:
                if nm in covered:
                    sets.append([None] * n)
                else:
                    sets.append(_lhs_values(nm, param_defs[nm], n))
            for i in range(n):
                row = {nm: _cast_value(param_defs[nm]["type"], sets[j][i]) for j, nm in enumerate(names)}
                _apply_interval_rules(row, rules, rng)
                if _constraints_ok(row, param_defs):
                    candidates.append(row)

        else:  # random
            # for _ in range(max(1, n * 3)):  # oversample
            #     row = {}
            #     for nm in names:
            #         if nm in covered:
            #             row[nm] = None
            #         else:
            #             row[nm] = _cast_value(param_defs[nm]["type"], _sample_one(param_defs[nm]))
            #     _apply_interval_rules(row, rules, rng)
            #     if _constraints_ok(row, param_defs):
            #         candidates.append(row)
            #     if len(candidates) >= n:
            #         break
            attempts = 0
            while len(candidates) < n:
                attempts += 1
                row = {}
                for nm in names:
                    row[nm] = None if nm in covered else _cast_value(param_defs[nm]["type"], _sample_one(nm, param_defs[nm]))
                _apply_interval_rules(row, rules, rng)
                if _constraints_ok(row, param_defs):
                    candidates.append(row)

        # Render SQL (if your SQL uses DATE ':d_lo', keep as-is; otherwise you may add dialect logic)
        for row in candidates[:n]:
            sql_filled = sql
            for k, v in row.items():
                sql_filled = sql_filled.replace(f":{k}", str(v))
            outputs.append({"tpl": tpl.get("id", "T"), "sql": sql_filled, "params": row})

    _ensure_parent_dir(out)
    write_workload(out, outputs)
    typer.echo(f"[fill] Wrote workload YAML to {out}")
    if sql_dir:
        sql_dir.mkdir(parents=True, exist_ok=True)
        for i, e in enumerate(outputs, 1):
            (sql_dir / f"{i:03d}_{e['tpl']}.sql").write_text(e["sql"])
        typer.echo(f"[fill] Wrote {len(outputs)} SQL files to {sql_dir}")
    typer.echo(f"[fill] generated entries: {len(outputs)}")


# -----------------------------------------------------------------------------
# PROFILE: build stats.yaml from a data source
# -----------------------------------------------------------------------------
@app.command(name="profile")
def profile(
    input: str = typer.Option(..., help="Source path or connection string."),
    format: str = typer.Option(..., help="Source format (parquet/csv/jsonl/postgres)."),
    table: Optional[str] = typer.Option(None, help="Table name (for Postgres)."),
    out: Path = typer.Option(Path("stats.yaml"), help="Output YAML path."),
    sample_rows: Optional[int] = typer.Option(None, help="Maximum number of rows to sample."),
    seed: Optional[int] = typer.Option(None, help="Seed for deterministic sampling."),
    # NEW: date parsing controls
    date_cols: Optional[str] = typer.Option(
        None, help="Comma-separated column names to parse as dates (overrides inference)."
    ),
    infer_dates: bool = typer.Option(
        True, help="Heuristically infer date columns from string/object columns."
    ),
) -> None:
    """Profile the input dataset and emit column statistics (with real date ranges if requested)."""

    # Build data source
    source = _create_source(format, input, table=table, sample_rows=sample_rows)

    # Configure date parsing on the source if supported
    # (Parquet/CSV/JSONL sources should implement `configure_date_parsing(explicit: List[str], infer: bool)`).
    explicit_cols = [c.strip() for c in (date_cols or "").split(",") if c.strip()]
    if hasattr(source, "configure_date_parsing"):
        source.configure_date_parsing(explicit=explicit_cols, infer=infer_dates)

    # Profile batches
    profiler = Profiler(seed=seed)
    for batch in source.scan_batches():
        profiler.update(batch)

    # Persist stats (+ schema & correlations metadata)
    stats = profiler.finalize()
    metadata = {
        "schema": source.schema(),                # reflects parsed dtypes (e.g., 'date' for datetime64)
        "correlations": profiler.correlations,    # pearson/spearman + top_pairs
    }
    _ensure_parent_dir(out)
    save_yaml(stats, out, metadata=metadata)
    typer.echo(f"Saved statistics to {out}")


# -----------------------------------------------------------------------------
# GEN: distribution-driven predicate generation using stats.yaml
# -----------------------------------------------------------------------------
@app.command(name="gen")
def gen(
    stats: Path = typer.Option(..., help="Statistics YAML file."),
    table: str = typer.Option(..., help="Table name for SQL templates."),
    templates: str = typer.Option("A,B,C", help="Comma-separated template identifiers."),
    dialect: str = typer.Option("sparksql", help="SQL dialect."),
    target_sel: float = typer.Option(0.05, help="Target selectivity for predicates."),
    out: Path = typer.Option(Path("workload.yaml"), help="Output workload YAML."),
    seed: Optional[int] = typer.Option(None, help="Sampling seed."),
    sql_dir: Optional[Path] = typer.Option(None, help="Optional directory for .sql files."),
) -> None:
    """Generate workload definitions from previously profiled statistics."""
    column_stats, metadata = load_yaml(stats)
    uni_dists = build_uni_dists(column_stats)
    renderer = TemplateRenderer(table=table, dialect=dialect)
    rng = random.Random(0 if seed is None else seed)
    template_ids = [item.strip().upper() for item in templates.split(",") if item.strip()]

    helper = _TemplateHelper(
        stats=column_stats,
        dists=uni_dists,
        metadata=metadata,
        rng=rng,
        target_sel=target_sel,
        renderer=renderer,
        dialect=dialect,
    )
    workload_entries: List[Dict[str, object]] = []
    for tpl in template_ids:
        spec = helper.generate_template(tpl)
        if spec is None:
            typer.echo(f"Skipped template {tpl}: insufficient statistics", err=True)
            continue
        # workload_entries.append(
        #     {"tpl": spec.name, "sql": format_sql(spec.sql, dialect), "params": spec.params}
        # )
        sql_txt = format_sql(spec.sql, dialect)
        # Prefer explicit inject_table; otherwise still replace with --table (kept for convenience)
        tbl_str = inject_table or table
        if tbl_str:
            sql_txt = sql_txt.replace("{{tbl}}", tbl_str).replace("{{table}}", tbl_str)
        workload_entries.append({"tpl": spec.name, "sql": sql_txt, "params": spec.params})

    if not workload_entries:
        raise typer.Exit(code=1)

    _ensure_parent_dir(out)
    write_workload(out, workload_entries)
    typer.echo(f"Wrote workload YAML to {out}")
    if sql_dir is not None:
        sql_dir.mkdir(parents=True, exist_ok=True)
        write_sql_dir(sql_dir, workload_entries)
        typer.echo(f"Wrote SQL files to {sql_dir}")


@app.command(name="viz")
def viz(
    stats: Path = typer.Option(..., help="stats.yaml produced by `profile`."),
    spec: Path = typer.Option(..., help="workload spec (interval_rules map params -> columns)."),
    workload: List[Path] = typer.Option(None, "--workload", help="One or more workload.yaml files."),
    workload_dir: Optional[Path] = typer.Option(None, help="Directory containing workload yaml files."),
    out_dir: Path = typer.Option(Path("viz_out"), help="Output folder for PNGs and index.html."),
    style: str = typer.Option("overlay", help="Visualization style: overlay | heat"),
):
    """
    Visualize where queries land on column distributions.

    Styles:
      - overlay: per-column histogram with all [lo,hi] ranges semi-transparently overlaid
      - heat:    per-column histogram on top + a 1-row heat strip showing coverage density of all ranges
    """
    import numpy as np
    import matplotlib
    import matplotlib.pyplot as plt
    import datetime as dt
    import yaml
    from typing import Dict, List, Tuple, Optional, Iterable

    style = style.lower().strip()
    if style not in {"overlay", "heat"}:
        raise typer.BadParameter("style must be one of: overlay, heat")

    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- load stats ----------
    s = yaml.safe_load(stats.read_text()) or {}
    stats_cols = s.get("columns", s) or {}

    # ---------- load spec / interval rules ----------
    spec_cfg = yaml.safe_load(spec.read_text()) or {}
    templates = spec_cfg.get("templates", []) or []
    tpl_rules: Dict[str, List[dict]] = {t.get("id", "T"): (t.get("interval_rules") or []) for t in templates}

    # Build a lookup: template id -> list[(column, lo_param, hi_param, type)]
    tpl_to_intervals: Dict[str, List[Tuple[str, str, str, str]]] = {}
    for tid, rules in tpl_rules.items():
        pairs = []
        for r in rules:
            col = r.get("column")
            lo, hi, tp = r.get("lo"), r.get("hi"), r.get("type", "")
            if col and lo and hi:
                pairs.append((col, lo, hi, tp))
        tpl_to_intervals[tid] = pairs

    # ---------- load workloads (files and/or directory) ----------
    def _iter_workload_files() -> Iterable[Path]:
        seen = set()
        if workload:
            for p in workload:
                q = p.resolve()
                if q not in seen and q.exists():
                    seen.add(q)
                    yield q
        if workload_dir and workload_dir.exists():
            for p in sorted(workload_dir.glob("**/*.y*ml")):
                q = p.resolve()
                if q not in seen:
                    seen.add(q)
                    yield q

    def _load_entries(p: Path) -> List[dict]:
        """Support list or {workload:[...]} or {entries:[...]} shapes."""
        try:
            data = yaml.safe_load(p.read_text()) or {}
        except Exception:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("workload"), list):
                return data["workload"]
            if isinstance(data.get("entries"), list):
                return data["entries"]
        return []

    all_entries: List[dict] = []
    for wf in _iter_workload_files():
        all_entries.extend(_load_entries(wf))

    if not all_entries:
        typer.echo("[viz] no workload entries found. Provide --workload and/or --workload-dir.", err=True)
        raise typer.Exit(code=1)

    # ---------- helpers ----------
    def _epochms_to_date(x):
        """Convert epoch milliseconds or ISO string to datetime.date; return None if not parseable."""
        try:
            if isinstance(x, (int, float)):
                return dt.date.fromtimestamp(float(x) / 1000.0)
        except Exception:
            pass
        if isinstance(x, str):
            try:
                return dt.date.fromisoformat(x)
            except Exception:
                return None
        return None

    def _get_hist(colmeta):
        """Return (counts, edges) if 'hist' is present and valid; else None."""
        hist = colmeta.get("hist")
        if not hist or not isinstance(hist, (list, tuple)) or len(hist) != 2:
            return None
        counts, edges = hist
        if not counts or not edges or len(edges) != len(counts) + 1:
            return None
        return counts, edges

    def _quantile_density(colmeta, bins=50):
        """Build a coarse histogram from min/quantiles/max when 'hist' is missing (viz only)."""
        qs = colmeta.get("quantiles") or {}
        pts = [colmeta.get("min")] + [qs[k] for k in sorted(qs.keys())] + [colmeta.get("max")]
        pts = [p for p in pts if p is not None]
        try:
            vals = sorted(set(float(p) for p in pts))
        except Exception:
            return None
        if len(vals) < 2:
            return None
        start, end = vals[0], vals[-1]
        edges = list(np.linspace(start, end, bins + 1))
        counts = [1] * bins
        return counts, edges

    def _col_kind(colmeta):
        return (colmeta.get("kind") or "").lower()

    def _draw_binned_hist(ax, counts, edges):
        """Draw a histogram from pre-binned counts+edges robustly."""
        c = np.asarray(counts, dtype=float)
        e = np.asarray(edges, dtype=float)
        try:
            ax.stairs(c, e, fill=False)
            ax.stairs(c, e, fill=True, alpha=0.12)
        except Exception:
            widths = e[1:] - e[:-1]
            centers = (e[:-1] + e[1:]) / 2.0
            ax.bar(centers, c, width=widths, align="center", edgecolor="black", alpha=0.8)

    def _ensure_numeric_edges_for_date(meta) -> Optional[np.ndarray]:
        """
        Convert date-like bin edges to ordinal numbers.
        Returns None if conversion fails.
        """
        hist = _get_hist(meta)
        if not hist:
            return None
        _, edges = hist
        try:
            dates = [_epochms_to_date(v) or dt.date.fromisoformat(str(v)) for v in edges]
            return np.array([d.toordinal() for d in dates], dtype=float)
        except Exception:
            return None

    # ---------- collect ranges per column ----------
    # col -> {"type": "numeric"|"date", "ranges": [(lo,hi), ...]}
    col_ranges: Dict[str, Dict[str, object]] = {}

    for e in all_entries:
        tid = e.get("tpl", "T")
        params = e.get("params", {}) or {}
        for (col, lo_key, hi_key, tp) in tpl_to_intervals.get(tid, []):
            if col not in stats_cols:
                continue
            if lo_key not in params or hi_key not in params:
                continue
            meta = stats_cols[col] or {}
            kind = _col_kind(meta)
            entry = col_ranges.setdefault(col, {"type": "numeric", "ranges": []})

            if tp == "date" or kind == "datetime":
                lo = _epochms_to_date(params[lo_key]) or dt.date.fromisoformat(str(params[lo_key]))
                hi = _epochms_to_date(params[hi_key]) or dt.date.fromisoformat(str(params[hi_key]))
                entry["type"] = "date"
                entry["ranges"].append((lo, hi))
            else:
                lo, hi = float(params[lo_key]), float(params[hi_key])
                entry["ranges"].append((lo, hi))

    # ---------- draw one figure per column ----------
    pages = []
    for col, info in col_ranges.items():
        meta = stats_cols.get(col, {})
        is_date = info["type"] == "date"
        ranges = info["ranges"]  # list of (lo, hi)

        # Bin source for the column
        hist = _get_hist(meta)
        if hist:
            counts, edges = hist
            counts = np.asarray(counts, dtype=float)
            if is_date:
                ord_edges = _ensure_numeric_edges_for_date(meta)
                if ord_edges is None:
                    # Fallback to min/max line if we cannot parse date bins
                    lo_all = _epochms_to_date(meta.get("min"))
                    hi_all = _epochms_to_date(meta.get("max"))
                    if not (lo_all and hi_all):
                        continue
                    edges = np.array([lo_all.toordinal(), hi_all.toordinal()], dtype=float)
                    counts = np.array([1.0], dtype=float)
                else:
                    edges = ord_edges
        else:
            # Build synthetic bins from quantiles
            qhist = _quantile_density(meta)
            if not qhist:
                # Last resort: for dates use min/max ordinal; for numeric skip
                if is_date:
                    lo_all = _epochms_to_date(meta.get("min"))
                    hi_all = _epochms_to_date(meta.get("max"))
                    if not (lo_all and hi_all):
                        continue
                    edges = np.linspace(lo_all.toordinal(), hi_all.toordinal(), 51)
                    counts = np.ones(50, dtype=float)
                else:
                    typer.echo(f"[viz] skip {col}: no hist/quantiles.", err=True)
                    continue
            else:
                counts, edges = qhist
                counts = np.asarray(counts, dtype=float)
                edges = np.asarray(edges, dtype=float)
                # dates won’t come here usually; keep numeric

        # Build heat coverage per bin (accumulate fractional overlap)
        heat = np.zeros_like(counts, dtype=float)
        if style in {"heat", "overlay"}:
            for (lo, hi) in ranges:
                if is_date:
                    lo = lo.toordinal()
                    hi = hi.toordinal()
                lo_f, hi_f = float(lo), float(hi)
                for b in range(len(counts)):
                    a, bnd = float(edges[b]), float(edges[b + 1])
                    width = max(bnd - a, 1e-12)
                    overlap = max(0.0, min(hi_f, bnd) - max(lo_f, a))
                    if overlap > 0:
                        heat[b] += overlap / width

        # Normalize heat to [0,1]
        heat_norm = heat / (heat.max() + 1e-12) if heat.size else heat

        # Create figure
        if style == "heat":
            fig, (ax1, ax2) = plt.subplots(
                2, 1, figsize=(7, 4.5), sharex=True, gridspec_kw={"height_ratios": [3, 0.6]}
            )
            _draw_binned_hist(ax1, counts, edges)
            ax1.set_title(f"{col}: data density (top) + query coverage (bottom)")
            ax1.set_ylabel("freq")

            # --- FIX: pcolormesh expects C with shape (len(y)-1, len(x)-1) = (1, B)
            # x = edges has B+1 values; y = [0,1] has 2 values -> C must be (1,B)
            H = heat_norm[None, :]  # shape (1, B)
            ax2.pcolormesh(edges, [0, 1], H, shading="auto")
            ax2.set_yticks([])
            ax2.set_ylim(0, 1)
            ax2.set_xlabel("date" if is_date else col)

            # Pretty date ticks (apply to the shared x on ax2)
            if is_date:
                ax2.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(6))
                xs = ax2.get_xticks()
                ax2.set_xticklabels(
                    [dt.date.fromordinal(int(v)).isoformat() for v in xs],
                    rotation=30, ha="right"
                )
            fig.tight_layout()
        else:
            # overlay: histogram + semi-transparent ranges
            fig, ax = plt.subplots(figsize=(7, 3.6))
            _draw_binned_hist(ax, counts, edges)
            alpha = 0.12 if len(ranges) > 20 else 0.2
            for (lo, hi) in ranges:
                if is_date:
                    lo, hi = lo.toordinal(), hi.toordinal()
                ax.axvspan(float(lo), float(hi), alpha=alpha)
            ax.set_title(f"{col}: data density + {len(ranges)} ranges")
            ax.set_ylabel("freq")
            ax.set_xlabel("date" if is_date else col)
            if is_date:
                ax.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(6))
                xs = ax.get_xticks()
                ax.set_xticklabels(
                    [dt.date.fromordinal(int(v)).isoformat() for v in xs],
                    rotation=30, ha="right"
                )
            fig.tight_layout()

        png = out_dir / f"{col}_{style}.png"
        fig.savefig(png)
        plt.close(fig)
        pages.append(png.name)

    # ---------- index ----------
    idx = out_dir / "index.html"
    with idx.open("w") as f:
        f.write("<html><body><h2>Workload Visualization</h2>\n")
        for png in pages:
            f.write(f"<div style='margin:10px 8px'><img src='{png}' style='max-width:920px;display:block'/></div>\n")
        f.write("</body></html>")
    typer.echo(f"[viz] wrote {len(pages)} plots → {out_dir} (and index.html)")


@app.command(name="viz-results")
def viz_results(
    results_dir: Path = typer.Option(..., help="Root folder containing result CSVs."),
    out_dir: Path = typer.Option(Path("viz_perf"), help="Output folder for plots and index.html."),
    metric_col: str = typer.Option("time_s", help="Primary metric column name if your CSV already has it."),
    assume_headerless: bool = typer.Option(
        True,
        help="If True, read CSVs without headers and map to [engine_in_file, query, rows, t1, t2, t3].",
    ),
    formats: List[str] = typer.Option(["png", "pdf"], help="Image formats to export, e.g. ['png','pdf','svg']."),
):
    """
    Visualize performance results across engines/layouts.

    Inputs:
      - A directory tree with CSV result files per engine/layout run.

    Outputs (saved under out_dir):
      - geomean_speedup.csv
      - heat_<engine>.(png|pdf|svg)
      - box_<engine>.(png|pdf|svg)
      - rows_vs_<metric>_<engine>.(png|pdf|svg)
      - ecdf_<engine>.(png|pdf|svg)
      - index.html aggregating previews/links
    """
    import re
    import numpy as np
    import pandas as pd
    import matplotlib
    matplotlib.use("Agg")  # safe for headless environments
    import matplotlib.pyplot as plt

    # --- sanitize formats ---
    fmts = []
    for f in formats:
        f = f.lower().strip(". ")
        if f in ("png", "pdf", "svg"):
            fmts.append(f)
    fmts = fmts or ["png", "pdf"]

    out_dir.mkdir(parents=True, exist_ok=True)

    # --- helper: save a figure to multiple formats ---
    def savefig_multi(fig, out_base: Path):
        """Save a Matplotlib figure to several formats using a common basename."""
        for ext in fmts:
            path = out_base.with_suffix(f".{ext}")
            if ext == "png":
                fig.savefig(path, dpi=150, bbox_inches="tight")
            else:
                fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    # -------- load & normalize results --------
    # Expect one CSV per run. If headerless, columns are:
    #   [engine_in_file, query, rows, t1, t2, t3]
    # Otherwise use metric_col directly.
    all_rows = []
    for csv_path in results_dir.rglob("*.csv"):
        df = pd.read_csv(csv_path, header=None if assume_headerless else "infer")
        if assume_headerless:
            df = df.rename(
                columns={0: "engine_in_file", 1: "query", 2: "rows", 3: "t1", 4: "t2", 5: "t3"}
            )
            # derive a single metric: median of provided timings
            df["time_s"] = pd.to_numeric(df[["t1", "t2", "t3"]].stack(), errors="coerce").groupby(level=0).median()
            df["engine"] = df["engine_in_file"].astype(str)
        else:
            # try to infer engine/layout from directory names
            df["engine"] = csv_path.parent.name
            if metric_col not in df.columns:
                raise ValueError(f"metric_col '{metric_col}' not found in {csv_path}")
            df["time_s"] = pd.to_numeric(df[metric_col], errors="coerce")

        # try to infer layout from engine name like "iceberg_zorder"
        # fallback: "baseline"
        def parse_layout(name: str) -> tuple[str, str]:
            m = re.match(r"^([a-zA-Z0-9]+)_(baseline|linear|zorder|hilbert)$", name)
            if m:
                return m.group(1), m.group(2)
            return name, "baseline"

        engines, layouts = [], []
        for e in df["engine"].astype(str):
            base, lay = parse_layout(e)
            engines.append(base)
            layouts.append(lay)
        df["engine"] = engines
        df["layout"] = layouts

        # keep only necessary columns
        keep = ["engine", "layout", "query", "rows", "time_s"]
        for k in keep:
            if k not in df.columns:
                df[k] = np.nan
        all_rows.append(df[keep])

    if not all_rows:
        raise typer.Exit(code=1)

    df = pd.concat(all_rows, ignore_index=True)
    df["rows"] = pd.to_numeric(df["rows"], errors="coerce")

    # -------- compute baseline & speedups --------
    # baseline per (engine, query): time under layout == baseline
    base = (
        df[df["layout"] == "baseline"]
        .groupby(["engine", "query"], as_index=False)["time_s"]
        .median()
        .rename(columns={"time_s": "t_base"})
    )
    df = df.merge(base, on=["engine", "query"], how="left")
    df["speedup"] = df["t_base"] / df["time_s"]

    # geometric mean speedup by engine/layout
    def gmean_safe(x):
        x = np.asarray(x, dtype=float)
        x = x[np.isfinite(x) & (x > 0)]
        if x.size == 0:
            return np.nan
        return float(np.exp(np.mean(np.log(x))))

    geomean = (
        df.groupby(["engine", "layout"])["speedup"]
        .apply(gmean_safe)
        .reset_index()
        .rename(columns={"speedup": "geomean_speedup"})
    )
    geomean.to_csv(out_dir / "geomean_speedup.csv", index=False)

    metric = "time_s"

    # -------- plots --------
    # 1) per-query speedup heatmap
    for eng, g in df.groupby("engine"):
        piv = g.pivot_table(index="query", columns="layout", values="speedup", aggfunc="median")
        order = [c for c in ("baseline", "linear", "zorder", "hilbert") if c in piv.columns]
        piv = piv[order] if order else piv
        piv = piv.replace([np.inf, -np.inf], np.nan).fillna(0.0)

        fig = plt.figure(figsize=(10, max(4, 0.25 * len(piv))))
        ax = fig.add_subplot(111)
        im = ax.imshow(piv.values, aspect="auto")
        fig.colorbar(im, ax=ax, label="Speedup vs baseline (×)")
        ax.set_yticks(range(len(piv.index))); ax.set_yticklabels(piv.index)
        ax.set_xticks(range(len(piv.columns))); ax.set_xticklabels(piv.columns)
        ax.set_title(f"{eng}: per-query speedup")
        fig.tight_layout()
        savefig_multi(fig, out_dir / f"heat_{eng}")

    # 2) time distribution boxplot per layout
    for eng, g in df.groupby("engine"):
        order = [c for c in ("baseline", "linear", "zorder", "hilbert") if c in g["layout"].unique()]
        series = [pd.to_numeric(g.loc[g["layout"] == lay, metric], errors="coerce").dropna().values for lay in order]
        if not any(len(s) for s in series):
            continue
        fig = plt.figure(figsize=(7, 4))
        ax = fig.add_subplot(111)
        ax.boxplot(series, labels=order, showfliers=False)
        ax.set_yscale("log")
        ax.set_ylabel(f"{metric} (log-scale)")
        ax.set_title(f"{eng}: time distribution by layout")
        fig.tight_layout()
        savefig_multi(fig, out_dir / f"box_{eng}")

    # 3) rows vs time scatter (log–log)
    for eng, g in df.groupby("engine"):
        fig = plt.figure(figsize=(7, 4))
        ax = fig.add_subplot(111)
        for lay in sorted(g["layout"].unique()):
            sub = g[g["layout"] == lay]
            x = pd.to_numeric(sub["rows"], errors="coerce").values
            y = pd.to_numeric(sub[metric], errors="coerce").values
            ax.scatter(x, y, label=lay, alpha=0.7)
        ax.set_xscale("log"); ax.set_yscale("log")
        ax.set_xlabel("rows (log)"); ax.set_ylabel(f"{metric} (log)")
        ax.set_title(f"{eng}: rows vs {metric}"); ax.legend()
        fig.tight_layout()
        savefig_multi(fig, out_dir / f"rows_vs_{metric}_{eng}")

    # 4) ECDF of times per layout
    for eng, g in df.groupby("engine"):
        fig = plt.figure(figsize=(7, 4))
        ax = fig.add_subplot(111)
        for lay in sorted(g["layout"].unique()):
            vals = pd.to_numeric(g.loc[g["layout"] == lay, metric], errors="coerce").dropna().values
            if len(vals) == 0:
                continue
            vals = np.sort(vals)
            y = np.arange(1, len(vals) + 1) / len(vals)
            ax.step(vals, y, where="post", label=lay)
        ax.set_xscale("log")
        ax.set_xlabel(f"{metric} (log)"); ax.set_ylabel("Fraction ≤ t")
        ax.set_title(f"{eng}: ECDF of {metric}"); ax.legend()
        fig.tight_layout()
        savefig_multi(fig, out_dir / f"ecdf_{eng}")

    # -------- HTML index with links to all formats --------
    with (out_dir / "index.html").open("w") as f:
        f.write("<html><body><h2>Performance Visualization</h2>\n")
        f.write("<h3>Geometric mean speedup vs baseline</h3>\n")
        f.write("<p><a href='geomean_speedup.csv'>geomean_speedup.csv</a></p>\n")
        for eng in sorted(df["engine"].unique()):
            f.write(f"<h3>{eng}</h3>\n")
            for stem in (f"heat_{eng}", f"box_{eng}", f"rows_vs_{metric}_{eng}", f"ecdf_{eng}"):
                links = " | ".join(f"<a href='{stem}.{ext}'>{ext.upper()}</a>" for ext in fmts)
                preview = stem + (".png" if "png" in fmts else f".{fmts[0]}")
                f.write(
                    f"<div style='margin:8px 0'>{links}"
                    f"<br><img src='{preview}' style='max-width:920px;display:block;margin-top:4px'/></div>\n"
                )
        f.write("</body></html>")
    typer.echo(f"[viz-results] wrote outputs → {out_dir} (formats: {', '.join(fmts)})")


# -----------------------------------------------------------------------------
# GEN helpers (remain unchanged)
# -----------------------------------------------------------------------------
@dataclass
class _TemplateHelper:
    stats: Dict[str, ColumnStats]
    dists: Dict[str, UniDist]
    metadata: Dict[str, object]
    rng: random.Random
    target_sel: float
    renderer: TemplateRenderer
    dialect: str

    def __post_init__(self) -> None:
        for name, stat in self.stats.items():
            if name not in self.dists:
                dist = _dist_from_quantiles(stat)
                if dist is not None:
                    self.dists[name] = dist

    def generate_template(self, template_id: str) -> Optional[TemplateSpec]:
        """Dispatch template generation based on template id."""
        if template_id == "A":
            return self._template_a()
        if template_id == "B":
            return self._template_b()
        if template_id == "C":
            return self._template_c()
        if template_id == "D":
            return self._template_d()
        typer.echo(f"Unknown template {template_id}", err=True)
        return None

    def _numeric_columns(self) -> List[str]:
        return [
            name
            for name, stat in self.stats.items()
            if stat.kind in {"numeric", "datetime"}
        ]

    def _template_a(self) -> Optional[TemplateSpec]:
        numeric_cols = self._numeric_columns()
        if len(numeric_cols) < 2:
            return None
        candidates = self._correlated_pairs()
        selected: List[str] = []
        for col_a, col_b in candidates:
            if col_a in self.dists and col_b in self.dists:
                selected = [col_a, col_b]
                break
        if not selected:
            fallback = [col for col in numeric_cols if col in self.dists]
            selected = fallback[:2]
        if len(selected) < 2:
            return None

        dists = [self.dists[col] for col in selected]
        ranges = sample_copula(dists, target_sel=self.target_sel, rng=self.rng)
        return self.renderer.template_a(selected, ranges)

    def _template_b(self) -> Optional[TemplateSpec]:
        for column in self._numeric_columns():
            if column not in self.dists:
                continue
            bounds = sample_between(self.dists[column], self.target_sel, self.rng)
            return self.renderer.template_b(column, bounds)
        return None

    def _template_c(self) -> Optional[TemplateSpec]:
        ranked = sorted(
            self.stats.items(),
            key=lambda item: (item[1].cardinality or 0),
            reverse=True,
        )
        for name, stat in ranked:
            if stat.topk:
                value = sample_eq_from_topk(stat.topk, self.rng)
                return self.renderer.template_c(name, value)
        # fallback: pick quantile median for numeric
        for name, stat in ranked:
            if stat.quantiles:
                median = stat.quantiles.get(0.5) or next(iter(stat.quantiles.values()))
                return self.renderer.template_c(name, median)
        return None

    def _template_d(self) -> Optional[TemplateSpec]:
        # Placeholder using the same columns as template A.
        pair = self._correlated_pairs()
        if not pair:
            return None
        columns = pair[0]
        if columns[0] not in self.dists:
            return None
        bounds = sample_between(self.dists[columns[0]], self.target_sel, self.rng)
        filters = {columns[0]: bounds}
        dim_table = f"{self.renderer.table}_dim"
        return self.renderer.template_d(
            fact_column=columns[0],
            dim_table=dim_table,
            dim_key=columns[0],
            filters=filters,
        )

    def _correlated_pairs(self) -> List[Tuple[str, str]]:
        metadata = self.metadata or {}
        correlations = metadata.get("correlations", {}) if isinstance(metadata, dict) else {}
        pairs = correlations.get("top_pairs", []) if isinstance(correlations, dict) else []
        formatted: List[Tuple[str, str]] = []
        for entry in pairs:
            cols = entry.get("columns")
            if isinstance(cols, list) and len(cols) == 2:
                formatted.append((cols[0], cols[1]))
        if not formatted:
            numeric_cols = self._numeric_columns()
            formatted = [
                (numeric_cols[i], numeric_cols[i + 1])
                for i in range(0, max(0, len(numeric_cols) - 1))
            ]
        return formatted


def _dist_from_quantiles(stat: ColumnStats) -> Optional[UniDist]:
    """Approximate a distribution using available quantile statistics."""
    if not stat.quantiles:
        return None
    values: List[float] = []
    if stat.min is not None:
        try:
            values.append(float(stat.min))
        except (TypeError, ValueError):
            return None
    for value in stat.quantiles.values():
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    if stat.max is not None:
        try:
            values.append(float(stat.max))
        except (TypeError, ValueError):
            pass
    edges = sorted(set(values))
    if len(edges) < 2:
        return None
    counts = [1 for _ in range(len(edges) - 1)]
    return UniDist(counts, edges)


# Allow `python -m wlg.cli.main` direct execution (and `python -m wlg.cli` via __main__.py)
if __name__ == "__main__":
    app()
