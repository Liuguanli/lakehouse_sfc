from __future__ import annotations

import math
import re
from itertools import combinations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import yaml

# Optional dependency. The toolkit works without it (heuristic fallback parser).
try:  # pragma: no cover
    import sqlglot  # type: ignore
except Exception:  # pragma: no cover
    sqlglot = None


TPCH_PREFIX_TO_TABLE = {
    "c": "customer",
    "o": "orders",
    "l": "lineitem",
    "s": "supplier",
    "n": "nation",
    "r": "region",
    "p": "part",
    "ps": "partsupp",
}

PRED_WEIGHT_DEFAULT = {
    "eq": 5.0,
    "in": 4.0,
    "range": 3.0,
    "like": 2.0,
    "null": 1.0,
    "other": 1.0,
}

TABLE_ROLE_WEIGHT_DEFAULT = {
    "lineitem": 1.2,
}

SQL_KEYWORDS = {
    "select",
    "from",
    "join",
    "where",
    "group",
    "order",
    "by",
    "having",
    "limit",
    "and",
    "or",
    "not",
    "exists",
    "in",
    "between",
    "like",
    "is",
    "null",
    "as",
    "on",
    "case",
    "when",
    "then",
    "else",
    "end",
    "date",
    "extract",
    "interval",
    "distinct",
}

# TPC-H columns have stable prefixes like l_shipdate / o_orderkey.
COLUMN_REF_RE = re.compile(
    r"\b(?:(?P<qual>[A-Za-z_][\w$]*)\.)?(?P<col>(?:ps|[a-z])_[A-Za-z0-9_]+)\b",
    flags=re.IGNORECASE,
)

ALIAS_DEF_RE = re.compile(
    r"\b(?:from|join)\s+([A-Za-z_][\w$.]*)(?:\s+(?:as\s+)?([A-Za-z_][\w$]*))?",
    flags=re.IGNORECASE,
)

COMMA_TABLE_RE = re.compile(
    r"(?:^|,)\s*([A-Za-z_][\w$.]*)(?:\s+(?:as\s+)?([A-Za-z_][\w$]*))?(?=\s*(?:,|$))",
    flags=re.IGNORECASE,
)

TPCH_TABLE_ALIAS_RE = re.compile(
    r"\b(?:from|join)\s+(customer|orders|lineitem|supplier|nation|region|part|partsupp)\b"
    r"(?:\s+(?:as\s+)?([A-Za-z_][\w$]*))?"
    r"|,\s*(customer|orders|lineitem|supplier|nation|region|part|partsupp)\b"
    r"(?:\s+(?:as\s+)?([A-Za-z_][\w$]*))?",
    flags=re.IGNORECASE,
)


@dataclass
class AnalysisResult:
    predicates: pd.DataFrame
    table_column_summary: pd.DataFrame
    cooccurrence: pd.DataFrame
    query_summary: pd.DataFrame
    join_column_summary: pd.DataFrame
    meta: Dict[str, object]


def discover_sql_files(
    root: str | Path = "workloads",
    include_globs: Optional[Sequence[str]] = None,
    exclude_substrings: Optional[Sequence[str]] = None,
) -> List[Path]:
    root = Path(root)
    if include_globs:
        files: List[Path] = []
        for pat in include_globs:
            files.extend(sorted(Path(".").glob(pat)))
    else:
        files = sorted(root.rglob("*.sql"))
    if exclude_substrings:
        files = [f for f in files if not any(s in str(f) for s in exclude_substrings)]
    return [f for f in files if f.is_file()]


def read_sql_files(files: Sequence[str | Path]) -> pd.DataFrame:
    rows = []
    for p in files:
        path = Path(p)
        try:
            sql = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            sql = path.read_text(encoding="latin-1")
        rows.append({"query_path": str(path), "query_name": path.name, "sql": sql})
    return pd.DataFrame(rows)


def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
    return sql


def _is_word_boundary(s: str, idx: int) -> bool:
    if idx < 0 or idx >= len(s):
        return True
    return not (s[idx].isalnum() or s[idx] == "_")


def _read_word(s: str, i: int) -> Tuple[Optional[str], int]:
    if i >= len(s) or not (s[i].isalpha() or s[i] == "_"):
        return None, i
    j = i + 1
    while j < len(s) and (s[j].isalnum() or s[j] == "_"):
        j += 1
    return s[i:j].lower(), j


def _skip_string(s: str, i: int) -> int:
    quote = s[i]
    i += 1
    while i < len(s):
        if s[i] == quote:
            if i + 1 < len(s) and s[i + 1] == quote:
                i += 2
                continue
            return i + 1
        i += 1
    return i


def _match_phrase_at(s: str, i: int, phrase: str) -> bool:
    phrase_l = phrase.lower()
    if s[i : i + len(phrase_l)].lower() != phrase_l:
        return False
    return _is_word_boundary(s, i - 1) and _is_word_boundary(s, i + len(phrase_l))


def _find_clause_spans(sql: str, clause_keyword: str) -> List[Tuple[int, int]]:
    """
    Find clause expressions for keyword (e.g., WHERE / ON) using a depth-aware scan.
    Returns spans containing clause body only (without the keyword).
    """
    clause_keyword = clause_keyword.lower()
    stop_phrases = {
        "from": ["where", "group by", "order by", "having", "limit", "union", "qualify", "window"],
        "where": ["group by", "order by", "having", "limit", "union", "qualify", "window"],
        "on": ["join", "where", "group by", "order by", "having", "limit", "union"],
    }.get(clause_keyword, ["group by", "order by", "having", "limit", "union"])

    spans: List[Tuple[int, int]] = []
    i = 0
    depth = 0
    while i < len(sql):
        ch = sql[i]
        if ch in ("'", '"'):
            i = _skip_string(sql, i)
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue

        word, j = _read_word(sql, i)
        if word != clause_keyword or not _is_word_boundary(sql, i - 1):
            i = j if word else i + 1
            continue

        start_depth = depth
        start = j
        # Skip whitespace after keyword
        while start < len(sql) and sql[start].isspace():
            start += 1

        k = start
        inner_depth = depth
        while k < len(sql):
            ch2 = sql[k]
            if ch2 in ("'", '"'):
                k = _skip_string(sql, k)
                continue
            if ch2 == "(":
                inner_depth += 1
                k += 1
                continue
            if ch2 == ")":
                if inner_depth == start_depth:
                    break
                inner_depth -= 1
                k += 1
                continue
            if ch2 == ";" and inner_depth == start_depth:
                break
            if inner_depth == start_depth:
                matched_stop = False
                for sp in stop_phrases:
                    if _match_phrase_at(sql, k, sp):
                        matched_stop = True
                        break
                if matched_stop:
                    break
            k += 1

        spans.append((start, k))
        i = k
    return spans


def _strip_outer_parens(expr: str) -> str:
    expr = expr.strip()
    while expr.startswith("(") and expr.endswith(")"):
        depth = 0
        valid = True
        for idx, ch in enumerate(expr):
            if ch in ("'", '"'):
                # coarse: do not strip based on quotes here; break to be safe
                valid = False
                break
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and idx != len(expr) - 1:
                    valid = False
                    break
        if not valid or depth != 0:
            break
        expr = expr[1:-1].strip()
    return expr


def _split_top_level_bool(expr: str, sep: str) -> List[str]:
    sep = sep.lower()
    out: List[str] = []
    i = 0
    depth = 0
    start = 0
    while i < len(expr):
        ch = expr[i]
        if ch in ("'", '"'):
            i = _skip_string(expr, i)
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0 and _match_phrase_at(expr, i, sep):
            out.append(expr[start:i].strip())
            i += len(sep)
            start = i
            continue
        i += 1
    tail = expr[start:].strip()
    if tail:
        out.append(tail)
    return out


def _flatten_boolean_predicates(expr: str, in_or: bool = False) -> List[Tuple[str, bool]]:
    expr = _strip_outer_parens(expr)
    if not expr:
        return []
    or_parts = _split_top_level_bool(expr, "or")
    if len(or_parts) > 1:
        out: List[Tuple[str, bool]] = []
        for p in or_parts:
            out.extend(_flatten_boolean_predicates(p, in_or=True))
        return out
    and_parts = _split_top_level_bool(expr, "and")
    if len(and_parts) > 1:
        out = []
        for p in and_parts:
            out.extend(_flatten_boolean_predicates(p, in_or=in_or))
        return out
    return [(expr.strip(), in_or)]


def _extract_alias_map_heuristic(sql: str) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}

    sql_no_comments = strip_sql_comments(sql)
    # TPC-H specific fast path (avoids false matches like "extract(year from l_shipdate)")
    for m in TPCH_TABLE_ALIAS_RE.finditer(sql_no_comments):
        table = (m.group(1) or m.group(3) or "").lower()
        alias = (m.group(2) or m.group(4) or "").lower()
        if not table:
            continue
        alias_map[table] = table
        if alias and alias not in SQL_KEYWORDS:
            alias_map[alias] = table

    # FROM/JOIN direct captures
    for m in ALIAS_DEF_RE.finditer(sql_no_comments):
        table = (m.group(1) or "").strip()
        alias = (m.group(2) or "").strip()
        if not table or table.startswith("("):
            continue
        table_name = table.split(".")[-1].lower()
        if table_name in SQL_KEYWORDS:
            continue
        # Skip obvious column-like tokens (e.g., "extract(year from l_shipdate)")
        if re.match(r"^(?:ps|[a-z])_[a-z0-9_]+$", table_name):
            continue
        alias_map[table_name] = table_name
        if alias and alias.lower() not in SQL_KEYWORDS:
            alias_map[alias.lower()] = table_name

    # Also parse comma-join entries inside FROM clauses (old-style TPCH)
    for s, e in _find_clause_spans(sql_no_comments, "from"):
        frag = sql_no_comments[s:e]
        for m in COMMA_TABLE_RE.finditer(frag):
            table = m.group(1)
            alias = m.group(2)
            if not table or table.startswith("("):
                continue
            table_name = table.split(".")[-1].lower()
            if table_name in SQL_KEYWORDS:
                continue
            if re.match(r"^(?:ps|[a-z])_[a-z0-9_]+$", table_name):
                continue
            alias_map[table_name] = table_name
            if alias and alias.lower() not in SQL_KEYWORDS:
                alias_map[alias.lower()] = table_name
    return alias_map


def _find_from_spans(sql: str) -> List[Tuple[int, int]]:
    return _find_clause_spans(sql, "from")


def _find_where_exprs(sql: str) -> List[str]:
    return [sql[s:e].strip() for s, e in _find_clause_spans(sql, "where")]


def _find_on_exprs(sql: str) -> List[str]:
    return [sql[s:e].strip() for s, e in _find_clause_spans(sql, "on")]


def _extract_column_refs(pred: str, alias_map: Dict[str, str]) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    seen = set()
    for m in COLUMN_REF_RE.finditer(pred):
        qual = m.group("qual")
        col = m.group("col")
        if not col:
            continue
        col_l = col.lower()
        qual_l = qual.lower() if qual else ""
        # Resolve table
        table = None
        if qual_l:
            table = alias_map.get(qual_l) or alias_map.get(qual) or qual_l
        else:
            prefix = "ps" if col_l.startswith("ps_") else col_l.split("_", 1)[0]
            table = TPCH_PREFIX_TO_TABLE.get(prefix, "_unknown")
        key = (str(table), col_l)
        if key in seen:
            continue
        seen.add(key)
        out.append((str(table), col_l, f"{table}.{col_l}"))
    return out


def _classify_predicate_type(pred: str) -> str:
    p = pred.strip().lower()
    if re.search(r"\bis\s+not\s+null\b|\bis\s+null\b", p):
        return "null"
    if re.search(r"\blike\b", p):
        return "like"
    if re.search(r"\bbetween\b", p):
        return "range"
    if re.search(r"\bin\s*\(", p):
        return "in"
    if re.search(r"(<=|>=|<>|!=|<|>)", p):
        # EQ should win only if there are no other comparison operators
        if re.search(r"(?<![<>=!])=(?!=)", p) and not re.search(r"(<=|>=|<>|!=|<|>)", p):
            return "eq"
        return "range"
    if re.search(r"(?<![<>=!])=(?!=)", p):
        return "eq"
    return "other"


def _looks_like_column_expr(expr: str) -> bool:
    return bool(COLUMN_REF_RE.search(expr))


def _split_simple_comparison(pred: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # Prefer BETWEEN / IN / IS NULL handling before generic comparison
    p = pred.strip()
    m = re.search(r"(?is)^(.*?)\s+between\s+(.*?)\s+and\s+(.*)$", p)
    if m:
        return m.group(1).strip(), "between", m.group(2).strip() + " AND " + m.group(3).strip()
    m = re.search(r"(?is)^(.*?)\s+in\s*\((.*)\)$", p)
    if m:
        return m.group(1).strip(), "in", "(" + m.group(2).strip() + ")"
    m = re.search(r"(?is)^(.*?)\s+is\s+(not\s+)?null$", p)
    if m:
        return m.group(1).strip(), "is null", "NULL"
    for op in ("<=", ">=", "<>", "!=", "=", "<", ">"):
        parts = p.split(op, 1)
        if len(parts) == 2:
            return parts[0].strip(), op, parts[1].strip()
    return None, None, None


def _predicate_is_join(pred: str, alias_map: Dict[str, str]) -> bool:
    left, op, right = _split_simple_comparison(pred)
    if not left or not right or op in {"between", "in", "is null"}:
        return False
    return _looks_like_column_expr(left) and _looks_like_column_expr(right)


def _score_for_event(
    pred_type: str,
    in_or: bool,
    table: str,
    pred_weight: Dict[str, float],
    table_role_weight: Optional[Dict[str, float]],
) -> float:
    s = float(pred_weight.get(pred_type, pred_weight.get("other", 1.0)))
    if in_or:
        s *= 0.5
    if table_role_weight:
        s *= float(table_role_weight.get(table, 1.0))
    return s


def _analyze_query_heuristic(
    query_path: str,
    sql: str,
    pred_weight: Dict[str, float],
    table_role_weight: Optional[Dict[str, float]],
) -> Tuple[List[Dict], List[Dict]]:
    sql_no_comments = strip_sql_comments(sql)
    alias_map = _extract_alias_map_heuristic(sql_no_comments)

    pred_rows: List[Dict] = []
    query_rows: List[Dict] = []
    query_rows.append(
        {
            "query_path": query_path,
            "query_name": Path(query_path).name,
            "num_aliases": len(alias_map),
            "aliases": dict(sorted(alias_map.items())),
        }
    )

    # WHERE and ON clauses
    for clause_name, exprs in (("where", _find_where_exprs(sql_no_comments)), ("on", _find_on_exprs(sql_no_comments))):
        for clause_expr in exprs:
            for pred_text, in_or in _flatten_boolean_predicates(clause_expr):
                pred_norm = _strip_outer_parens(pred_text.strip())
                if not pred_norm:
                    continue
                pred_type = _classify_predicate_type(pred_norm)
                is_join = _predicate_is_join(pred_norm, alias_map)
                cols = _extract_column_refs(pred_norm, alias_map)
                if not cols:
                    continue
                for table, col, table_col in cols:
                    score = _score_for_event(pred_type, in_or, table, pred_weight, table_role_weight)
                    pred_rows.append(
                        {
                            "query_path": query_path,
                            "query_name": Path(query_path).name,
                            "clause": clause_name,
                            "predicate": pred_norm,
                            "pred_type": pred_type,
                            "in_or": bool(in_or),
                            "is_join": bool(is_join),
                            "is_filter": not bool(is_join),
                            "table": table,
                            "column": col,
                            "table_column": table_col,
                            "score": score,
                            "parser": "heuristic",
                        }
                    )
    return pred_rows, query_rows


def analyze_workload(
    sql_df: pd.DataFrame,
    pred_weight: Optional[Dict[str, float]] = None,
    table_role_weight: Optional[Dict[str, float]] = None,
    use_sqlglot_if_available: bool = False,
) -> AnalysisResult:
    """
    Analyze SQL workload and produce per-column predicate stats for layout decisions.

    Current implementation defaults to a heuristic parser because sqlglot is optional
    and may not be installed in the notebook environment.
    """
    pred_weight = dict(PRED_WEIGHT_DEFAULT if pred_weight is None else pred_weight)
    if table_role_weight is None:
        table_role_weight = dict(TABLE_ROLE_WEIGHT_DEFAULT)

    pred_rows: List[Dict] = []
    query_rows: List[Dict] = []

    for row in sql_df.to_dict("records"):
        p_rows, q_rows = _analyze_query_heuristic(
            query_path=row["query_path"],
            sql=row["sql"],
            pred_weight=pred_weight,
            table_role_weight=table_role_weight,
        )
        pred_rows.extend(p_rows)
        query_rows.extend(q_rows)

    predicates = pd.DataFrame(pred_rows)
    if predicates.empty:
        empty = pd.DataFrame()
        return AnalysisResult(
            predicates=empty,
            table_column_summary=empty,
            cooccurrence=empty,
            query_summary=pd.DataFrame(query_rows),
            join_column_summary=empty,
            meta={
                "parser": "heuristic",
                "sqlglot_available": sqlglot is not None,
                "sqlglot_used": False,
            },
        )

    # Per-table-column summary
    grp = predicates.groupby(["table", "column", "table_column"], dropna=False)
    summary = grp.agg(
        occurrences=("column", "size"),
        queries=("query_path", "nunique"),
        score=("score", "sum"),
        filters=("is_filter", "sum"),
        joins=("is_join", "sum"),
        or_occurrences=("in_or", "sum"),
    ).reset_index()

    # Predicate type breakdown columns
    pred_type_counts = (
        predicates.pivot_table(
            index=["table", "column", "table_column"],
            columns="pred_type",
            values="query_path",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
    )
    pred_type_counts.columns = [
        c if isinstance(c, str) else c[1] if isinstance(c, tuple) else str(c)
        for c in pred_type_counts.columns
    ]
    summary = summary.merge(pred_type_counts, on=["table", "column", "table_column"], how="left")
    for c in ["eq", "in", "range", "like", "null", "other"]:
        if c not in summary.columns:
            summary[c] = 0
        summary[f"pred_{c}_ratio"] = summary[c] / summary["occurrences"].clip(lower=1)

    # Co-occurrence (filter predicates only) per query per table
    filt = predicates[predicates["is_filter"]].copy()
    co_rows: List[Dict] = []
    if not filt.empty:
        for (qp, tbl), g in filt.groupby(["query_path", "table"]):
            cols = sorted(set(g["column"]))
            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    co_rows.append(
                        {
                            "query_path": qp,
                            "table": tbl,
                            "col_a": cols[i],
                            "col_b": cols[j],
                            "pair": f"{cols[i]}|{cols[j]}",
                        }
                    )
    cooccurrence = pd.DataFrame(co_rows)
    if not cooccurrence.empty:
        cooccurrence = (
            cooccurrence.groupby(["table", "col_a", "col_b", "pair"], dropna=False)
            .agg(count=("query_path", "nunique"))
            .reset_index()
            .sort_values(["table", "count", "pair"], ascending=[True, False, True])
        )

    join_summary = (
        predicates[predicates["is_join"]]
        .groupby(["table", "column", "table_column"], dropna=False)
        .agg(join_occurrences=("column", "size"), join_queries=("query_path", "nunique"))
        .reset_index()
        .sort_values(["join_occurrences", "join_queries"], ascending=False)
    )

    summary = summary.sort_values(["table", "score", "occurrences"], ascending=[True, False, False]).reset_index(drop=True)
    query_summary = pd.DataFrame(query_rows)

    return AnalysisResult(
        predicates=predicates,
        table_column_summary=summary,
        cooccurrence=cooccurrence,
        query_summary=query_summary,
        join_column_summary=join_summary,
        meta={
            "parser": "heuristic",
            "sqlglot_available": sqlglot is not None,
            "sqlglot_used": False and use_sqlglot_if_available,
            "pred_weight": pred_weight,
            "table_role_weight": table_role_weight,
        },
    )


def load_stats_yaml(stats_path: str | Path) -> pd.DataFrame:
    stats_path = Path(stats_path)
    raw = yaml.safe_load(stats_path.read_text(encoding="utf-8")) or {}
    cols = raw.get("columns", {})
    rows = []
    for col, info in cols.items():
        info = info or {}
        col_l = str(col).lower()
        prefix = "ps" if col_l.startswith("ps_") else col_l.split("_", 1)[0]
        table = TPCH_PREFIX_TO_TABLE.get(prefix, "_unknown")
        count = _to_num(info.get("count"))
        cardinality = _to_num(info.get("cardinality"))
        nulls = _to_num(info.get("nulls"))
        rows.append(
            {
                "table": table,
                "column": col_l,
                "table_column": f"{table}.{col_l}",
                "kind": info.get("kind"),
                "count": count,
                "cardinality": cardinality,
                "nulls": nulls,
                "null_ratio": (nulls / count) if _is_pos(count) and nulls is not None else None,
                "unique_ratio": (cardinality / count) if _is_pos(count) and cardinality is not None else None,
                "is_unique_like": (cardinality / count) >= 0.95 if _is_pos(count) and cardinality is not None else None,
                "is_low_cardinality": cardinality <= 32 if cardinality is not None else None,
                "topk": info.get("topk"),
                "hist": info.get("hist"),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["distinctness_bucket"] = df["unique_ratio"].apply(_distinctness_bucket)
    return df


def _to_num(v):
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        x = float(v)
        if math.isfinite(x):
            return x
    except Exception:
        return None
    return None


def _is_pos(v) -> bool:
    return v is not None and v > 0


def _distinctness_bucket(r: Optional[float]) -> str:
    if r is None or pd.isna(r):
        return "unknown"
    if r <= 0.001:
        return "very_low"
    if r <= 0.01:
        return "low"
    if r <= 0.1:
        return "medium"
    if r <= 0.5:
        return "high"
    return "near_unique"


def enrich_summary_with_stats(summary_df: pd.DataFrame, stats_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df is None or summary_df.empty:
        return summary_df.copy()
    if stats_df is None or stats_df.empty:
        out = summary_df.copy()
        out["kind"] = None
        out["cardinality"] = None
        out["count"] = None
        out["unique_ratio"] = None
        return out
    keep_cols = [
        "table",
        "column",
        "table_column",
        "kind",
        "count",
        "cardinality",
        "null_ratio",
        "unique_ratio",
        "distinctness_bucket",
        "is_low_cardinality",
        "is_unique_like",
    ]
    return summary_df.merge(stats_df[keep_cols], on=["table", "column", "table_column"], how="left")


def summarize_tables_for_display(summary_df: pd.DataFrame, top_k: int = 10) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df
    rows = []
    for tbl, g in summary_df.groupby("table", sort=True):
        rows.append(g.sort_values("score", ascending=False).head(top_k))
    return pd.concat(rows, ignore_index=True) if rows else summary_df.head(0)


def plot_top_columns(
    summary_df: pd.DataFrame,
    table: str,
    top_k: int = 10,
    score_col: str = "score",
    figsize: Tuple[int, int] = (10, 5),
):
    import matplotlib.pyplot as plt
    import seaborn as sns

    g = summary_df[summary_df["table"] == table].sort_values(score_col, ascending=False).head(top_k)
    if g.empty:
        raise ValueError(f"No rows for table={table}")
    plt.figure(figsize=figsize)
    sns.barplot(data=g, x=score_col, y="column", orient="h", color="#2E7D32")
    plt.title(f"Top {top_k} layout candidate columns ({table})")
    plt.xlabel(score_col)
    plt.ylabel("")
    plt.tight_layout()
    return plt.gca()


def plot_predicate_mix(
    summary_df: pd.DataFrame,
    table: str,
    top_k: int = 10,
    figsize: Tuple[int, int] = (12, 6),
):
    import matplotlib.pyplot as plt

    cols = ["eq", "in", "range", "like", "null", "other"]
    g = summary_df[summary_df["table"] == table].sort_values("score", ascending=False).head(top_k).copy()
    if g.empty:
        raise ValueError(f"No rows for table={table}")
    plot_df = g[["column"] + cols].set_index("column")
    ax = plot_df.plot(kind="barh", stacked=True, figsize=figsize, colormap="tab20c")
    ax.invert_yaxis()
    ax.set_title(f"Predicate type mix ({table})")
    ax.set_xlabel("Occurrences")
    ax.set_ylabel("")
    plt.tight_layout()
    return ax


def plot_score_vs_distinctness(
    enriched_df: pd.DataFrame,
    table: Optional[str] = None,
    figsize: Tuple[int, int] = (9, 6),
):
    import matplotlib.pyplot as plt
    import seaborn as sns

    g = enriched_df.copy()
    if table:
        g = g[g["table"] == table]
    g = g[g["unique_ratio"].notna()].copy()
    if g.empty:
        raise ValueError("No rows with unique_ratio available")
    plt.figure(figsize=figsize)
    sns.scatterplot(
        data=g,
        x="unique_ratio",
        y="score",
        hue="kind" if "kind" in g.columns else None,
        size="occurrences",
        sizes=(30, 300),
        alpha=0.8,
    )
    for _, r in g.nlargest(min(12, len(g)), "score").iterrows():
        plt.text(r["unique_ratio"], r["score"], r["column"], fontsize=8, alpha=0.85)
    plt.xscale("log")
    plt.xlabel("Distinctness ratio (cardinality / count, log scale)")
    plt.ylabel("Layout score")
    title = "Layout score vs distinctness"
    if table:
        title += f" ({table})"
    plt.title(title)
    plt.tight_layout()
    return plt.gca()


def plot_cooccurrence_heatmap(
    cooccurrence_df: pd.DataFrame,
    table: str,
    min_count: int = 1,
    figsize: Tuple[int, int] = (8, 6),
):
    import matplotlib.pyplot as plt
    import seaborn as sns

    g = cooccurrence_df[cooccurrence_df["table"] == table].copy()
    if g.empty:
        raise ValueError(f"No cooccurrence rows for table={table}")
    g = g[g["count"] >= min_count]
    if g.empty:
        raise ValueError(f"No cooccurrence rows for table={table} with min_count={min_count}")
    cols = sorted(set(g["col_a"]).union(set(g["col_b"])))
    mat = pd.DataFrame(0, index=cols, columns=cols, dtype=int)
    for _, r in g.iterrows():
        mat.loc[r["col_a"], r["col_b"]] = int(r["count"])
        mat.loc[r["col_b"], r["col_a"]] = int(r["count"])
    plt.figure(figsize=figsize)
    sns.heatmap(mat, annot=True, fmt="d", cmap="YlGnBu")
    plt.title(f"Filter-column co-occurrence ({table})")
    plt.tight_layout()
    return plt.gca()


def analyze_sql_and_stats(
    sql_files: Sequence[str | Path],
    stats_yaml: Optional[str | Path] = None,
    pred_weight: Optional[Dict[str, float]] = None,
    table_role_weight: Optional[Dict[str, float]] = None,
) -> Dict[str, pd.DataFrame | AnalysisResult]:
    sql_df = read_sql_files(sql_files)
    analysis = analyze_workload(
        sql_df,
        pred_weight=pred_weight,
        table_role_weight=table_role_weight,
    )
    stats_df = load_stats_yaml(stats_yaml) if stats_yaml else pd.DataFrame()
    enriched = enrich_summary_with_stats(analysis.table_column_summary, stats_df) if not analysis.table_column_summary.empty else analysis.table_column_summary
    return {
        "sql_df": sql_df,
        "analysis": analysis,
        "predicates": analysis.predicates,
        "summary": analysis.table_column_summary,
        "summary_enriched": enriched,
        "cooccurrence": analysis.cooccurrence,
        "join_summary": analysis.join_column_summary,
        "stats": stats_df,
    }


def example_tpch_file_selection(
    stream: Optional[str] = "stream_1",
    root: str | Path = "workloads/rq6_tpch_all",
) -> List[Path]:
    root = Path(root)
    if stream:
        return sorted((root / stream).glob("query_*.sql"))
    return sorted(root.rglob("query_*.sql"))


def recommend_layout_tables(
    summary_enriched: pd.DataFrame,
    cooccurrence_df: Optional[pd.DataFrame] = None,
    predicates_df: Optional[pd.DataFrame] = None,
    min_candidate_columns: int = 2,
    top_k: int = 8,
) -> pd.DataFrame:
    """
    Rank tables by expected payoff for layout tuning.

    Heuristic: high predicate score + repeated filter usage + co-occurring filter columns
    indicates a good target for multi-column layout design.
    """
    if summary_enriched is None or summary_enriched.empty:
        return pd.DataFrame()

    rows: List[Dict[str, object]] = []
    cooccurrence_df = cooccurrence_df if cooccurrence_df is not None else pd.DataFrame()
    predicates_df = predicates_df if predicates_df is not None else pd.DataFrame()

    for table, g in summary_enriched.groupby("table", sort=True):
        g = g.copy()
        # "Candidate" columns: columns that appear in filters or joins and are not trivially useless.
        cand = g[(g["filters"].fillna(0) > 0) | (g["joins"].fillna(0) > 0)].copy()
        if "is_unique_like" in cand.columns:
            # Keep join keys even if unique-like; otherwise unique-like filter cols are usually weak layout anchors.
            cand = cand[(cand["is_unique_like"] != True) | (cand["joins"].fillna(0) > 0)]  # noqa: E712

        n_candidate_cols = int(cand["column"].nunique()) if not cand.empty else 0

        tbl_pair = cooccurrence_df[cooccurrence_df["table"] == table] if not cooccurrence_df.empty else pd.DataFrame()
        pair_count_sum = float(tbl_pair["count"].sum()) if not tbl_pair.empty else 0.0
        pair_count_max = float(tbl_pair["count"].max()) if not tbl_pair.empty else 0.0

        if not predicates_df.empty and "table" in predicates_df.columns:
            p_tbl = predicates_df[predicates_df["table"] == table]
            query_coverage = int(p_tbl["query_path"].nunique()) if not p_tbl.empty else 0
            filter_query_coverage = int(p_tbl[p_tbl["is_filter"] == True]["query_path"].nunique()) if not p_tbl.empty else 0  # noqa: E712
        else:
            query_coverage = int(g["queries"].max()) if "queries" in g.columns and not g.empty else 0
            filter_query_coverage = 0

        total_score = float(g["score"].sum())
        filter_score = float((g["score"] * g["filters"].fillna(0).clip(lower=0)).sum())
        filter_events = float(g["filters"].sum()) if "filters" in g.columns else 0.0
        join_events = float(g["joins"].sum()) if "joins" in g.columns else 0.0

        # Favor tables with strong predicates and actual multi-column opportunities.
        layout_opportunity_score = (
            total_score
            + 0.25 * filter_events
            + 0.10 * join_events
            + 0.75 * pair_count_sum
            + 1.50 * pair_count_max
        )

        top_cols = (
            cand.sort_values(["score", "filters", "joins"], ascending=False)
            .head(5)["column"]
            .tolist()
            if not cand.empty
            else []
        )

        rows.append(
            {
                "table": table,
                "layout_opportunity_score": layout_opportunity_score,
                "total_score": total_score,
                "filter_score_proxy": filter_score,
                "filter_events": int(filter_events),
                "join_events": int(join_events),
                "candidate_columns": n_candidate_cols,
                "cooccur_pairs": int(len(tbl_pair)) if not tbl_pair.empty else 0,
                "cooccur_count_sum": pair_count_sum,
                "cooccur_count_max": pair_count_max,
                "query_coverage": query_coverage,
                "filter_query_coverage": filter_query_coverage,
                "top_columns_preview": ", ".join(top_cols),
                "eligible_for_multicol_layout": n_candidate_cols >= min_candidate_columns,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values(
        ["eligible_for_multicol_layout", "layout_opportunity_score", "candidate_columns", "query_coverage"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    return out.head(top_k) if top_k and top_k > 0 else out


def _column_layout_anchor_score(row: pd.Series) -> float:
    """
    Heuristic per-column score for being part of a layout key.
    Balances predicate score, filter frequency, and distinctness.
    """
    base = float(row.get("score", 0.0) or 0.0)
    filters = float(row.get("filters", 0.0) or 0.0)
    joins = float(row.get("joins", 0.0) or 0.0)
    pred_range = float(row.get("range", 0.0) or 0.0)
    pred_eq = float(row.get("eq", 0.0) or 0.0)
    pred_in = float(row.get("in", 0.0) or 0.0)

    score = base + 0.5 * filters + 0.25 * joins + 0.4 * pred_range + 0.2 * pred_eq + 0.2 * pred_in

    kind = str(row.get("kind", "") or "").lower()
    if kind in {"date", "datetime", "datetime64[ns]", "timestamp"}:
        score *= 1.15
    elif kind in {"double", "float64", "float32", "int", "int32", "int64", "long", "number", "decimal"}:
        score *= 1.05

    unique_ratio = row.get("unique_ratio")
    try:
        ur = float(unique_ratio) if unique_ratio is not None and not pd.isna(unique_ratio) else None
    except Exception:
        ur = None

    if ur is not None:
        # Too-low cardinality alone is usually weak as a leading layout key.
        if ur < 1e-4:
            score *= 0.55
        elif ur < 1e-3:
            score *= 0.70
        elif ur < 1e-2:
            score *= 0.85
        elif ur > 0.95 and joins <= 0:
            # near-unique non-join columns often give poor pruning value
            score *= 0.75
    return score


def recommend_layout_column_sets(
    summary_enriched: pd.DataFrame,
    cooccurrence_df: pd.DataFrame,
    predicates_df: Optional[pd.DataFrame],
    table: str,
    combo_sizes: Sequence[int] = (2, 3),
    top_n: int = 12,
    max_candidate_pool: int = 12,
    min_anchor_score: float = 0.0,
) -> pd.DataFrame:
    """
    Recommend multi-column layout candidates for one table.

    Output rows are column combinations (size 2/3 by default) with a heuristic score and
    simple evidence features (pair co-occurrence, query coverage).
    """
    if summary_enriched is None or summary_enriched.empty:
        return pd.DataFrame()

    tbl = summary_enriched[summary_enriched["table"] == table].copy()
    if tbl.empty:
        return pd.DataFrame()

    # Build candidate pool
    tbl["anchor_score"] = tbl.apply(_column_layout_anchor_score, axis=1)
    cand = tbl[(tbl["filters"].fillna(0) > 0) | (tbl["joins"].fillna(0) > 0)].copy()
    if "is_unique_like" in cand.columns:
        cand = cand[(cand["is_unique_like"] != True) | (cand["joins"].fillna(0) > 0)]  # noqa: E712
    cand = cand[cand["anchor_score"] >= min_anchor_score]
    cand = cand.sort_values(["anchor_score", "score", "filters"], ascending=False).head(max_candidate_pool)
    if cand.empty or cand["column"].nunique() < 2:
        return pd.DataFrame()

    # Co-occurrence lookup
    pair_counts: Dict[Tuple[str, str], float] = {}
    if cooccurrence_df is not None and not cooccurrence_df.empty:
        cg = cooccurrence_df[cooccurrence_df["table"] == table].copy()
        for _, r in cg.iterrows():
            a, b = sorted([str(r["col_a"]), str(r["col_b"])])
            pair_counts[(a, b)] = float(r.get("count", 0) or 0)

    # Query coverage lookup per column
    col_query_sets: Dict[str, set] = {}
    if predicates_df is not None and not predicates_df.empty:
        pg = predicates_df[(predicates_df["table"] == table) & (predicates_df["is_filter"] == True)].copy()  # noqa: E712
        for col, g in pg.groupby("column"):
            col_query_sets[str(col)] = set(g["query_path"].tolist())

    # Lookup metadata
    col_meta = cand.set_index("column").to_dict("index")
    cols = cand["column"].dropna().astype(str).unique().tolist()

    out_rows: List[Dict[str, object]] = []
    valid_sizes = sorted({int(k) for k in combo_sizes if int(k) >= 2})
    for k in valid_sizes:
        if len(cols) < k:
            continue
        for combo in combinations(cols, k):
            combo = tuple(sorted(combo))
            singleton_score = sum(float(col_meta[c]["anchor_score"]) for c in combo)
            pair_bonus = 0.0
            pair_evidence_count = 0
            for a, b in combinations(combo, 2):
                cnt = float(pair_counts.get(tuple(sorted((a, b))), 0.0))
                if cnt > 0:
                    pair_evidence_count += 1
                pair_bonus += 1.25 * cnt

            qset = set()
            for c in combo:
                qset |= set(col_query_sets.get(c, set()))
            query_coverage = len(qset)

            # Encourage combos whose first two columns have stronger anchors.
            ordered = sorted(combo, key=lambda c: float(col_meta[c]["anchor_score"]), reverse=True)
            leading_strength = float(col_meta[ordered[0]]["anchor_score"]) + 0.6 * float(col_meta[ordered[1]]["anchor_score"])

            combo_score = singleton_score + pair_bonus + 0.75 * query_coverage + 0.15 * leading_strength

            out_rows.append(
                {
                    "table": table,
                    "k": k,
                    "columns": combo,
                    "columns_csv": ",".join(combo),
                    "suggested_order": ",".join(ordered),  # simple default for linear/sort-based layouts
                    "combo_score": combo_score,
                    "singleton_score": singleton_score,
                    "pair_bonus": pair_bonus,
                    "pair_evidence_edges": pair_evidence_count,
                    "query_coverage": query_coverage,
                    "lead_anchor": ordered[0],
                    "lead_anchor_score": float(col_meta[ordered[0]]["anchor_score"]),
                    "second_anchor": ordered[1],
                    "second_anchor_score": float(col_meta[ordered[1]]["anchor_score"]),
                }
            )

    out = pd.DataFrame(out_rows)
    if out.empty:
        return out
    out = out.sort_values(
        ["combo_score", "pair_bonus", "query_coverage", "k"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    return out.head(top_n) if top_n and top_n > 0 else out


def recommend_layout_candidates(
    result: Dict[str, object],
    top_tables: int = 5,
    combo_sizes: Sequence[int] = (2, 3),
    combos_per_table: int = 8,
) -> Dict[str, pd.DataFrame]:
    """
    Convenience wrapper over `analyze_sql_and_stats(...)` output.

    Returns:
      - table_recommendations
      - column_set_recommendations (stacked for selected tables)
    """
    summary_enriched = result.get("summary_enriched")
    cooccurrence = result.get("cooccurrence")
    predicates = result.get("predicates")

    if not isinstance(summary_enriched, pd.DataFrame):
        raise TypeError("result['summary_enriched'] must be a DataFrame")
    if not isinstance(cooccurrence, pd.DataFrame):
        cooccurrence = pd.DataFrame()
    if not isinstance(predicates, pd.DataFrame):
        predicates = pd.DataFrame()

    tbl_rec = recommend_layout_tables(
        summary_enriched=summary_enriched,
        cooccurrence_df=cooccurrence,
        predicates_df=predicates,
        top_k=top_tables,
    )

    if tbl_rec.empty or "eligible_for_multicol_layout" not in tbl_rec.columns:
        return {
            "table_recommendations": tbl_rec,
            "column_set_recommendations": pd.DataFrame(),
        }

    combo_rows = []
    for table in tbl_rec[tbl_rec["eligible_for_multicol_layout"] == True]["table"].tolist():  # noqa: E712
        combos = recommend_layout_column_sets(
            summary_enriched=summary_enriched,
            cooccurrence_df=cooccurrence,
            predicates_df=predicates,
            table=table,
            combo_sizes=combo_sizes,
            top_n=combos_per_table,
        )
        if not combos.empty:
            combo_rows.append(combos)

    combo_df = pd.concat(combo_rows, ignore_index=True) if combo_rows else pd.DataFrame()
    return {
        "table_recommendations": tbl_rec,
        "column_set_recommendations": combo_df,
    }
