"""SQL template helpers for workload generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple


@dataclass
class TemplateSpec:
    """Concrete template instance produced by a renderer."""

    name: str
    sql: str
    params: Dict[str, object]


class TemplateRenderer:
    """Render predefined query templates A/B/C/D."""

    def __init__(self, table: str, dialect: str = "sparksql") -> None:
        self.table = table
        self.dialect = dialect

    def template_a(
        self,
        columns: Sequence[str],
        ranges: Sequence[Tuple[float, float]],
    ) -> TemplateSpec:
        """Template A: multi-dimensional BETWEEN predicates (2-3 dimensions)."""

        predicates: List[str] = []
        params: Dict[str, object] = {}
        for idx, (column, bounds) in enumerate(zip(columns, ranges), start=1):
            lo, hi = bounds
            params[f"{column}_lo"] = lo
            params[f"{column}_hi"] = hi
            predicates.append(
                f"{column} BETWEEN :{column}_lo AND :{column}_hi"
            )
        where_clause = " AND ".join(predicates)
        sql = (
            f"SELECT * FROM {self.table} WHERE {where_clause}"
        )
        return TemplateSpec(name="A", sql=sql, params=params)

    def template_b(
        self,
        column: str,
        bounds: Tuple[float, float],
    ) -> TemplateSpec:
        """Template B: single-dimensional BETWEEN predicate."""

        lo, hi = bounds
        params = {f"{column}_lo": lo, f"{column}_hi": hi}
        sql = (
            f"SELECT * FROM {self.table} "
            f"WHERE {column} BETWEEN :{column}_lo AND :{column}_hi"
        )
        return TemplateSpec(name="B", sql=sql, params=params)

    def template_c(
        self,
        column: str,
        value: object,
    ) -> TemplateSpec:
        """Template C: equality predicate on high-cardinality column."""

        params = {column: value}
        sql = (
            f"SELECT * FROM {self.table} WHERE {column} = :{column}"
        )
        return TemplateSpec(name="C", sql=sql, params=params)

    def template_d(
        self,
        fact_column: str,
        dim_table: str,
        dim_key: str,
        filters: Dict[str, Tuple[float, float]] | None = None,
    ) -> TemplateSpec:
        """
        Template D: simple fact-to-dimension join with optional filters.

        The implementation keeps placeholders minimal to serve as an initial
        scaffold. Future revisions can extend this with richer join logic.
        """

        join_pred = f"{self.table}.{fact_column} = {dim_table}.{dim_key}"
        where_clauses: List[str] = []
        params: Dict[str, object] = {}
        if filters:
            for column, (lo, hi) in filters.items():
                key_lo = f"{column}_lo"
                key_hi = f"{column}_hi"
                params[key_lo] = lo
                params[key_hi] = hi
                where_clauses.append(
                    f"{dim_table}.{column} BETWEEN :{key_lo} AND :{key_hi}"
                )
        where_sql = ""
        if where_clauses:
            where_sql = " WHERE " + " AND ".join(where_clauses)
        sql = (
            f"SELECT {self.table}.* FROM {self.table} "
            f"JOIN {dim_table} ON {join_pred}"
            f"{where_sql}"
        )
        return TemplateSpec(name="D", sql=sql, params=params)

