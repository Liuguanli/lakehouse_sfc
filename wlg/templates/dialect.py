"""Dialect-specific SQL adjustments."""

from __future__ import annotations


def format_sql(sql: str, dialect: str) -> str:
    """
    Format SQL text for the requested dialect.

    The initial implementation returns the SQL unchanged while providing a
    central hook for future dialect-specific rewrites (e.g. SparkSQL, Delta,
    Hudi, Iceberg).
    """

    _ = dialect  # Placeholder until dialect-specific behaviour is required.
    return sql

