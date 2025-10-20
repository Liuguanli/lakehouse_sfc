"""
Workload generation primitives for demo SQL workloads.

This module provides a light-weight framework for composing query workloads
from the SQL templates stored under ``workloads/demo``. The templates make use
of ``{{tbl}}`` placeholders which can be substituted with the concrete table
or view names required by a given benchmark run. Additional parameters can be
introduced later without changing the structure provided here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional


DEMO_WORKLOAD_DIR = Path(__file__).resolve().parents[1] / "workloads" / "demo"


@dataclass(frozen=True)
class QueryTemplate:
    """Represents an individual SQL template (e.g. q1_filter.sql)."""

    name: str
    path: Path
    description: str = ""

    def read(self) -> str:
        """Return the raw SQL template contents."""
        return self.path.read_text(encoding="utf-8")


@dataclass
class QueryInstance:
    """
    A concrete query ready to execute after parameter substitution.

    Parameters are stored as a simple key/value mapping. Substitution uses a
    naive string replace for ``{{key}}`` tokens which is sufficient for the
    existing demo templates. This can be replaced with a templating engine if
    we later need more sophisticated rendering.
    """

    template: QueryTemplate
    parameters: Dict[str, str] = field(default_factory=dict)

    def render(self) -> str:
        sql = self.template.read()
        for key, value in self.parameters.items():
            sql = sql.replace(f"{{{{{key}}}}}", value)
        return sql


@dataclass
class WorkloadSpec:
    """
    Declarative description of a workload to be generated.

    For now we simply capture the template names and shared parameters. The
    structure is intentionally conservative so that additional knobs (e.g.
    per-query overrides, repetitions, concurrency) can be layered on later.
    """

    template_names: Iterable[str]
    parameters: Dict[str, str]


class WorkloadGenerator:
    """
    Registry-backed generator for assembling workloads from SQL templates.

    Usage example (to be expanded as we flesh out requirements):

        generator = WorkloadGenerator()
        generator.load_demo_templates()
        workload = generator.generate(
            WorkloadSpec(
                template_names=["q1_filter", "q2_date_range"],
                parameters={"tbl": "lineitem_demo"},
            )
        )

    ``workload`` will be an ordered list of ``QueryInstance`` objects that can
    be executed or materialised on disk by the caller.
    """

    def __init__(self) -> None:
        self._templates: Dict[str, QueryTemplate] = {}

    # ------------------------------------------------------------------ utils
    def register_template(self, alias: str, template: QueryTemplate) -> None:
        """
        Register a query template under the provided alias.

        Aliases are case-sensitive and must be unique. Existing registrations
        are overwritten which keeps the API simple while we iterate on the
        design.
        """

        self._templates[alias] = template

    def load_demo_templates(self, directory: Optional[Path] = None) -> None:
        """
        Discover and register all demo SQL templates.

        The loader expects filenames like ``q1_filter.sql`` and will register
        them under aliases matching the stem (``q1_filter``). The first SQL
        comment line (if present) is used as the template description.
        """

        sql_dir = directory or DEMO_WORKLOAD_DIR
        for path in sorted(sql_dir.glob("*.sql")):
            alias = path.stem
            description = _extract_leading_comment(path)
            self.register_template(
                alias, QueryTemplate(name=alias, path=path, description=description)
            )

    # --------------------------------------------------------------- generation
    def generate(self, spec: WorkloadSpec) -> List[QueryInstance]:
        """
        Materialise concrete query instances for the supplied spec.

        The returned list preserves the order defined in ``spec.template_names``.
        Missing templates raise ``KeyError`` so that issues surface early during
        development.
        """

        instances: List[QueryInstance] = []
        for alias in spec.template_names:
            template = self._templates[alias]
            instances.append(QueryInstance(template=template, parameters=dict(spec.parameters)))
        return instances

    # ---------------------------------------------------------------- queries
    def list_templates(self) -> List[QueryTemplate]:
        """Return the registered templates in alphabetical order."""
        return [self._templates[key] for key in sorted(self._templates)]


# --------------------------------------------------------------------------- helpers
def _extract_leading_comment(path: Path) -> str:
    """
    Return the first SQL comment line (without the ``--``) if present.

    This provides a lightweight way to capture the description of demo queries
    such as the annotations found in ``workloads/demo``.
    """

    for line in path.read_text(encoding="utf-8").splitlines():
        striped = line.strip()
        if not striped:
            continue
        if striped.startswith("--"):
            return striped.lstrip("-").strip()
        break
    return ""


__all__ = [
    "QueryInstance",
    "QueryTemplate",
    "WorkloadGenerator",
    "WorkloadSpec",
]

