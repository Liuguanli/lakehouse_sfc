#!/usr/bin/env python3
"""Collect RQ1/RQ2/RQ3/RQ4/RQ5 spec result metadata into structured Python objects."""
from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SCRIPT_DIR = Path(__file__).resolve().parent

SCENARIO_DIR_RE = re.compile(
    r"^(?P<timestamp>\d{8}_\d{6})(?:__scenario=(?P<scenario>.+))?$"
)
FILENAME_RE = re.compile(
    r"^results_(?P<engine>[^_]+)_(?P<layout>.+?)_spec.+\.csv$",
    re.IGNORECASE,
)
DECL_RE = re.compile(r"^declare\s+-A\s+(?P<name>[A-Za-z0-9_]+)=\($")
KV_RE = re.compile(r'^\[(?P<key>[^\]]+)\]="(?P<value>.*)"$')
RESULT_VALUE_COLUMNS = [
    "bytesRead",
    "elapsedTime_s",
    "executorRunTime_s",
    "executorCpuTime_s",
    "bytes_input_files",
    "files_scanned",
    "bytes_scanned",
    "bytesRead_ev",
    "files_scanned_ev",
    "bytes_scanned_ev",
    "executorRunTime_s_ev",
    "executorCpuTime_s_ev",
]


@dataclass(slots=True)
class ResultFile:
    layout: str
    filename: str
    path: str


@dataclass(slots=True)
class ScenarioConfigInfo:
    key: str
    display_name: str
    options: dict[str, str]
    layout_options: list[str]


@dataclass(slots=True)
class ScenarioResults:
    timestamp: str
    scenario: str | None
    scenario_config: ScenarioConfigInfo | None
    display_name: str
    directory: str
    is_final: bool
    files: list[ResultFile]


@dataclass(slots=True)
class QueryInfo:
    id: str
    family: str
    kind: str
    columns: list[str]
    fanout: int | None


@dataclass(slots=True)
class SelectivityInfo:
    label: str
    ratio_range: tuple[float, float] | None


@dataclass(slots=True)
class ColumnConfigInfo:
    label: str
    columns: list[str]


@dataclass(slots=True)
class SpecResults:
    name: str
    path: str
    query: QueryInfo
    selectivity: SelectivityInfo
    column_config: ColumnConfigInfo
    scenarios: list[ScenarioResults]


@dataclass(slots=True)
class ResultCollection:
    root: str
    specs: list[SpecResults]

    def by_query(self) -> dict[str, list[SpecResults]]:
        grouped: dict[str, list[SpecResults]] = {}
        for spec in self.specs:
            grouped.setdefault(spec.query.id, []).append(spec)
        return grouped

    def by_scenario(self) -> dict[str, list[tuple[SpecResults, ScenarioResults]]]:
        grouped: dict[str, list[tuple[SpecResults, ScenarioResults]]] = {}
        for spec in self.specs:
            for scenario in spec.scenarios:
                if not scenario.scenario:
                    continue
                grouped.setdefault(scenario.scenario, []).append((spec, scenario))
        return grouped


@dataclass(slots=True)
class WorkloadDefinition:
    key: str
    spec_prefix: str
    results_root: Path
    query_defs: list[dict]
    column_configs: dict[str, list[str]]
    selectivity_bands: dict[str, tuple[float, float]]
    scenario_configs: dict[str, ScenarioConfigInfo]
    scenario_display_usage: dict[str, int] = field(init=False, repr=False)
    spec_name_re: re.Pattern[str] = field(init=False, repr=False)
    query_index: dict[str, dict] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        prefix = re.escape(self.spec_prefix)
        self.spec_name_re = re.compile(
            rf"^{prefix}_(?P<query>.+?)_(?P<selectivity>S\d+)_(?P<column>.+)$"
        )
        self.query_defs = copy.deepcopy(self.query_defs)
        self.column_configs = copy.deepcopy(self.column_configs)
        self.selectivity_bands = copy.deepcopy(self.selectivity_bands)
        self.query_index = {entry["id"]: entry for entry in self.query_defs}
        self.scenario_display_usage = {}


def load_scenario_configs(script_path: Path) -> dict[str, ScenarioConfigInfo]:
    if not script_path.exists():
        return {}
    configs: dict[str, ScenarioConfigInfo] = {}
    current_name: str | None = None
    current_data: dict[str, str] = {}
    with script_path.open() as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if current_name is None:
                match = DECL_RE.match(line)
                if match:
                    current_name = match.group("name")
                    current_data = {}
                continue
            if line == ")":
                display_name = current_data.get("name") or current_name
                layouts = [
                    value.strip()
                    for value in current_data.get("layouts", "").split(",")
                    if value.strip()
                ]
                configs[current_name] = ScenarioConfigInfo(
                    key=current_name,
                    display_name=display_name,
                    options=dict(current_data),
                    layout_options=layouts,
                )
                current_name = None
                current_data = {}
                continue
            if line.startswith("#"):
                continue
            match = KV_RE.match(line)
            if match:
                current_data[match.group("key")] = match.group("value")
    return configs


def _import_generator(module_name: str) -> tuple[list[dict], dict[str, list[str]], dict[str, tuple[float, float]]]:
    module = import_module(module_name)
    query_defs = copy.deepcopy(getattr(module, "QUERY_DEFS", []))
    column_configs = copy.deepcopy(getattr(module, "COLUMN_CONFIGS", {}))
    selectivity = copy.deepcopy(getattr(module, "SELECTIVITY_BANDS", {}))
    return query_defs, column_configs, selectivity


def _make_tpch_rq1_definition() -> WorkloadDefinition:
    query_defs, column_configs, selectivity = _import_generator(
        "workload_spec.generate_tpch_rq1_specs"
    )
    return WorkloadDefinition(
        key="tpch_rq1",
        spec_prefix="spec_tpch_RQ1",
        results_root=ROOT / "results" / "RQ1_tpch",
        query_defs=query_defs,
        column_configs=column_configs,
        selectivity_bands=selectivity,
        scenario_configs=load_scenario_configs(SCRIPT_DIR / "run_RQ_1_matrix.sh"),
    )


def _make_amazon_rq1_definition() -> WorkloadDefinition:
    query_defs, column_configs, selectivity = _import_generator(
        "workload_spec.generate_amazon_rq1_specs"
    )
    return WorkloadDefinition(
        key="amazon_rq1",
        spec_prefix="spec_amazon_RQ1",
        results_root=ROOT / "results" / "RQ1_amazon",
        query_defs=query_defs,
        column_configs=column_configs,
        selectivity_bands=selectivity,
        scenario_configs=load_scenario_configs(SCRIPT_DIR / "run_RQ_1_matrix.sh"),
    )


def _make_tpch_rq2_definition() -> WorkloadDefinition:
    query_defs, column_configs, selectivity = _import_generator(
        "workload_spec.generate_tpch_rq2_specs"
    )
    return WorkloadDefinition(
        key="tpch_rq2",
        spec_prefix="spec_tpch_RQ2",
        results_root=ROOT / "results" / "RQ2",
        query_defs=query_defs,
        column_configs=column_configs,
        selectivity_bands=selectivity,
        scenario_configs=load_scenario_configs(SCRIPT_DIR / "run_RQ_2_matrix.sh"),
    )


def _make_tpch_rq3_definition() -> WorkloadDefinition:
    query_defs, column_configs, selectivity = _import_generator(
        "workload_spec.generate_tpch_rq3_specs"
    )
    return WorkloadDefinition(
        key="tpch_rq3",
        spec_prefix="spec_tpch_RQ3",
        results_root=ROOT / "results" / "RQ3",
        query_defs=query_defs,
        column_configs=column_configs,
        selectivity_bands=selectivity,
        scenario_configs=load_scenario_configs(SCRIPT_DIR / "run_RQ_3_matrix.sh"),
    )


def _make_tpch_rq4_definition() -> WorkloadDefinition:
    query_defs, column_configs, selectivity = _import_generator(
        "workload_spec.generate_tpch_rq4_specs"
    )
    return WorkloadDefinition(
        key="tpch_rq4",
        spec_prefix="spec_tpch_RQ4",
        results_root=ROOT / "results" / "RQ4",
        query_defs=query_defs,
        column_configs=column_configs,
        selectivity_bands=selectivity,
        scenario_configs=load_scenario_configs(SCRIPT_DIR / "run_RQ_4_matrix.sh"),
    )


def _make_tpch_rq5_definition() -> WorkloadDefinition:
    query_defs, column_configs, selectivity = _import_generator(
        "workload_spec.generate_tpch_rq1_specs"
    )
    return WorkloadDefinition(
        key="tpch_rq5",
        spec_prefix="spec_tpch_RQ5",
        results_root=ROOT / "results" / "RQ5_tpch",
        query_defs=query_defs,
        column_configs=column_configs,
        selectivity_bands=selectivity,
        scenario_configs=load_scenario_configs(SCRIPT_DIR / "run_RQ_5_matrix_delta_tpch.sh"),
    )


def _make_amazon_rq5_definition() -> WorkloadDefinition:
    query_defs, column_configs, selectivity = _import_generator(
        "workload_spec.generate_amazon_rq1_specs"
    )
    return WorkloadDefinition(
        key="amazon_rq5",
        spec_prefix="spec_amazon_RQ5",
        results_root=ROOT / "results" / "RQ5_amazon",
        query_defs=query_defs,
        column_configs=column_configs,
        selectivity_bands=selectivity,
        scenario_configs=load_scenario_configs(SCRIPT_DIR / "run_RQ_5_matrix_delta_amazon.sh"),
    )


WORKLOAD_BUILDERS = {
    "tpch_rq1": _make_tpch_rq1_definition,
    "amazon_rq1": _make_amazon_rq1_definition,
    "tpch_rq2": _make_tpch_rq2_definition,
    "tpch_rq3": _make_tpch_rq3_definition,
    "tpch_rq4": _make_tpch_rq4_definition,
    "tpch_rq5": _make_tpch_rq5_definition,
    "amazon_rq5": _make_amazon_rq5_definition,
}

WORKLOAD_BY_ROOT = {
    "RQ1_tpch": "tpch_rq1",
    "RQ1_amazon": "amazon_rq1",
    "RQ2": "tpch_rq2",
    "RQ3": "tpch_rq3",
    "RQ4": "tpch_rq4",
    "RQ5_tpch": "tpch_rq5",
    "RQ5_amazon": "amazon_rq5",
}

WORKLOAD_BY_PREFIX = {
    "spec_tpch_RQ1": "tpch_rq1",
    "spec_amazon_RQ1": "amazon_rq1",
    "spec_tpch_RQ2": "tpch_rq2",
    "spec_tpch_RQ3": "tpch_rq3",
    "spec_tpch_RQ4": "tpch_rq4",
    "spec_tpch_RQ5": "tpch_rq5",
    "spec_amazon_RQ5": "amazon_rq5",
}

_WORKLOAD_CACHE: dict[str, WorkloadDefinition] = {}


def get_workload_definition(key: str) -> WorkloadDefinition:
    if key not in WORKLOAD_BUILDERS:
        raise ValueError(f"unknown workload key: {key}")
    if key not in _WORKLOAD_CACHE:
        _WORKLOAD_CACHE[key] = WORKLOAD_BUILDERS[key]()
    return _WORKLOAD_CACHE[key]


def resolve_workload_definition(
    root: Path, workload: str | WorkloadDefinition | None
) -> WorkloadDefinition:
    if isinstance(workload, WorkloadDefinition):
        return workload
    if isinstance(workload, str):
        return get_workload_definition(workload)
    inferred = WORKLOAD_BY_ROOT.get(root.name)
    if inferred:
        return get_workload_definition(inferred)
    for child in root.iterdir():
        if not child.is_dir():
            continue
        for prefix, key in WORKLOAD_BY_PREFIX.items():
            if child.name.startswith(prefix):
                return get_workload_definition(key)
    raise ValueError(f"could not infer workload from root {root}")


def parse_timestamp_and_scenario(dir_name: str) -> tuple[str, str | None]:
    match = SCENARIO_DIR_RE.match(dir_name)
    if match:
        return match.group("timestamp"), match.group("scenario")
    return dir_name, None


def parse_spec_name(
    spec_name: str, workload: WorkloadDefinition
) -> tuple[str, str, str]:
    if workload.key == "tpch_rq4":
        prefix = f"{workload.spec_prefix}_"
        if not spec_name.startswith(prefix):
            raise ValueError(f"unexpected spec name format: {spec_name}")
        remainder = spec_name[len(prefix) :]
        parts = remainder.split("_")
        if len(parts) < 3:
            raise ValueError(f"unexpected spec name format: {spec_name}")
        selectivity = parts[-1]
        variant = parts[0]
        query = "_".join(parts[1:-1])
        return (query, selectivity, variant)
    match = workload.spec_name_re.match(spec_name)
    if not match:
        raise ValueError(f"unexpected spec name format: {spec_name}")
    return (
        match.group("query"),
        match.group("selectivity"),
        match.group("column"),
    )


def build_query_info(query_id: str, workload: WorkloadDefinition) -> QueryInfo:
    meta = workload.query_index.get(query_id, {})
    columns = list(meta.get("columns", []))
    fanout = meta.get("fanout")
    kind = meta.get("kind", "unknown")
    family = query_id.split("_", 1)[0]
    return QueryInfo(
        id=query_id,
        family=family,
        kind=kind,
        columns=columns,
        fanout=fanout,
    )


def build_selectivity_info(
    label: str, workload: WorkloadDefinition
) -> SelectivityInfo:
    ratio = workload.selectivity_bands.get(label)
    ratio_tuple = (float(ratio[0]), float(ratio[1])) if ratio else None
    return SelectivityInfo(label=label, ratio_range=ratio_tuple)


def build_column_config_info(
    label: str, workload: WorkloadDefinition
) -> ColumnConfigInfo:
    columns = workload.column_configs.get(label, [])
    return ColumnConfigInfo(label=label, columns=list(columns))


def extract_result_files(
    directory: Path, relative_to: Path
) -> list[ResultFile]:
    csv_files = sorted(
        p for p in directory.iterdir() if p.is_file() and p.suffix.lower() == ".csv"
    )
    results: list[ResultFile] = []
    for csv_path in csv_files:
        layout = "unknown"
        name = csv_path.name
        match = FILENAME_RE.match(name)
        if match:
            layout = match.group("layout") or layout
        results.append(
            ResultFile(
                layout=layout,
                filename=name,
                path=str(csv_path.relative_to(relative_to)),
            )
        )
    return results


def build_scenario_results(
    directory: Path,
    relative_to: Path,
    *,
    scenario: str | None,
    workload: WorkloadDefinition,
    is_final: bool,
    timestamp_override: str | None = None,
) -> ScenarioResults:
    timestamp = timestamp_override or directory.name
    files = extract_result_files(directory, relative_to)
    if scenario:
        scenario_config = workload.scenario_configs.get(scenario)
        base_display = scenario_config.display_name if scenario_config else scenario
        usage = workload.scenario_display_usage.get(base_display, 0)
        if usage == 0:
            display_name = base_display
        else:
            display_name = scenario
        workload.scenario_display_usage[base_display] = usage + 1
    else:
        scenario_config = None
        display_name = "final"
    return ScenarioResults(
        timestamp=timestamp,
        scenario=scenario,
        scenario_config=scenario_config,
        display_name=display_name,
        directory=str(directory.relative_to(relative_to)),
        is_final=is_final,
        files=files,
    )


def parse_spec_dir(
    spec_dir: Path, relative_to: Path, workload: WorkloadDefinition
) -> SpecResults:
    query_id, selectivity_label, column_label = parse_spec_name(
        spec_dir.name, workload
    )
    query_info = build_query_info(query_id, workload)
    selectivity_info = build_selectivity_info(selectivity_label, workload)
    column_info = build_column_config_info(column_label, workload)
    scenarios: list[ScenarioResults] = []
    for child in sorted(p for p in spec_dir.iterdir() if p.is_dir()):
        if child.name == "final":
            final_runs = sorted(p for p in child.iterdir() if p.is_dir())
            for run_dir in final_runs:
                scenarios.append(
                    build_scenario_results(
                        run_dir,
                        relative_to,
                        scenario=None,
                        workload=workload,
                        is_final=True,
                    )
                )
            continue

        timestamp, scenario_name = parse_timestamp_and_scenario(child.name)
        scenarios.append(
            build_scenario_results(
                child,
                relative_to,
                scenario=scenario_name,
                workload=workload,
                is_final=False,
                timestamp_override=timestamp,
            )
        )
    return SpecResults(
        name=spec_dir.name,
        path=str(spec_dir.relative_to(relative_to)),
        query=query_info,
        selectivity=selectivity_info,
        column_config=column_info,
        scenarios=scenarios,
    )


def collect_spec_results(
    root: Path | None = None,
    workload: str | WorkloadDefinition | None = None,
) -> ResultCollection:
    if root is None and workload is None:
        workload = "tpch_rq1"
    if root is None:
        definition = (
            workload
            if isinstance(workload, WorkloadDefinition)
            else get_workload_definition(workload)  # type: ignore[arg-type]
        )
        root_path = definition.results_root
    else:
        root_path = root.resolve()
        definition = resolve_workload_definition(root_path, workload)

    if not root_path.is_dir():
        raise FileNotFoundError(f"results directory not found: {root_path}")

    spec_dirs = sorted(
        p
        for p in root_path.iterdir()
        if p.is_dir() and p.name.startswith(definition.spec_prefix)
    )
    specs = [parse_spec_dir(spec_dir, root_path, definition) for spec_dir in spec_dirs]
    return ResultCollection(root=str(root_path), specs=specs)


def collect_tpch_rq1_results(root: Path | None = None) -> ResultCollection:
    return collect_spec_results(root=root, workload="tpch_rq1")


def collect_amazon_rq1_results(root: Path | None = None) -> ResultCollection:
    return collect_spec_results(root=root, workload="amazon_rq1")


def collect_tpch_rq2_results(root: Path | None = None) -> ResultCollection:
    return collect_spec_results(root=root, workload="tpch_rq2")


def collect_tpch_rq3_results(root: Path | None = None) -> ResultCollection:
    return collect_spec_results(root=root, workload="tpch_rq3")


def collect_tpch_rq4_results(root: Path | None = None) -> ResultCollection:
    return collect_spec_results(root=root, workload="tpch_rq4")


def collect_tpch_rq5_results(root: Path | None = None) -> ResultCollection:
    return collect_spec_results(root=root, workload="tpch_rq5")


def collect_amazon_rq5_results(root: Path | None = None) -> ResultCollection:
    return collect_spec_results(root=root, workload="amazon_rq5")


def scenario_results_dataframe(
    collection: ResultCollection,
    spec: SpecResults,
    scenario: ScenarioResults,
) -> pd.DataFrame:
    """
    Load one scenario's CSVs into a DataFrame enriched with metadata.
    """
    base = Path(collection.root)
    frames: list[pd.DataFrame] = []
    scenario_name = scenario.scenario or "final"
    scenario_options = scenario.scenario_config.options if scenario.scenario_config else {}
    for result_file in scenario.files:
        csv_path = base / result_file.path
        if not csv_path.exists():
            raise FileNotFoundError(f"missing CSV: {csv_path}")
        df = pd.read_csv(csv_path)
        df = df.copy()
        df["layout"] = result_file.layout
        df["result_file"] = str(csv_path)
        df["spec_name"] = spec.name
        df["spec_path"] = spec.path
        df["query_id"] = spec.query.id
        df["query_family"] = spec.query.family
        df["query_kind"] = spec.query.kind
        df["query_columns"] = ",".join(spec.query.columns)
        df["query_fanout"] = spec.query.fanout
        df["column_config"] = spec.column_config.label
        df["column_features"] = ",".join(spec.column_config.columns)
        df["selectivity_label"] = spec.selectivity.label
        if spec.selectivity.ratio_range:
            df["selectivity_lo"] = spec.selectivity.ratio_range[0]
            df["selectivity_hi"] = spec.selectivity.ratio_range[1]
        else:
            df["selectivity_lo"] = None
            df["selectivity_hi"] = None
        df["scenario_name"] = scenario_name
        df["scenario_display"] = scenario.display_name
        df["scenario_sort_expr"] = scenario_options.get("sort")
        df["scenario_timestamp"] = scenario.timestamp
        df["is_final"] = scenario.is_final
        frames.append(df)
    if not frames:
        layouts = [
            entry.strip()
            for entry in scenario_options.get("layouts", "").split(",")
            if entry.strip()
        ]
        if not layouts:
            layouts = ["unknown"]
        placeholder_rows: list[dict[str, object]] = []
        for layout in layouts:
            row: dict[str, object] = {}
            row["engine"] = scenario_options.get("engine", "unknown")
            row["query"] = spec.query.id
            for col in RESULT_VALUE_COLUMNS:
                row[col] = 0.0
            row["layout"] = layout
            row["result_file"] = None
            row["spec_name"] = spec.name
            row["spec_path"] = spec.path
            row["query_id"] = spec.query.id
            row["query_family"] = spec.query.family
            row["query_kind"] = spec.query.kind
            row["query_columns"] = ",".join(spec.query.columns)
            row["query_fanout"] = spec.query.fanout
            row["column_config"] = spec.column_config.label
            row["column_features"] = ",".join(spec.column_config.columns)
            row["selectivity_label"] = spec.selectivity.label
            if spec.selectivity.ratio_range:
                row["selectivity_lo"] = spec.selectivity.ratio_range[0]
                row["selectivity_hi"] = spec.selectivity.ratio_range[1]
            else:
                row["selectivity_lo"] = None
                row["selectivity_hi"] = None
            row["scenario_name"] = scenario_name
            row["scenario_display"] = scenario.display_name
            row["scenario_sort_expr"] = scenario_options.get("sort")
            row["scenario_timestamp"] = scenario.timestamp
            row["is_final"] = scenario.is_final
            row["placeholder"] = True
            placeholder_rows.append(row)
        frames.append(pd.DataFrame(placeholder_rows))
    else:
        for df in frames:
            df["placeholder"] = False
    return pd.concat(frames, ignore_index=True)


def spec_results_dataframe(
    collection: ResultCollection,
    spec: SpecResults,
    *,
    include_final: bool = False,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for scenario in spec.scenarios:
        if not include_final and not scenario.scenario:
            continue
        frames.append(scenario_results_dataframe(collection, spec, scenario))
    if not frames:
        raise ValueError(f"no scenarios matched for spec {spec.name}")
    return pd.concat(frames, ignore_index=True)


def query_results_dataframe(
    collection: ResultCollection,
    query_id: str,
    *,
    include_final: bool = False,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for spec in collection.specs:
        if spec.query.id != query_id:
            continue
        frames.append(spec_results_dataframe(collection, spec, include_final=include_final))
    if not frames:
        raise ValueError(f"query {query_id} not found in collection rooted at {collection.root}")
    return pd.concat(frames, ignore_index=True)


def display_query_scenarios(
    collection: ResultCollection, query_id: str
) -> None:
    specs = collection.by_query().get(query_id, [])
    if not specs:
        print(f"No specs found for query {query_id}")
        return
    for spec in specs:
        feature = spec.column_config.label
        print(f"------------------{feature}------------------")
        for run in spec.scenarios:
            if not run.scenario:
                continue
            sort_value = ""
            if run.scenario_config:
                sort_value = run.scenario_config.options.get("sort", "")
            layouts = [result.layout for result in run.files]
            print(run.scenario, sort_value, layouts)


def demo_tpch_rq1(query_id: str = "Q3_K16_1") -> None:
    collection = collect_tpch_rq1_results()
    display_query_scenarios(collection, query_id)


def demo_amazon_rq1(query_id: str = "Q3_K16_1") -> None:
    collection = collect_amazon_rq1_results()
    display_query_scenarios(collection, query_id)


def demo_tpch_rq2(query_id: str = "Q4_K16_1") -> None:
    collection = collect_tpch_rq2_results()
    display_query_scenarios(collection, query_id)


def demo_tpch_rq3(query_id: str = "Q3_K16_1") -> None:
    collection = collect_tpch_rq3_results()
    display_query_scenarios(collection, query_id)


def demo_tpch_rq4(query_id: str = "Q1_N1_1") -> None:
    collection = collect_tpch_rq4_results()
    display_query_scenarios(collection, query_id)


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Parse spec result directories into structured objects."
    )
    parser.add_argument(
        "--workload",
        choices=sorted(WORKLOAD_BUILDERS),
        default=None,
        help="workload key (tpch_rq1, amazon_rq1, tpch_rq2, tpch_rq3, tpch_rq4); inferred from --root when omitted",
    )
    parser.add_argument(
        "--root",
        type=Path,
        help="override results root directory; defaults based on workload",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="optional path to dump the collected results as JSON",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="pretty-print the collected results to stdout as JSON",
    )
    parser.add_argument(
        "--demo",
        choices=["tpch_rq1", "amazon_rq1", "tpch_rq2", "tpch_rq3", "tpch_rq4"],
        help="run a demo summary print for the chosen workload instead of dumping JSON",
    )
    parser.add_argument(
        "--query",
        help="query id to use with --demo helpers (default depends on workload)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.demo:
        query = args.query
        if args.demo == "tpch_rq1":
            demo_tpch_rq1(query or "Q3_K16_1")
        elif args.demo == "amazon_rq1":
            demo_amazon_rq1(query or "Q3_K16_1")
        elif args.demo == "tpch_rq2":
            demo_tpch_rq2(query or "Q4_K16_1")
        elif args.demo == "tpch_rq3":
            demo_tpch_rq3(query or "Q3_K16_1")
        else:
            demo_tpch_rq4(query or "Q1_N1_1")
        return

    collection = collect_spec_results(root=args.root, workload=args.workload)
    payload = asdict(collection)

    if args.pretty or not args.json_output:
        json_text = json.dumps(payload, indent=2 if args.pretty else None)
        print(json_text)

    if args.json_output:
        args.json_output.write_text(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
