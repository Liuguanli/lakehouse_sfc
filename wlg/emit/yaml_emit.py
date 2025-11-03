"""YAML workload emission."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import yaml


def write_workload(path: str | Path, queries: List[Dict[str, object]]) -> None:
    """Persist workload definitions to YAML."""

    payload = {"workload": queries}
    with Path(path).open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)

