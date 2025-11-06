#!/usr/bin/env python3
"""Sync latest results CSVs into per-workload `final/` directory."""
from __future__ import annotations
import shutil
from pathlib import Path

def sync_final(results_root: Path) -> None:
    if not results_root.is_dir():
        raise SystemExit(f"results root not found: {results_root}")

    for workload_dir in sorted(results_root.iterdir()):
        if not workload_dir.is_dir():
            continue
        run_dirs = sorted([p for p in workload_dir.iterdir() if p.is_dir()])
        if not run_dirs:
            continue
        latest = run_dirs[-2]
        final_dir = workload_dir / "final"
        final_dir.mkdir(exist_ok=True)

        for csv_path in final_dir.glob("*.csv"):
            csv_path.unlink()

        copied = 0
        for csv_path in latest.glob("*.csv"):
            if csv_path.is_file() and csv_path.suffix.lower() == ".csv":
                shutil.copy2(csv_path, final_dir / csv_path.name)
                copied += 1

        print(f"{workload_dir.name}: copied {copied} from {latest.name} -> final")


def main() -> None:
    results_root = Path(__file__).resolve().parents[1] / "results"
    sync_final(results_root)


if __name__ == "__main__":
    main()
