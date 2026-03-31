from __future__ import annotations

import argparse
import concurrent.futures
import importlib
import json
import shutil
import tempfile
import time
from pathlib import Path

import numpy as np


def default_source_path() -> Path:
    return (
        Path.home()
        / "AppData"
        / "Local"
        / "Temp"
        / "ecrust_hrrr_bench"
        / "hrrr.t00z.wrfprsf00.grib2"
    )


def default_copy_dir() -> Path:
    return Path(tempfile.gettempdir()) / "ecrust_hrrr_parallel"


def ensure_hrrr_copies(source: Path, target_dir: Path, copies: int) -> list[Path]:
    target_dir.mkdir(parents=True, exist_ok=True)
    source_size = source.stat().st_size
    out = []
    for idx in range(copies):
        target = target_dir / f"hrrr_copy_{idx + 1}.grib2"
        if not target.exists() or target.stat().st_size != source_size:
            shutil.copy2(source, target)
        out.append(target)
    return out


def _get(module, gid, key: str, expected_type):
    if module.__name__ == "eccodes":
        return module.codes_get(gid, key, expected_type)
    return module.codes_get(gid, key, expected_type)


def ecrust_runtime_info() -> dict[str, object]:
    module = importlib.import_module("ecrust")
    info = module.codes_get_version_info()
    return dict(info)


def _process_file(module_name: str, path_str: str) -> dict[str, object]:
    module = importlib.import_module(module_name)
    path = Path(path_str)
    started = time.perf_counter()
    messages = 0
    value_count = 0
    digest = 0.0

    with path.open("rb") as fh:
        while True:
            gid = module.codes_grib_new_from_file(fh)
            if gid is None:
                break
            short_name = _get(module, gid, "shortName", str)
            param_id = _get(module, gid, "paramId", int)
            grid_type = _get(module, gid, "gridType", str)
            values = np.asarray(module.codes_get_values(gid), dtype=np.float64)
            digest += float(np.nansum(values[: min(16, values.size)]))
            digest += float(param_id + len(short_name) + len(grid_type))
            messages += 1
            value_count += int(values.size)
            module.codes_release(gid)

    return {
        "file": path.name,
        "seconds": time.perf_counter() - started,
        "messages": messages,
        "value_count": value_count,
        "digest": digest,
    }


def run_batch(module_name: str, paths: list[Path], workers: int) -> dict[str, object]:
    started = time.perf_counter()
    if workers == 1:
        results = [_process_file(module_name, str(path)) for path in paths]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_process_file, [module_name] * len(paths), map(str, paths)))
    wall_seconds = time.perf_counter() - started
    cpu_seconds = sum(result["seconds"] for result in results)
    messages = sum(result["messages"] for result in results)
    values = sum(result["value_count"] for result in results)
    digest = sum(result["digest"] for result in results)
    return {
        "module": module_name,
        "workers": workers,
        "files": len(paths),
        "wall_seconds": wall_seconds,
        "aggregate_file_seconds": cpu_seconds,
        "messages": messages,
        "value_count": values,
        "digest": digest,
        "files_per_second": len(paths) / wall_seconds,
        "messages_per_second": messages / wall_seconds,
        "values_per_second": values / wall_seconds,
        "per_file": sorted(results, key=lambda item: item["seconds"], reverse=True),
    }


def compare(paths: list[Path], workers: list[int]) -> dict[str, object]:
    runs: list[dict[str, object]] = []
    for worker_count in workers:
        eccodes = run_batch("eccodes", paths, worker_count)
        ecrust = run_batch("ecrust", paths, worker_count)
        runs.append(
            {
                "workers": worker_count,
                "files": len(paths),
                "eccodes": eccodes,
                "ecrust": ecrust,
                "ecrust_over_eccodes": ecrust["wall_seconds"] / eccodes["wall_seconds"],
                "digest_match": abs(ecrust["digest"] - eccodes["digest"]) < 1e-6,
                "message_count_match": ecrust["messages"] == eccodes["messages"],
                "value_count_match": ecrust["value_count"] == eccodes["value_count"],
            }
        )
    return {
        "paths": [str(path) for path in paths],
        "ecrust_runtime": ecrust_runtime_info(),
        "runs": runs,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=default_source_path())
    parser.add_argument("--copy-dir", type=Path, default=default_copy_dir())
    parser.add_argument("--copies", type=int, default=4)
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 2, 4])
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = ensure_hrrr_copies(args.source, args.copy_dir, args.copies)
    report = compare(paths, args.workers)
    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
