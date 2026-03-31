from __future__ import annotations

import argparse
import importlib
import json
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


def load_all_handles(module, path: Path) -> list[object]:
    handles: list[object] = []
    with path.open("rb") as fh:
        while True:
            gid = module.codes_grib_new_from_file(fh)
            if gid is None:
                break
            handles.append(gid)
    return handles


def release_all(module, handles: list[object]) -> None:
    for gid in handles:
        module.codes_release(gid)


def sample_digest(values) -> float:
    arr = np.asarray(values, dtype=np.float64)
    return float(np.nansum(arr[: min(16, arr.size)]))


def ecrust_runtime_info() -> dict[str, object]:
    module = importlib.import_module("ecrust")
    info = module.codes_get_version_info()
    return dict(info)


def benchmark_end_to_end(module_name: str, path: Path) -> dict[str, object]:
    module = importlib.import_module(module_name)
    started = time.perf_counter()
    messages = 0
    value_count = 0
    digest = 0.0

    with path.open("rb") as fh:
        while True:
            gid = module.codes_grib_new_from_file(fh)
            if gid is None:
                break
            values = module.codes_get_values(gid)
            digest += sample_digest(values)
            messages += 1
            value_count += int(np.asarray(values).size)
            module.codes_release(gid)

    return {
        "module": module_name,
        "seconds": time.perf_counter() - started,
        "messages": messages,
        "value_count": value_count,
        "digest": digest,
    }


def benchmark_ecrust_decode_only(path: Path) -> dict[str, object]:
    module = importlib.import_module("ecrust")
    raw = importlib.import_module("ecrust._ecrust")
    sample_writeable = False
    with path.open("rb") as fh:
        sample = module.codes_grib_new_from_file(fh)
        if sample is not None:
            sample_writeable = bool(np.asarray(module.codes_get_values(sample)).flags.writeable)
            module.codes_release(sample)

    started = time.perf_counter()
    messages = 0
    value_count = 0
    with path.open("rb") as fh:
        while True:
            gid = module.codes_grib_new_from_file(fh)
            if gid is None:
                break
            # Stream the file so this phase measures decode work, not retaining
            # every decoded values array from the full HRRR file in memory.
            count = raw._benchmark_decode_values_uncached(gid)
            value_count += int(count)
            messages += 1
            module.codes_release(gid)
    seconds = time.perf_counter() - started
    return {
        "module": "ecrust",
        "seconds": seconds,
        "messages": messages,
        "value_count": value_count,
        "writeable": sample_writeable,
    }


def benchmark_ecrust_cached_export(path: Path) -> dict[str, object]:
    module = importlib.import_module("ecrust")
    raw = importlib.import_module("ecrust._ecrust")
    handles = load_all_handles(module, path)
    for gid in handles:
        raw._benchmark_force_decode_values(gid)

    started = time.perf_counter()
    value_count = 0
    digest = 0.0
    for gid in handles:
        values = module.codes_get_values(gid)
        digest += sample_digest(values)
        value_count += int(np.asarray(values).size)
    seconds = time.perf_counter() - started
    writeable = bool(np.asarray(module.codes_get_values(handles[0])).flags.writeable) if handles else False
    release_all(module, handles)
    return {
        "module": "ecrust",
        "seconds": seconds,
        "messages": len(handles),
        "value_count": value_count,
        "digest": digest,
        "writeable": writeable,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=default_source_path())
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = {
        "source": str(args.source),
        "ecrust_runtime": ecrust_runtime_info(),
        "eccodes_end_to_end": benchmark_end_to_end("eccodes", args.source),
        "ecrust_end_to_end": benchmark_end_to_end("ecrust", args.source),
        "ecrust_decode_only": benchmark_ecrust_decode_only(args.source),
        "ecrust_cached_export": benchmark_ecrust_cached_export(args.source),
    }
    report["digest_match"] = (
        abs(report["eccodes_end_to_end"]["digest"] - report["ecrust_end_to_end"]["digest"]) < 1e-6
    )

    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
