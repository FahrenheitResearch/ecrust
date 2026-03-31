from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import URLError

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.real_world_support import EXCLUDED_REAL_WORLD_CASES, fetch_benchmark_real_world_files


def maybe_configure_cache(module, *, enabled: bool | None = None, clear: bool = False) -> dict[str, object] | None:
    if enabled is not None and hasattr(module, "codes_grib_cache_set_enabled"):
        module.codes_grib_cache_set_enabled(enabled)
    if clear and hasattr(module, "codes_grib_cache_clear"):
        module.codes_grib_cache_clear()
    if hasattr(module, "codes_grib_cache_info"):
        return dict(module.codes_grib_cache_info())
    return None


def profile_file(module, path: Path) -> dict[str, object]:
    phases = {
        "count": 0.0,
        "construct": 0.0,
        "keys": 0.0,
        "values": 0.0,
        "release": 0.0,
    }
    digest = 0.0
    message_count = 0
    value_count = 0

    started = time.perf_counter()
    with path.open("rb") as fh:
        counted = module.codes_count_in_file(fh)
    phases["count"] += time.perf_counter() - started

    with path.open("rb") as fh:
        while True:
            started = time.perf_counter()
            gid = module.codes_grib_new_from_file(fh)
            phases["construct"] += time.perf_counter() - started
            if gid is None:
                break

            started = time.perf_counter()
            short_name = (
                module.codes_get_string(gid, "shortName")
                if module.codes_is_defined(gid, "shortName")
                else ""
            )
            param_id = (
                module.codes_get_long(gid, "paramId")
                if module.codes_is_defined(gid, "paramId")
                else -1
            )
            grid_type = (
                module.codes_get_string(gid, "gridType")
                if module.codes_is_defined(gid, "gridType")
                else ""
            )
            phases["keys"] += time.perf_counter() - started

            started = time.perf_counter()
            values = np.asarray(module.codes_get_values(gid), dtype=np.float64)
            phases["values"] += time.perf_counter() - started

            digest += float(np.nansum(values[: min(values.size, 16)]))
            digest += float(len(short_name) + len(grid_type))
            digest += float(param_id)
            message_count += 1
            value_count += int(values.size)

            started = time.perf_counter()
            module.codes_release(gid)
            phases["release"] += time.perf_counter() - started

    return {
        "file": path.name,
        "counted_messages": counted,
        "decoded_messages": message_count,
        "value_count": value_count,
        "digest": digest,
        "total_seconds": sum(phases.values()),
        "phases": phases,
    }


def merge_profile(target: dict[str, object], profile: dict[str, object]) -> None:
    target["counted_messages"] += profile["counted_messages"]
    target["decoded_messages"] += profile["decoded_messages"]
    target["value_count"] += profile["value_count"]
    target["digest"] += profile["digest"]
    for phase, seconds in profile["phases"].items():
        target["phases"][phase] += seconds
    target["slowest_files"].append(profile)
    target["slowest_files"].sort(key=lambda item: item["total_seconds"], reverse=True)
    del target["slowest_files"][5:]


def profile_corpus(
    module_name: str,
    paths: list[Path],
    *,
    loops: int,
    clear_each_loop: bool,
    clear_each_file: bool,
    cache_enabled: bool | None,
) -> dict[str, object]:
    module = importlib.import_module(module_name)
    initial_cache = maybe_configure_cache(module, enabled=cache_enabled, clear=True)
    result = {
        "module": module_name,
        "loops": loops,
        "slowest_files": [],
        "loop_seconds": [],
        "counted_messages": 0,
        "decoded_messages": 0,
        "value_count": 0,
        "digest": 0.0,
        "phases": {
            "count": 0.0,
            "construct": 0.0,
            "keys": 0.0,
            "values": 0.0,
            "release": 0.0,
        },
        "initial_cache": initial_cache,
        "final_cache": None,
    }

    overall_started = time.perf_counter()
    for _ in range(loops):
        if clear_each_loop:
            maybe_configure_cache(module, clear=True)
        loop_started = time.perf_counter()
        for path in paths:
            if clear_each_file:
                maybe_configure_cache(module, clear=True)
            merge_profile(result, profile_file(module, path))
        result["loop_seconds"].append(time.perf_counter() - loop_started)
    result["total_seconds"] = time.perf_counter() - overall_started
    result["final_cache"] = maybe_configure_cache(module)
    return result


def worker_main(args: argparse.Namespace) -> int:
    paths = [Path(path) for path in json.loads(Path(args.manifest).read_text())]
    report = profile_corpus(
        args.module,
        paths,
        loops=args.loops,
        clear_each_loop=args.clear_each_loop,
        clear_each_file=args.clear_each_file,
        cache_enabled=args.cache_enabled,
    )
    print(json.dumps(report))
    return 0


def run_worker(
    manifest_path: Path,
    *,
    module_name: str,
    loops: int,
    clear_each_loop: bool = False,
    clear_each_file: bool = False,
    cache_enabled: bool | None = None,
) -> dict[str, object]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--module",
        module_name,
        "--manifest",
        str(manifest_path),
        "--loops",
        str(loops),
    ]
    if clear_each_loop:
        command.append("--clear-each-loop")
    if clear_each_file:
        command.append("--clear-each-file")
    if cache_enabled is not None:
        command.extend(["--cache-enabled", "true" if cache_enabled else "false"])

    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def ratio_for(results: dict[str, dict[str, object]]) -> float | None:
    eccodes = results.get("eccodes", {}).get("total_seconds")
    ecrust = results.get("ecrust", {}).get("total_seconds")
    if not eccodes:
        return None
    return ecrust / eccodes


def comparison_summary(results: dict[str, dict[str, object]]) -> dict[str, object]:
    eccodes = results["eccodes"]
    ecrust = results["ecrust"]
    return {
        "message_count_match": eccodes["decoded_messages"] == ecrust["decoded_messages"],
        "value_count_match": eccodes["value_count"] == ecrust["value_count"],
        "digest_match": abs(eccodes["digest"] - ecrust["digest"]) <= 1.0e-6,
        "ecrust_over_eccodes": ratio_for(results),
    }


def paired_results(
    manifest_path: Path,
    *,
    loops: int,
    clear_each_loop: bool = False,
    clear_each_file: bool = False,
    ecrust_cache_enabled: bool | None = True,
) -> dict[str, dict[str, object]]:
    return {
        "eccodes": run_worker(
            manifest_path,
            module_name="eccodes",
            loops=loops,
            clear_each_loop=clear_each_loop,
            clear_each_file=clear_each_file,
            cache_enabled=None,
        ),
        "ecrust": run_worker(
            manifest_path,
            module_name="ecrust",
            loops=loops,
            clear_each_loop=clear_each_loop,
            clear_each_file=clear_each_file,
            cache_enabled=ecrust_cache_enabled,
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--module")
    parser.add_argument("--manifest")
    parser.add_argument("--loops", type=int, default=1)
    parser.add_argument("--clear-each-loop", action="store_true")
    parser.add_argument("--clear-each-file", action="store_true")
    parser.add_argument(
        "--cache-enabled",
        choices=("true", "false"),
        default=None,
    )
    args = parser.parse_args()
    if args.cache_enabled is not None:
        args.cache_enabled = args.cache_enabled == "true"

    if args.worker:
        return worker_main(args)

    try:
        paths = fetch_benchmark_real_world_files()
    except URLError as exc:
        print(f"real-world GRIB fixtures could not be downloaded: {exc}", file=sys.stderr)
        return 1

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as manifest:
        manifest_path = Path(manifest.name)
        json.dump([str(path) for path in paths], manifest)

    try:
        fresh = paired_results(manifest_path, loops=1, ecrust_cache_enabled=True)
        warm = paired_results(manifest_path, loops=3, ecrust_cache_enabled=True)
        no_cache = {
            "eccodes": fresh["eccodes"],
            "ecrust": run_worker(
                manifest_path,
                module_name="ecrust",
                loops=1,
                cache_enabled=False,
            ),
        }

        strict_cold = {
            "eccodes": {
                "total_seconds": 0.0,
                "counted_messages": 0,
                "decoded_messages": 0,
                "value_count": 0,
            },
            "ecrust": {
                "total_seconds": 0.0,
                "counted_messages": 0,
                "decoded_messages": 0,
                "value_count": 0,
            },
        }
        for path in paths:
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as single_manifest:
                single_manifest_path = Path(single_manifest.name)
                json.dump([str(path)], single_manifest)
            try:
                per_file = paired_results(
                    single_manifest_path,
                    loops=1,
                    clear_each_file=True,
                    ecrust_cache_enabled=True,
                )
            finally:
                single_manifest_path.unlink(missing_ok=True)
            for module_name in ("eccodes", "ecrust"):
                strict_cold[module_name]["total_seconds"] += per_file[module_name]["total_seconds"]
                strict_cold[module_name]["counted_messages"] += per_file[module_name]["counted_messages"]
                strict_cold[module_name]["decoded_messages"] += per_file[module_name]["decoded_messages"]
                strict_cold[module_name]["value_count"] += per_file[module_name]["value_count"]

        report = {
            "excluded_cases": EXCLUDED_REAL_WORLD_CASES,
            "files": [path.name for path in paths],
            "fresh_process_single_pass": {
                **fresh,
                **comparison_summary(fresh),
            },
            "warm_repeated_pass": {
                **warm,
                **comparison_summary(warm),
            },
            "fresh_process_single_pass_cache_disabled": {
                **no_cache,
                **comparison_summary(no_cache),
            },
            "strict_cold_per_file": {
                **strict_cold,
                "ecrust_over_eccodes": ratio_for(strict_cold),
            },
        }
        print(json.dumps(report, indent=2))
        return 0
    finally:
        manifest_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
