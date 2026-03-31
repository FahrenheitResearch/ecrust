from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from urllib.error import URLError

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import eccodes  # noqa: E402
import ecrust  # noqa: E402

from tests.real_world_support import (  # noqa: E402
    EXCLUDED_REAL_WORLD_CASES,
    fetch_benchmark_real_world_files,
)


def read_messages(module, path: Path) -> list[dict[str, object]]:
    messages = []
    with path.open("rb") as fh:
        while True:
            gid = module.codes_grib_new_from_file(fh)
            if gid is None:
                break
            values = np.asarray(module.codes_get_values(gid), dtype=np.float64)
            messages.append(
                {
                    "shortName": module.codes_get_string(gid, "shortName")
                    if module.codes_is_defined(gid, "shortName")
                    else None,
                    "paramId": module.codes_get_long(gid, "paramId")
                    if module.codes_is_defined(gid, "paramId")
                    else None,
                    "gridType": module.codes_get_string(gid, "gridType")
                    if module.codes_is_defined(gid, "gridType")
                    else None,
                    "count": int(values.size),
                    "min": float(values.min()) if values.size else None,
                    "max": float(values.max()) if values.size else None,
                    "mean": float(values.mean()) if values.size else None,
                }
            )
            module.codes_release(gid)
    return messages


def timed_read(module, paths: list[Path], loops: int = 3) -> float:
    started = time.perf_counter()
    for _ in range(loops):
        for path in paths:
            read_messages(module, path)
    return time.perf_counter() - started


def main() -> int:
    try:
        paths = fetch_benchmark_real_world_files()
    except URLError as exc:
        print(f"real-world GRIB fixtures could not be downloaded: {exc}", file=sys.stderr)
        return 1

    comparisons = []
    for path in paths:
        emessages = read_messages(eccodes, path)
        rmessages = read_messages(ecrust, path)
        comparisons.append(
            {
                "file": path.name,
                "message_count_match": len(emessages) == len(rmessages),
                "message_count": len(emessages),
                "messages_match": emessages == rmessages,
            }
        )

    eccodes_seconds = timed_read(eccodes, paths)
    ecrust_seconds = timed_read(ecrust, paths)
    report = {
        "excluded_cases": EXCLUDED_REAL_WORLD_CASES,
        "files": comparisons,
        "timings_seconds": {
            "eccodes": eccodes_seconds,
            "ecrust": ecrust_seconds,
            "ecrust_over_eccodes": (ecrust_seconds / eccodes_seconds) if eccodes_seconds else None,
        },
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
