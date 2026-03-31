from __future__ import annotations

import json
import tempfile
import urllib.request
from pathlib import Path


BASE_URL = "https://raw.githubusercontent.com/ecmwf/cfgrib/master/tests/sample-data/{name}.grib"

CORE_REAL_WORLD_CASES = [
    "alternate-scanning",
    "cfrzr_and_cprat",
    "era5-single-level-scalar-time",
    "fields_with_missing_values",
    "hpa_and_pa",
    "multi_param_on_multi_dims",
    "regular_ll_msl",
    "regular_ll_wrong_increment",
    "regular_gg_wrong_increment",
    "soil-surface-level-mix",
    "step_60m",
    "tp_on_different_grid_resolutions",
    "uv_on_different_levels",
]

BENCHMARK_REAL_WORLD_CASES = [
    "alternate-scanning",
    "cams-egg4-monthly",
    "cfrzr_and_cprat",
    "cfrzr_and_cprat_0s",
    "ds.waveh.5",
    "era5-levels-members",
    "era5-single-level-scalar-time",
    "fields_with_missing_values",
    "forecast_monthly_ukmo",
    "hpa_and_pa",
    "lambert_grid",
    "multi_param_on_multi_dims",
    "ncep-seasonal-monthly",
    "reduced_gg",
    "regular_gg_ml",
    "regular_gg_ml_g2",
    "regular_gg_pl",
    "regular_gg_sfc",
    "regular_gg_wrong_increment",
    "regular_ll_msl",
    "regular_ll_sfc",
    "regular_ll_wrong_increment",
    "scanning_mode_64",
    "single_gridpoint",
    "soil-surface-level-mix",
    "step_60m",
    "t_analysis_and_fc_0",
    "t_on_different_level_types",
    "tp_on_different_grid_resolutions",
    "uv_on_different_levels",
]

EXCLUDED_REAL_WORLD_CASES = {
    "era5-levels-corrupted": "corrupted fixture; both libraries fail but error types differ",
    "spherical_harmonics": "known GRIB1 spectral-complex gap",
}

OFFICIAL_OPERATIONAL_FIXTURES = {
    "gfs-operational-near-surface": {
        "data_url": "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20260330/00/atmos/gfs.t00z.pgrb2.0p25.f000",
        "idx_url": "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20260330/00/atmos/gfs.t00z.pgrb2.0p25.f000.idx",
        "selectors": [
            ":TMP:2 m above ground:anl:",
            ":RH:2 m above ground:anl:",
            ":UGRD:10 m above ground:anl:",
            ":VGRD:10 m above ground:anl:",
        ],
        "suffix": ".grib2",
    },
    "rrfs-operational-near-surface": {
        "data_url": "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/rrfs.20260330/00/rrfs.t00z.prslev.2p5km.f000.hi.grib2",
        "idx_url": "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/rrfs.20260330/00/rrfs.t00z.prslev.2p5km.f000.hi.grib2.idx",
        "selectors": [
            ":TMP:2 m above ground:anl:",
            ":RH:2 m above ground:anl:",
            ":UGRD:10 m above ground:anl:",
            ":VGRD:10 m above ground:anl:",
        ],
        "suffix": ".grib2",
    },
    "ecmwf-operational-near-surface": {
        "data_url": "https://data.ecmwf.int/forecasts/20260330/00z/ifs/0p25/oper/20260330000000-0h-oper-fc.grib2",
        "idx_url": "https://data.ecmwf.int/forecasts/20260330/00z/ifs/0p25/oper/20260330000000-0h-oper-fc.index",
        "selectors": [
            "\"param\": \"2t\"",
            "\"param\": \"2d\"",
            "\"param\": \"10u\"",
            "\"param\": \"10v\"",
        ],
        "suffix": ".grib2",
    },
}

OFFICIAL_OPERATIONAL_CASES = list(OFFICIAL_OPERATIONAL_FIXTURES)


def real_world_cache_dir() -> Path:
    cache_dir = Path(tempfile.gettempdir()) / "ecrust_real_world"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def fetch_named_real_world_files(names: list[str]) -> list[Path]:
    paths = []
    for name in names:
        path = real_world_cache_dir() / f"{name}.grib"
        if not path.exists():
            urllib.request.urlretrieve(BASE_URL.format(name=name), path)
        paths.append(path)
    return paths


def fetch_real_world_files() -> list[Path]:
    return fetch_named_real_world_files(CORE_REAL_WORLD_CASES)


def fetch_benchmark_real_world_files() -> list[Path]:
    return fetch_named_real_world_files(BENCHMARK_REAL_WORLD_CASES)


def _read_url_bytes(url: str, *, headers: dict[str, str] | None = None) -> bytes:
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request) as response:
        return response.read()


def _parse_idx_entries(text: str) -> list[dict[str, int | str | None]]:
    entries: list[dict[str, int | str | None]] = []
    raw_entries = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            payload = json.loads(line)
            raw_entries.append(
                {
                    "message_no": len(raw_entries) + 1,
                    "offset": int(payload["_offset"]),
                    "length": int(payload["_length"]),
                    "line": line,
                }
            )
            continue

        message_no, offset, *_rest = line.split(":", 3)
        raw_entries.append(
            {
                "message_no": int(message_no),
                "offset": int(offset),
                "length": None,
                "line": line,
            }
        )

    for index, entry in enumerate(raw_entries):
        next_offset = raw_entries[index + 1]["offset"] if index + 1 < len(raw_entries) else None
        entries.append(
            {
                "message_no": entry["message_no"],
                "offset": entry["offset"],
                "length": entry["length"],
                "next_offset": next_offset,
                "line": entry["line"],
            }
        )
    return entries


def _fetch_range_subset(
    *,
    data_url: str,
    idx_url: str,
    selectors: list[str],
    path: Path,
) -> Path:
    idx_entries = _parse_idx_entries(_read_url_bytes(idx_url).decode("utf-8"))
    selected_entries = []
    for selector in selectors:
        matches = [entry for entry in idx_entries if selector in str(entry["line"])]
        if len(matches) != 1:
            raise RuntimeError(
                f"expected exactly one idx match for selector {selector!r}, found {len(matches)}"
            )
        selected_entries.append(matches[0])

    selected_entries.sort(key=lambda entry: int(entry["message_no"]))
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    try:
        with tmp_path.open("wb") as fh:
            for entry in selected_entries:
                start = int(entry["offset"])
                length = entry["length"]
                if length is not None:
                    end = start + int(length) - 1
                else:
                    next_offset = entry["next_offset"]
                    if next_offset is None:
                        raise RuntimeError(
                            f"cannot determine byte range for terminal idx entry {entry['message_no']}"
                        )
                    end = int(next_offset) - 1
                fh.write(
                    _read_url_bytes(
                        data_url,
                        headers={"Range": f"bytes={start}-{end}"},
                    )
                )
        tmp_path.replace(path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return path


def fetch_named_official_operational_files(names: list[str]) -> list[Path]:
    paths = []
    for name in names:
        fixture = OFFICIAL_OPERATIONAL_FIXTURES[name]
        path = real_world_cache_dir() / f"{name}{fixture['suffix']}"
        if not path.exists():
            _fetch_range_subset(
                data_url=str(fixture["data_url"]),
                idx_url=str(fixture["idx_url"]),
                selectors=list(fixture["selectors"]),
                path=path,
            )
        paths.append(path)
    return paths


def fetch_official_operational_files() -> list[Path]:
    return fetch_named_official_operational_files(OFFICIAL_OPERATIONAL_CASES)
