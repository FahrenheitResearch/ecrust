from __future__ import annotations

import io
import time
from urllib.error import URLError

import numpy as np
import pytest

import eccodes
import ecrust

from tests.real_world_support import (
    fetch_benchmark_real_world_files,
    fetch_official_operational_files,
    fetch_named_real_world_files,
    fetch_real_world_files,
)


def _read_messages(module, path):
    messages = []
    with path.open("rb") as fh:
        count = module.codes_count_in_file(fh)
    with path.open("rb") as fh:
        while True:
            gid = module.codes_grib_new_from_file(fh)
            if gid is None:
                break
            values = np.asarray(module.codes_get_values(gid), dtype=np.float64)
            summary = {
                "shortName": module.codes_get_string(gid, "shortName")
                if module.codes_is_defined(gid, "shortName")
                else None,
                "paramId": module.codes_get_long(gid, "paramId")
                if module.codes_is_defined(gid, "paramId")
                else None,
                "gridType": module.codes_get_string(gid, "gridType")
                if module.codes_is_defined(gid, "gridType")
                else None,
                "packingType": module.codes_get_string(gid, "packingType")
                if module.codes_is_defined(gid, "packingType")
                else None,
                "typeOfLevel": module.codes_get_string(gid, "typeOfLevel")
                if module.codes_is_defined(gid, "typeOfLevel")
                else None,
                "level": module.codes_get_double(gid, "level")
                if module.codes_is_defined(gid, "level")
                else None,
                "numberOfPoints": len(values),
                "values": values,
            }
            module.codes_release(gid)
            messages.append(summary)
    return count, messages


def test_real_world_accuracy_against_eccodes():
    try:
        paths = fetch_real_world_files()
    except URLError as exc:
        pytest.skip(f"real-world GRIB fixtures could not be downloaded: {exc}")

    for path in paths:
        ecount, emessages = _read_messages(eccodes, path)
        rcount, rmessages = _read_messages(ecrust, path)
        assert ecount == rcount == len(emessages) == len(rmessages), path.name

        for expected, actual in zip(emessages, rmessages, strict=True):
            assert expected["shortName"] == actual["shortName"], path.name
            assert expected["paramId"] == actual["paramId"], path.name
            assert expected["gridType"] == actual["gridType"], path.name
            assert expected["packingType"] == actual["packingType"], path.name
            assert expected["typeOfLevel"] == actual["typeOfLevel"], path.name
            assert expected["level"] == actual["level"], path.name
            assert expected["numberOfPoints"] == actual["numberOfPoints"], path.name
            assert np.allclose(expected["values"], actual["values"], equal_nan=True), path.name


def test_benchmark_real_world_accuracy_against_eccodes():
    try:
        paths = fetch_benchmark_real_world_files()
    except URLError as exc:
        pytest.skip(f"real-world GRIB fixtures could not be downloaded: {exc}")

    for path in paths:
        ecount, emessages = _read_messages(eccodes, path)
        rcount, rmessages = _read_messages(ecrust, path)
        assert ecount == rcount == len(emessages) == len(rmessages), path.name

        for expected, actual in zip(emessages, rmessages, strict=True):
            assert expected["shortName"] == actual["shortName"], path.name
            assert expected["paramId"] == actual["paramId"], path.name
            assert expected["gridType"] == actual["gridType"], path.name
            assert expected["packingType"] == actual["packingType"], path.name
            assert expected["typeOfLevel"] == actual["typeOfLevel"], path.name
            assert expected["level"] == actual["level"], path.name
            assert expected["numberOfPoints"] == actual["numberOfPoints"], path.name
            assert np.allclose(expected["values"], actual["values"], equal_nan=True), path.name


def test_official_operational_model_accuracy_against_eccodes():
    try:
        paths = fetch_official_operational_files()
    except URLError as exc:
        pytest.skip(f"official operational GRIB fixtures could not be downloaded: {exc}")

    for path in paths:
        ecount, emessages = _read_messages(eccodes, path)
        rcount, rmessages = _read_messages(ecrust, path)
        assert ecount == rcount == len(emessages) == len(rmessages), path.name

        for expected, actual in zip(emessages, rmessages, strict=True):
            assert expected["shortName"] == actual["shortName"], path.name
            assert expected["paramId"] == actual["paramId"], path.name
            assert expected["gridType"] == actual["gridType"], path.name
            assert expected["packingType"] == actual["packingType"], path.name
            assert expected["typeOfLevel"] == actual["typeOfLevel"], path.name
            assert expected["level"] == actual["level"], path.name
            assert expected["numberOfPoints"] == actual["numberOfPoints"], path.name
            assert np.allclose(expected["values"], actual["values"], equal_nan=True), path.name


def test_scan_order_latitudes_match_eccodes_for_scanning_mode_64():
    try:
        [path] = fetch_named_real_world_files(["scanning_mode_64"])
    except URLError as exc:
        pytest.skip(f"real-world GRIB fixtures could not be downloaded: {exc}")

    with path.open("rb") as efile, path.open("rb") as rfile:
        egid = eccodes.codes_grib_new_from_file(efile)
        rgid = ecrust.codes_grib_new_from_file(rfile)

    for key in ["values", "latitudes", "longitudes", "distinctLatitudes", "distinctLongitudes"]:
        expected = np.asarray(eccodes.codes_get_double_array(egid, key), dtype=np.float64)
        actual = np.asarray(ecrust.codes_get_double_array(rgid, key), dtype=np.float64)
        assert np.allclose(expected, actual, equal_nan=True), key


def test_real_world_mutation_roundtrip_matches_eccodes():
    try:
        msl_path, scanning_path = fetch_named_real_world_files(
            ["regular_ll_msl", "alternate-scanning"]
        )
    except URLError as exc:
        pytest.skip(f"real-world GRIB fixtures could not be downloaded: {exc}")

    with msl_path.open("rb") as efile, msl_path.open("rb") as rfile:
        egid = eccodes.codes_grib_new_from_file(efile)
        rgid = ecrust.codes_grib_new_from_file(rfile)

    eccodes.codes_set_long(egid, "forecastTime", 96)
    ecrust.codes_set_long(rgid, "forecastTime", 96)
    ebuf = io.BytesIO()
    rbuf = io.BytesIO()
    eccodes.codes_write(egid, ebuf)
    ecrust.codes_write(rgid, rbuf)
    eh2 = eccodes.codes_new_from_message(ebuf.getvalue())
    rh2 = ecrust.codes_new_from_message(rbuf.getvalue())
    assert eccodes.codes_get_long(eh2, "forecastTime") == ecrust.codes_get_long(
        rh2, "forecastTime"
    )
    assert eccodes.codes_get_long(eh2, "validityDate") == ecrust.codes_get_long(
        rh2, "validityDate"
    )
    assert eccodes.codes_get_long(eh2, "validityTime") == ecrust.codes_get_long(
        rh2, "validityTime"
    )
    assert np.allclose(
        np.asarray(eccodes.codes_get_values(eh2), dtype=np.float64),
        np.asarray(ecrust.codes_get_values(rh2), dtype=np.float64),
        equal_nan=True,
    )

    with scanning_path.open("rb") as efile, scanning_path.open("rb") as rfile:
        egid = eccodes.codes_grib_new_from_file(efile)
        rgid = ecrust.codes_grib_new_from_file(rfile)

    eccodes.codes_set_long(egid, "dataDate", 20190102)
    ecrust.codes_set_long(rgid, "dataDate", 20190102)
    ebuf = io.BytesIO()
    rbuf = io.BytesIO()
    eccodes.codes_write(egid, ebuf)
    ecrust.codes_write(rgid, rbuf)
    eh2 = eccodes.codes_new_from_message(ebuf.getvalue())
    rh2 = ecrust.codes_new_from_message(rbuf.getvalue())
    assert eccodes.codes_get_long(eh2, "dataDate") == ecrust.codes_get_long(rh2, "dataDate")
    assert np.allclose(
        np.asarray(eccodes.codes_get_values(eh2), dtype=np.float64),
        np.asarray(ecrust.codes_get_values(rh2), dtype=np.float64),
        equal_nan=True,
    )


def test_real_world_speed_smoke():
    try:
        paths = fetch_real_world_files()
    except URLError as exc:
        pytest.skip(f"real-world GRIB fixtures could not be downloaded: {exc}")

    timings = {}
    for name, module in [("eccodes", eccodes), ("ecrust", ecrust)]:
        started = time.perf_counter()
        for path in paths:
            _read_messages(module, path)
        timings[name] = time.perf_counter() - started

    assert timings["eccodes"] > 0.0
    assert timings["ecrust"] > 0.0
