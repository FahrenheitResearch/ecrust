from __future__ import annotations

import io
import json
import tempfile
from pathlib import Path

import numpy as np
import pytest

import eccodes
import ecrust


EXPECTED_DROPIN_NAMES = [
    "codes_grib_new_from_file",
    "codes_new_from_file",
    "codes_any_new_from_file",
    "codes_grib_new_from_samples",
    "codes_get",
    "codes_get_values",
    "codes_set",
    "codes_set_array",
    "codes_set_key_vals",
    "codes_dump",
    "codes_clone",
    "codes_release",
    "codes_index_new_from_file",
    "codes_index_write",
    "codes_index_read",
    "codes_grib_multi_new",
    "codes_grib_multi_append",
    "codes_grib_multi_write",
    "codes_grib_multi_release",
]


def dropin_sample_path() -> Path:
    gid = ecrust.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    try:
        payload = ecrust.codes_get_message(gid)
    finally:
        ecrust.codes_release(gid)
    path = Path(tempfile.gettempdir()) / "ecrust_dropin_surface.grib2"
    path.write_bytes(payload)
    return path


def test_exports_curated_dropin_surface():
    for name in EXPECTED_DROPIN_NAMES:
        assert hasattr(eccodes, name), name
        assert hasattr(ecrust, name), name

    assert ecrust.Message is ecrust.GRIBMessage
    assert ecrust.message.Message is ecrust.Message
    assert ecrust.highlevel.GRIBMessage is ecrust.GRIBMessage
    assert ecrust.reader.FileReader is object

    info = ecrust.codes_get_version_info()
    assert info["bindings"] == ecrust.__version__
    assert info["allocator"] in {"system", "mimalloc"}
    assert info["target_os"] in {"windows", "linux", "macos"}
    assert info["target_arch"]
    assert ecrust.eccodes.__allocator__ == info["allocator"]

    assert ecrust.CODES_CHECK(None) is None
    assert ecrust.CODES_CHECK(0) is None
    with pytest.raises(ecrust.CodesInternalError):
        ecrust.CODES_CHECK(7)


def test_new_from_file_dispatch_matches_eccodes_for_grib_and_any():
    path = dropin_sample_path()

    with path.open("rb") as efile, path.open("rb") as rfile:
        egid = eccodes.codes_new_from_file(efile, eccodes.CODES_PRODUCT_GRIB)
        rgid = ecrust.codes_new_from_file(rfile, ecrust.CODES_PRODUCT_GRIB)
        assert egid is not None
        assert rgid is not None
        assert eccodes.codes_get_string(egid, "shortName") == ecrust.codes_get_string(
            rgid, "shortName"
        )
        assert np.allclose(eccodes.codes_get_values(egid), ecrust.codes_get_values(rgid))
        eccodes.codes_release(egid)
        ecrust.codes_release(rgid)

    with path.open("rb") as efile, path.open("rb") as rfile:
        egid = eccodes.codes_any_new_from_file(efile)
        rgid = ecrust.codes_any_new_from_file(rfile)
        assert egid is not None
        assert rgid is not None
        assert eccodes.codes_get_long(egid, "paramId") == ecrust.codes_get_long(rgid, "paramId")
        eccodes.codes_release(egid)
        ecrust.codes_release(rgid)

    with pytest.raises(ecrust.FunctionNotImplementedError):
        ecrust.codes_new_from_file(io.BytesIO(b""), ecrust.CODES_PRODUCT_BUFR)


def test_string_and_list_keyvals_match_eccodes():
    egid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    rgid = ecrust.codes_grib_new_from_samples("regular_ll_sfc_grib2")

    eccodes.codes_set_key_vals(egid, "forecastTime=24,level=10,shortName=10u")
    ecrust.codes_set_key_vals(rgid, "forecastTime=24,level=10,shortName=10u")
    for key in ["forecastTime", "level", "shortName", "paramId"]:
        assert eccodes.codes_get(egid, key) == ecrust.codes_get(rgid, key)

    eccodes.codes_set_key_vals(egid, ["forecastTime=48", "shortName=10v"])
    ecrust.codes_set_key_vals(rgid, ["forecastTime=48", "shortName=10v"])
    for key in ["forecastTime", "shortName", "paramId", "parameterNumber"]:
        assert eccodes.codes_get(egid, key) == ecrust.codes_get(rgid, key)

    eccodes.codes_release(egid)
    ecrust.codes_release(rgid)


def test_generic_setters_and_dump_json_cover_python_compat_path():
    gid = ecrust.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    values = np.linspace(280.0, 285.0, ecrust.codes_get_size(gid, "values"), dtype=np.float64)

    ecrust.codes_set(gid, "forecastTime", 18)
    ecrust.codes_set(gid, "shortName", "2d")
    ecrust.codes_set_array(gid, "values", values)

    payload = io.StringIO()
    ecrust.codes_dump(gid, payload, mode="json")
    decoded = json.loads(payload.getvalue())

    assert decoded["forecastTime"] == 18
    assert decoded["shortName"] == "2d"
    assert decoded["paramId"] == 168
    assert len(decoded["values"]) == values.size
    assert np.allclose(np.asarray(decoded["values"], dtype=np.float64), values)

    ecrust.codes_release(gid)


def test_clone_preserves_mutations_and_is_independent():
    gid = ecrust.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    values = np.linspace(260.0, 270.0, ecrust.codes_get_size(gid, "values"), dtype=np.float64)

    ecrust.codes_set_key_vals(gid, {"forecastTime": 12, "shortName": "2d"})
    ecrust.codes_set_values(gid, values)

    clone = ecrust.codes_clone(gid)
    assert ecrust.codes_get_long(clone, "forecastTime") == 12
    assert ecrust.codes_get_string(clone, "shortName") == "2d"
    assert np.allclose(ecrust.codes_get_values(clone), values)
    assert ecrust.codes_get_message(clone) == ecrust.codes_get_message(gid)

    ecrust.codes_set_long(clone, "forecastTime", 36)
    assert ecrust.codes_get_long(clone, "forecastTime") == 36
    assert ecrust.codes_get_long(gid, "forecastTime") == 12

    ecrust.codes_release(clone)
    ecrust.codes_release(gid)
