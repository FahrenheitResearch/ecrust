from __future__ import annotations

import io
import struct
import tempfile
from pathlib import Path
from tempfile import NamedTemporaryFile

import numpy as np
import pytest

import eccodes
import ecrust


def build_test_grib2(
    *,
    center: int,
    category: int,
    number: int,
    level_type: int,
    level_value: int,
    forecast_time: int,
    reference_value: float,
) -> bytes:
    def section1(center_code: int) -> bytes:
        sec = bytearray(21)
        sec[0:4] = (21).to_bytes(4, "big")
        sec[4] = 1
        sec[5:7] = center_code.to_bytes(2, "big")
        sec[7:9] = (0).to_bytes(2, "big")
        sec[9] = 28
        sec[10] = 0
        sec[11] = 1
        sec[12:14] = (2026).to_bytes(2, "big")
        sec[14] = 3
        sec[15] = 16
        sec[16] = 18
        sec[17] = 0
        sec[18] = 0
        sec[19] = 0
        sec[20] = 1
        return bytes(sec)

    def section3() -> bytes:
        sec = bytearray(72)
        sec[0:4] = (72).to_bytes(4, "big")
        sec[4] = 3
        sec[5] = 0
        sec[6:10] = (4).to_bytes(4, "big")
        sec[10] = 0
        sec[11] = 0
        sec[12:14] = (0).to_bytes(2, "big")
        sec[14] = 6
        sec[30:34] = (2).to_bytes(4, "big")
        sec[34:38] = (2).to_bytes(4, "big")
        sec[46:50] = (41_000_000).to_bytes(4, "big")
        sec[50:54] = (100_000_000).to_bytes(4, "big")
        sec[55:59] = (40_000_000).to_bytes(4, "big")
        sec[59:63] = (101_000_000).to_bytes(4, "big")
        sec[63:67] = (1_000_000).to_bytes(4, "big")
        sec[67:71] = (1_000_000).to_bytes(4, "big")
        sec[71] = 0
        return bytes(sec)

    def section4() -> bytes:
        sec = bytearray(34)
        sec[0:4] = (34).to_bytes(4, "big")
        sec[4] = 4
        sec[5:7] = (0).to_bytes(2, "big")
        sec[7:9] = (0).to_bytes(2, "big")
        sec[9] = category
        sec[10] = number
        sec[11] = 2
        sec[17] = 1
        sec[18:22] = forecast_time.to_bytes(4, "big")
        sec[22] = level_type
        sec[23] = 0
        sec[24:28] = level_value.to_bytes(4, "big")
        return bytes(sec)

    def section5() -> bytes:
        sec = bytearray(21)
        sec[0:4] = (21).to_bytes(4, "big")
        sec[4] = 5
        sec[5:9] = (4).to_bytes(4, "big")
        sec[9:11] = (0).to_bytes(2, "big")
        sec[11:15] = struct.pack(">f", reference_value)
        sec[15:17] = (0).to_bytes(2, "big", signed=True)
        sec[17:19] = (0).to_bytes(2, "big", signed=True)
        sec[19] = 8
        sec[20] = 0
        return bytes(sec)

    section6 = bytes([0, 0, 0, 6, 6, 255])
    section7 = bytes([0, 0, 0, 9, 7, 0, 1, 2, 3])

    s1 = section1(center)
    s3 = section3()
    s4 = section4()
    s5 = section5()
    total = 16 + len(s1) + len(s3) + len(s4) + len(s5) + len(section6) + len(section7) + 4

    out = bytearray()
    out.extend(b"GRIB")
    out.extend(b"\x00\x00")
    out.append(0)
    out.append(2)
    out.extend(total.to_bytes(8, "big"))
    out.extend(s1)
    out.extend(s3)
    out.extend(s4)
    out.extend(s5)
    out.extend(section6)
    out.extend(section7)
    out.extend(b"7777")
    return bytes(out)


def sample_grib_path() -> Path:
    payload = b"".join(
        [
            build_test_grib2(
                center=98,
                category=0,
                number=0,
                level_type=103,
                level_value=2,
                forecast_time=6,
                reference_value=1.0,
            ),
            build_test_grib2(
                center=98,
                category=2,
                number=2,
                level_type=103,
                level_value=10,
                forecast_time=0,
                reference_value=10.0,
            ),
        ]
    )
    path = Path(tempfile.gettempdir()) / "ecrust_pytest_sample.grib2"
    path.write_bytes(payload)
    return path


def test_grib_cache_controls_and_info():
    path = sample_grib_path()
    file_size = path.stat().st_size

    try:
        ecrust.codes_grib_cache_set_enabled(True)
        ecrust.codes_grib_cache_set_limits(32, 512 * 1024 * 1024)
        ecrust.codes_grib_cache_clear()

        info = ecrust.codes_grib_cache_info()
        assert info["enabled"] is True
        assert info["entries"] == 0
        assert info["total_bytes"] == 0

        with path.open("rb") as fh:
            assert ecrust.codes_count_in_file(fh) == 2

        info = ecrust.codes_grib_cache_info()
        assert info["entries"] == 1
        assert info["total_bytes"] >= file_size

        ecrust.codes_grib_cache_set_limits(1, file_size)
        info = ecrust.codes_grib_cache_info()
        assert info["max_entries"] == 1
        assert info["max_bytes"] == file_size

        ecrust.codes_grib_cache_set_enabled(False)
        ecrust.codes_grib_cache_clear()
        with path.open("rb") as fh:
            assert ecrust.codes_count_in_file(fh) == 2

        info = ecrust.codes_grib_cache_info()
        assert info["enabled"] is False
        assert info["entries"] == 0
        assert info["total_bytes"] == 0
    finally:
        ecrust.codes_grib_cache_set_enabled(True)
        ecrust.codes_grib_cache_set_limits(32, 512 * 1024 * 1024)
        ecrust.codes_grib_cache_clear()


def test_matches_eccodes_for_core_grib_flow():
    path = sample_grib_path()

    with path.open("rb") as efile, path.open("rb") as rfile:
        assert eccodes.codes_count_in_file(efile) == 2
        assert ecrust.codes_count_in_file(rfile) == 2

        egid1 = eccodes.codes_grib_new_from_file(efile)
        rgid1 = ecrust.codes_grib_new_from_file(rfile)
        assert egid1 is not None
        assert rgid1 is not None

        assert eccodes.codes_get(egid1, "shortName", str) == ecrust.codes_get(
            rgid1, "shortName", str
        )
        assert eccodes.codes_get_long(egid1, "paramId") == ecrust.codes_get_long(
            rgid1, "paramId"
        )
        assert np.allclose(
            eccodes.codes_get_values(egid1),
            ecrust.codes_get_values(rgid1),
        )
        assert eccodes.codes_get_native_type(egid1, "shortName") is str
        assert ecrust.codes_get_native_type(rgid1, "shortName") is str

        # The real eccodes implementation counts from the current file position
        # and then rewinds to the start of the file. ecrust should mirror that.
        assert eccodes.codes_count_in_file(efile) == 1
        assert ecrust.codes_count_in_file(rfile) == 1
        assert efile.tell() == 0
        assert rfile.tell() == 0

        # Seek manually to the second message to keep the read order aligned.
        second_offset = ecrust.codes_get_message_size(rgid1)
        efile.seek(second_offset)
        rfile.seek(second_offset)

        egid2 = eccodes.codes_grib_new_from_file(efile)
        rgid2 = ecrust.codes_grib_new_from_file(rfile)
        assert egid2 is not None
        assert rgid2 is not None

        assert eccodes.codes_get(egid2, "shortName", str) == ecrust.codes_get(
            rgid2, "shortName", str
        )
        assert eccodes.codes_get_long(egid2, "paramId") == ecrust.codes_get_long(
            rgid2, "paramId"
        )
        assert np.allclose(
            eccodes.codes_get_values(egid2),
            ecrust.codes_get_values(rgid2),
        )

        assert eccodes.codes_grib_new_from_file(efile) is None
        assert ecrust.codes_grib_new_from_file(rfile) is None

        eccodes.codes_release(egid1)
        eccodes.codes_release(egid2)
        ecrust.codes_release(rgid1)
        ecrust.codes_release(rgid2)


def test_keys_iterator_exposes_stable_names():
    path = sample_grib_path()

    with path.open("rb") as fh:
        gid = ecrust.codes_grib_new_from_file(fh)
        assert gid is not None

        iterid = ecrust.codes_keys_iterator_new(gid)
        keys = []
        while ecrust.codes_keys_iterator_next(iterid):
            keys.append(ecrust.codes_keys_iterator_get_name(iterid))
        ecrust.codes_keys_iterator_delete(iterid)

        assert "shortName" in keys
        assert "paramId" in keys
        assert "values" in keys


def test_values_arrays_are_read_only_views():
    path = sample_grib_path()

    with path.open("rb") as fh:
        gid = ecrust.codes_grib_new_from_file(fh)
        assert gid is not None

        values = np.asarray(ecrust.codes_get_values(gid), dtype=np.float64)
        values_via_key = np.asarray(ecrust.codes_get_double_array(gid, "values"), dtype=np.float64)

        assert not values.flags.writeable
        assert not values_via_key.flags.writeable
        assert np.allclose(values, values_via_key)

        with pytest.raises(ValueError):
            values[0] = values[0]

        ecrust.codes_release(gid)


def test_matches_eccodes_for_extended_grib_helpers():
    path = sample_grib_path()

    with path.open("rb") as efile, path.open("rb") as rfile:
        egid = eccodes.codes_grib_new_from_file(efile)
        rgid = ecrust.codes_grib_new_from_file(rfile)
        assert egid is not None
        assert rgid is not None

        emessage = eccodes.codes_get_message(egid)
        rmessage = ecrust.codes_get_message(rgid)
        assert emessage == rmessage

        emsg2 = eccodes.codes_new_from_message(emessage)
        rmsg2 = ecrust.codes_new_from_message(rmessage)
        assert eccodes.codes_get_string(emsg2, "shortName") == ecrust.codes_get_string(
            rmsg2, "shortName"
        )
        assert np.allclose(
            eccodes.codes_get_values(emsg2),
            ecrust.codes_get_values(rmsg2),
        )

        assert eccodes.codes_get_string_length(
            egid, "shortName"
        ) == ecrust.codes_get_string_length(rgid, "shortName")
        assert eccodes.codes_get_string_length(
            egid, "centreDescription"
        ) == ecrust.codes_get_string_length(rgid, "centreDescription")

        assert eccodes.codes_get_double_element(egid, "values", 2) == ecrust.codes_get_double_element(
            rgid, "values", 2
        )
        assert eccodes.codes_get_double_elements(
            egid, "values", [0, 3]
        ) == ecrust.codes_get_double_elements(rgid, "values", [0, 3])
        assert eccodes.codes_get_elements(egid, "values", [1, 2]) == ecrust.codes_get_elements(
            rgid, "values", [1, 2]
        )

        assert np.allclose(
            eccodes.codes_get_float_array(egid, "values"),
            ecrust.codes_get_float_array(rgid, "values"),
        )

        eiter = eccodes.codes_grib_iterator_new(egid, 0)
        riter = ecrust.codes_grib_iterator_new(rgid, 0)
        epoints = []
        rpoints = []
        while True:
            enext = eccodes.codes_grib_iterator_next(eiter)
            rnext = ecrust.codes_grib_iterator_next(riter)
            if not enext:
                assert rnext == []
                break
            epoints.append(enext)
            rpoints.append(rnext)
        eccodes.codes_grib_iterator_delete(eiter)
        ecrust.codes_grib_iterator_delete(riter)
        assert epoints == rpoints

        edata = tuple(eccodes.codes_grib_get_data(egid))
        rdata = tuple(ecrust.codes_grib_get_data(rgid))
        assert edata == rdata

        enearest = eccodes.codes_grib_find_nearest(egid, 40.2, 100.2)
        rnearest = ecrust.codes_grib_find_nearest(rgid, 40.2, 100.2)
        assert enearest[0]["index"] == rnearest[0]["index"]
        assert enearest[0]["value"] == rnearest[0]["value"]
        assert np.isclose(
            enearest[0]["distance"], rnearest[0]["distance"], atol=0.01
        )

        emulti = eccodes.codes_grib_find_nearest_multiple(
            egid, False, [40.2, 40.8], [100.2, 100.8]
        )
        rmulti = ecrust.codes_grib_find_nearest_multiple(
            rgid, False, [40.2, 40.8], [100.2, 100.8]
        )
        assert [item["index"] for item in emulti] == [item["index"] for item in rmulti]

        enearid = eccodes.codes_grib_nearest_new(egid)
        rnearid = ecrust.codes_grib_nearest_new(rgid)
        enearest4 = eccodes.codes_grib_nearest_find(enearid, egid, 40.2, 100.2, 0)
        rnearest4 = ecrust.codes_grib_nearest_find(rnearid, rgid, 40.2, 100.2, 0)
        assert {item["index"] for item in enearest4} == {
            item["index"] for item in rnearest4
        }
        eccodes.codes_grib_nearest_delete(enearid)
        ecrust.codes_grib_nearest_delete(rnearid)

        ebuf = io.BytesIO()
        rbuf = io.BytesIO()
        eccodes.codes_write(egid, ebuf)
        ecrust.codes_write(rgid, rbuf)
        assert ebuf.getvalue() == rbuf.getvalue() == emessage

        eccodes.codes_release(egid)
        eccodes.codes_release(emsg2)
        ecrust.codes_release(rgid)
        ecrust.codes_release(rmsg2)


def test_matches_eccodes_for_index_offsets_and_paths():
    path = sample_grib_path()

    assert list(eccodes.codes_extract_offsets(str(path), eccodes.CODES_PRODUCT_GRIB)) == list(
        ecrust.codes_extract_offsets(str(path), ecrust.CODES_PRODUCT_GRIB)
    )
    assert list(
        eccodes.codes_extract_offsets_sizes(str(path), eccodes.CODES_PRODUCT_GRIB)
    ) == list(ecrust.codes_extract_offsets_sizes(str(path), ecrust.CODES_PRODUCT_GRIB))

    eidx = eccodes.codes_index_new_from_file(str(path), ["shortName", "paramId:l"])
    ridx = ecrust.codes_index_new_from_file(str(path), ["shortName", "paramId:l"])

    assert eccodes.codes_index_get_size(eidx, "shortName") == ecrust.codes_index_get_size(
        ridx, "shortName"
    )
    assert eccodes.codes_index_get(eidx, "shortName") == ecrust.codes_index_get(
        ridx, "shortName"
    )
    assert eccodes.codes_index_get(eidx, "paramId") == ecrust.codes_index_get(
        ridx, "paramId"
    )
    assert eccodes.codes_index_get_long(eidx, "paramId") == ecrust.codes_index_get_long(
        ridx, "paramId"
    )

    eccodes.codes_index_select(eidx, "shortName", "2t")
    eccodes.codes_index_select_long(eidx, "paramId", 167)
    ehandle = eccodes.codes_new_from_index(eidx)

    ecrust.codes_index_select(ridx, "shortName", "2t")
    ecrust.codes_index_select_long(ridx, "paramId", 167)
    rhandle = ecrust.codes_new_from_index(ridx)

    assert ehandle is not None
    assert rhandle is not None
    assert eccodes.codes_get_string(ehandle, "shortName") == ecrust.codes_get_string(
        rhandle, "shortName"
    )
    assert eccodes.codes_new_from_index(eidx) is None
    assert ecrust.codes_new_from_index(ridx) is None

    orig_defs = ecrust.codes_definition_path()
    orig_samples = ecrust.codes_samples_path()
    ecrust.codes_set_definitions_path("X:/defs")
    ecrust.codes_set_samples_path("X:/samples")
    assert ecrust.codes_definition_path() == "X:/defs"
    assert ecrust.codes_samples_path() == "X:/samples"
    ecrust.codes_set_definitions_path(orig_defs)
    ecrust.codes_set_samples_path(orig_samples)

    assert isinstance(ecrust.codes_get_library_path(), str)
    assert isinstance(ecrust.codes_get_features(), str)
    assert isinstance(ecrust.codes_get_features(ecrust.CODES_FEATURES_ENABLED), str)
    assert isinstance(ecrust.codes_get_features(ecrust.CODES_FEATURES_DISABLED), str)

    eccodes.codes_release(ehandle)
    ecrust.codes_release(rhandle)
    eccodes.codes_index_release(eidx)
    ecrust.codes_index_release(ridx)


def test_matches_eccodes_for_sample_creation_and_setters():
    egid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    rgid = ecrust.codes_grib_new_from_samples("regular_ll_sfc_grib2")

    for key in [
        "gridType",
        "packingType",
        "shortName",
        "typeOfLevel",
        "level",
        "Nx",
        "Ny",
        "numberOfPoints",
        "numberOfValues",
        "paramId",
        "dataDate",
        "dataTime",
        "gridDefinitionTemplateNumber",
        "dataRepresentationTemplateNumber",
        "bitsPerValue",
    ]:
        assert eccodes.codes_get(egid, key) == ecrust.codes_get(rgid, key)

    assert len(eccodes.codes_get_values(egid)) == len(ecrust.codes_get_values(rgid))
    assert np.allclose(eccodes.codes_get_values(egid), ecrust.codes_get_values(rgid))

    eccodes.codes_set_long(egid, "forecastTime", 12)
    ecrust.codes_set_long(rgid, "forecastTime", 12)
    assert eccodes.codes_get_long(egid, "forecastTime") == ecrust.codes_get_long(
        rgid, "forecastTime"
    )

    eccodes.codes_set_string(egid, "shortName", "2d")
    ecrust.codes_set_string(rgid, "shortName", "2d")
    assert eccodes.codes_get_string(egid, "shortName") == ecrust.codes_get_string(
        rgid, "shortName"
    )

    eccodes.codes_set_key_vals(egid, {"forecastTime": 18, "level": 5, "shortName": "msl"})
    ecrust.codes_set_key_vals(rgid, {"forecastTime": 18, "level": 5, "shortName": "msl"})
    assert eccodes.codes_get_long(egid, "forecastTime") == ecrust.codes_get_long(
        rgid, "forecastTime"
    )
    assert eccodes.codes_get_double(egid, "level") == ecrust.codes_get_double(rgid, "level")
    assert eccodes.codes_get_string(egid, "shortName") == ecrust.codes_get_string(
        rgid, "shortName"
    )

    eccodes.codes_release(egid)
    ecrust.codes_release(rgid)


def test_matches_eccodes_for_mutation_namespace_and_multi_helpers():
    path = sample_grib_path()

    with path.open("rb") as efile, path.open("rb") as rfile:
        esrc = eccodes.codes_grib_new_from_file(efile)
        edst = eccodes.codes_grib_new_from_file(efile)
        rsrc = ecrust.codes_grib_new_from_file(rfile)
        rdst = ecrust.codes_grib_new_from_file(rfile)

    eccodes.codes_set_key_vals(esrc, {"forecastTime": 18, "level": 25, "shortName": "2d"})
    ecrust.codes_set_key_vals(rsrc, {"forecastTime": 18, "level": 25, "shortName": "2d"})
    for namespace in ["time", "parameter", "vertical", "geography"]:
        eccodes.codes_copy_namespace(esrc, namespace, edst)
        ecrust.codes_copy_namespace(rsrc, namespace, rdst)
        for key in [
            "forecastTime",
            "shortName",
            "level",
            "discipline",
            "parameterCategory",
            "parameterNumber",
            "paramId",
        ]:
            if eccodes.codes_is_defined(edst, key):
                assert ecrust.codes_is_defined(rdst, key)
                assert eccodes.codes_get(edst, key) == ecrust.codes_get(rdst, key)

    with NamedTemporaryFile(suffix=".grib2", delete=False) as eout:
        epath = Path(eout.name)
    with NamedTemporaryFile(suffix=".grib2", delete=False) as rout:
        rpath = Path(rout.name)

    try:
        emulti = eccodes.codes_grib_multi_new()
        rmulti = ecrust.codes_grib_multi_new()
        eccodes.codes_grib_multi_append(esrc, 0, emulti)
        eccodes.codes_grib_multi_append(edst, 0, emulti)
        ecrust.codes_grib_multi_append(rsrc, 0, rmulti)
        ecrust.codes_grib_multi_append(rdst, 0, rmulti)

        with epath.open("wb") as fh:
            eccodes.codes_grib_multi_write(emulti, fh)
        with rpath.open("wb") as fh:
            ecrust.codes_grib_multi_write(rmulti, fh)

        with epath.open("rb") as efh, rpath.open("rb") as rfh:
            assert eccodes.codes_count_in_file(efh) == ecrust.codes_count_in_file(rfh) == 2
    finally:
        ecrust.codes_grib_multi_release(rmulti)
        eccodes.codes_grib_multi_release(emulti)
        try:
            epath.unlink(missing_ok=True)
        except PermissionError:
            pass
        try:
            rpath.unlink(missing_ok=True)
        except PermissionError:
            pass

    eccodes.codes_release(esrc)
    eccodes.codes_release(edst)
    ecrust.codes_release(rsrc)
    ecrust.codes_release(rdst)


def test_matches_eccodes_for_gaussian_latitudes_and_index_roundtrip():
    egauss = eccodes.codes_get_gaussian_latitudes(4)
    rgauss = ecrust.codes_get_gaussian_latitudes(4)
    assert np.allclose(
        np.asarray([egauss[i] for i in range(len(egauss))], dtype=np.float64),
        np.asarray(rgauss, dtype=np.float64),
    )

    path = sample_grib_path()
    eidx = eccodes.codes_index_new_from_file(str(path), ["shortName", "paramId:l"])
    ridx = ecrust.codes_index_new_from_file(str(path), ["shortName", "paramId:l"])
    eccodes.codes_index_select_string(eidx, "shortName", "2t")
    ecrust.codes_index_select_string(ridx, "shortName", "2t")
    eccodes.codes_index_select_long(eidx, "paramId", 167)
    ecrust.codes_index_select_long(ridx, "paramId", 167)

    with NamedTemporaryFile(suffix=".idx", delete=False) as efile:
        epath = Path(efile.name)
    with NamedTemporaryFile(suffix=".json", delete=False) as rfile:
        rpath = Path(rfile.name)

    try:
        eccodes.codes_index_write(eidx, str(epath))
        ecrust.codes_index_write(ridx, str(rpath))
        eidx2 = eccodes.codes_index_read(str(epath))
        ridx2 = ecrust.codes_index_read(str(rpath))
        assert eccodes.codes_index_get(eidx2, "shortName") == ecrust.codes_index_get(
            ridx2, "shortName"
        )
        eccodes.codes_index_select_string(eidx2, "shortName", "2t")
        ecrust.codes_index_select_string(ridx2, "shortName", "2t")
        eccodes.codes_index_select_long(eidx2, "paramId", 167)
        ecrust.codes_index_select_long(ridx2, "paramId", 167)
        ehandle = eccodes.codes_new_from_index(eidx2)
        rhandle = ecrust.codes_new_from_index(ridx2)
        assert ehandle is not None
        assert rhandle is not None
        assert eccodes.codes_get_string(ehandle, "shortName") == ecrust.codes_get_string(
            rhandle, "shortName"
        )
        eccodes.codes_release(ehandle)
        ecrust.codes_release(rhandle)
        eccodes.codes_index_release(eidx2)
        ecrust.codes_index_release(ridx2)
    finally:
        try:
            epath.unlink(missing_ok=True)
        except PermissionError:
            pass
        try:
            rpath.unlink(missing_ok=True)
        except PermissionError:
            pass
        eccodes.codes_index_release(eidx)
        ecrust.codes_index_release(ridx)


def test_set_missing_raises_for_non_missing_keys():
    path = sample_grib_path()
    with path.open("rb") as fh:
        gid = ecrust.codes_grib_new_from_file(fh)

    try:
        ecrust.codes_set_missing(gid, "forecastTime")
    except Exception as exc:  # noqa: BLE001
        assert "Value cannot be missing" in str(exc)
    else:
        raise AssertionError("codes_set_missing should reject forecastTime")
    finally:
        ecrust.codes_release(gid)


def test_mutated_grib_messages_roundtrip_semantics():
    egid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    rgid = ecrust.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    values = np.linspace(270.0, 279.5, 16 * 31, dtype=np.float64)

    eccodes.codes_set_string(egid, "shortName", "2d")
    eccodes.codes_set_long(egid, "forecastTime", 12)
    eccodes.codes_set_values(egid, values.tolist())

    ecrust.codes_set_string(rgid, "shortName", "2d")
    ecrust.codes_set_long(rgid, "forecastTime", 12)
    ecrust.codes_set_values(rgid, values.tolist())

    emsg = eccodes.codes_get_message(egid)
    rmsg = ecrust.codes_get_message(rgid)
    assert eccodes.codes_get_message_size(egid) == len(emsg)
    assert ecrust.codes_get_message_size(rgid) == len(rmsg)

    eh2 = eccodes.codes_new_from_message(emsg)
    rh2 = ecrust.codes_new_from_message(rmsg)
    assert eccodes.codes_get_string(eh2, "shortName") == ecrust.codes_get_string(rh2, "shortName")
    assert eccodes.codes_get_long(eh2, "paramId") == ecrust.codes_get_long(rh2, "paramId")
    assert eccodes.codes_get_string(eh2, "typeOfLevel") == ecrust.codes_get_string(
        rh2, "typeOfLevel"
    )
    assert eccodes.codes_get_double(eh2, "level") == ecrust.codes_get_double(rh2, "level")
    assert eccodes.codes_get_long(eh2, "forecastTime") == ecrust.codes_get_long(
        rh2, "forecastTime"
    )
    assert ecrust.codes_get_string(rh2, "packingType") == "grid_simple"
    assert ecrust.codes_get_long(rh2, "dataRepresentationTemplateNumber") == 0
    assert np.allclose(eccodes.codes_get_values(eh2), ecrust.codes_get_values(rh2))

    ebuf = io.BytesIO()
    rbuf = io.BytesIO()
    eccodes.codes_write(egid, ebuf)
    ecrust.codes_write(rgid, rbuf)
    eh3 = eccodes.codes_new_from_message(ebuf.getvalue())
    rh3 = ecrust.codes_new_from_message(rbuf.getvalue())
    assert eccodes.codes_get_string(eh3, "shortName") == ecrust.codes_get_string(rh3, "shortName")
    assert ecrust.codes_get_string(rh3, "packingType") == "grid_simple"
    assert np.allclose(eccodes.codes_get_values(eh3), ecrust.codes_get_values(rh3))

    eccodes.codes_release(egid)
    eccodes.codes_release(eh2)
    eccodes.codes_release(eh3)
    ecrust.codes_release(rgid)
    ecrust.codes_release(rh2)
    ecrust.codes_release(rh3)
