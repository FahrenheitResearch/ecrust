from __future__ import annotations

import copy
import json
import math
import struct
import sys
from pathlib import Path

import numpy as np


_HANDLE_STATE: dict[int, dict[str, object]] = {}
_INDEX_STATE: dict[int, dict[str, object]] = {}
_MISSING = object()
_STRING_BUFFER_SMALL = 256
_STRING_BUFFER_LARGE = 1025
_LEVEL_TYPE_CODES = {
    "surface": 1,
    "meanSea": 101,
    "heightAboveGround": 103,
    "isobaricInhPa": 100,
    "isobaricInPa": 100,
}
_SHORTNAME_SET_MAPPINGS = {
    "t": {
        "discipline": 0,
        "parameterCategory": 0,
        "parameterNumber": 0,
        "paramId": 130,
        "typeOfLevel": "surface",
        "level": 0,
    },
    "2t": {
        "discipline": 0,
        "parameterCategory": 0,
        "parameterNumber": 0,
        "paramId": 167,
        "typeOfLevel": "heightAboveGround",
        "level": 2,
    },
    "2d": {
        "discipline": 0,
        "parameterCategory": 0,
        "parameterNumber": 6,
        "paramId": 168,
        "typeOfLevel": "heightAboveGround",
        "level": 2,
    },
    "10u": {
        "discipline": 0,
        "parameterCategory": 2,
        "parameterNumber": 2,
        "paramId": 165,
        "typeOfLevel": "heightAboveGround",
        "level": 10,
    },
    "10v": {
        "discipline": 0,
        "parameterCategory": 2,
        "parameterNumber": 3,
        "paramId": 166,
        "typeOfLevel": "heightAboveGround",
        "level": 10,
    },
    "msl": {
        "discipline": 0,
        "parameterCategory": 3,
        "parameterNumber": 0,
        "paramId": 151,
        "typeOfLevel": "meanSea",
        "level": 0,
    },
    "tp": {
        "discipline": 0,
        "parameterCategory": 1,
        "parameterNumber": 193,
        "paramId": 228,
        "typeOfLevel": "surface",
        "level": 0,
    },
}
_PARAMID_SET_MAPPINGS = {
    130: {**_SHORTNAME_SET_MAPPINGS["t"], "shortName": "t"},
    167: {**_SHORTNAME_SET_MAPPINGS["2t"], "shortName": "2t"},
    168: {**_SHORTNAME_SET_MAPPINGS["2d"], "shortName": "2d"},
    165: {**_SHORTNAME_SET_MAPPINGS["10u"], "shortName": "10u"},
    166: {**_SHORTNAME_SET_MAPPINGS["10v"], "shortName": "10v"},
    151: {**_SHORTNAME_SET_MAPPINGS["msl"], "shortName": "msl"},
    228: {**_SHORTNAME_SET_MAPPINGS["tp"], "shortName": "tp"},
    228228: {
        "discipline": 0,
        "parameterCategory": 1,
        "parameterNumber": 52,
        "paramId": 228228,
        "shortName": "tp",
        "typeOfLevel": "surface",
        "level": 0,
    },
}
_SERIALIZED_UNAVAILABLE = object()
_GRIB2_PATCHABLE_PRODUCT_TEMPLATES = {0, 1, 8, 11}
_DERIVED_SERIALIZED_KEYS = {
    "bitsPerValue",
    "cfName",
    "cfVarName",
    "dataDate",
    "dataRepresentationTemplateNumber",
    "dataTime",
    "discipline",
    "endStep",
    "forecastTime",
    "gridDefinitionTemplateNumber",
    "level",
    "levelType",
    "name",
    "numberOfDataPoints",
    "numberOfPoints",
    "numberOfValues",
    "packingType",
    "paramId",
    "parameterCategory",
    "parameterNumber",
    "productDefinitionTemplateNumber",
    "stepRange",
    "totalLength",
    "typeOfLevel",
    "units",
    "validityDate",
    "validityTime",
}


def _string_buffer_length(value: str) -> int:
    return _STRING_BUFFER_LARGE if len(value) > 32 else _STRING_BUFFER_SMALL


def _encode_sign_magnitude_i16(value: int) -> bytes:
    magnitude = abs(int(value))
    if magnitude > 0x7FFF:
        raise ValueError(f"cannot encode signed magnitude 16-bit integer {value}")
    raw = magnitude | (0x8000 if value < 0 else 0)
    return raw.to_bytes(2, "big")


def _set_section_length(section: bytearray) -> None:
    section[0:4] = len(section).to_bytes(4, "big")


def _pack_unsigned_bits(values: list[int], bits_per_value: int) -> bytes:
    if bits_per_value == 0 or not values:
        return b""
    out = bytearray((len(values) * bits_per_value + 7) // 8)
    bit_pos = 0
    for value in values:
        if value < 0 or value >= (1 << bits_per_value):
            raise ValueError(f"value {value} does not fit in {bits_per_value} bits")
        for shift in range(bits_per_value - 1, -1, -1):
            if (value >> shift) & 1:
                byte_index = bit_pos // 8
                bit_offset = 7 - (bit_pos % 8)
                out[byte_index] |= 1 << bit_offset
            bit_pos += 1
    return bytes(out)


def _encode_decimal_scaled_u32(value: float) -> tuple[int, int]:
    numeric = float(value)
    if numeric < 0.0:
        raise ValueError(f"negative scaled values are not supported: {value}")
    for scale in range(0, 7):
        factor = 10**scale
        scaled = round(numeric * factor)
        if abs((scaled / factor) - numeric) <= 1e-9 and 0 <= scaled <= 0xFFFFFFFF:
            return scale, int(scaled)
    rounded = int(round(numeric))
    if 0 <= rounded <= 0xFFFFFFFF:
        return 0, rounded
    raise ValueError(f"value {value} is out of range for GRIB2 scaled encoding")


def _choose_simple_packing(values: np.ndarray) -> dict[str, object] | None:
    if values.size == 0 or not np.isfinite(values).all():
        return None

    constant_value = float(values[0])
    if np.allclose(values, constant_value, rtol=0.0, atol=0.0):
        return {
            "reference_value": constant_value,
            "binary_scale": 0,
            "decimal_scale": 0,
            "bits_per_value": 0,
            "payload": b"",
        }

    for decimal_scale in range(0, 7):
        factor = 10**decimal_scale
        scaled = values * factor
        rounded = np.rint(scaled)
        if np.allclose(scaled, rounded, rtol=0.0, atol=1e-9):
            reference = float(np.min(rounded))
            raw = rounded - reference
            if np.any(raw < 0.0):
                continue
            max_raw = int(np.max(raw))
            bits_per_value = max_raw.bit_length()
            if bits_per_value <= 24:
                return {
                    "reference_value": reference,
                    "binary_scale": 0,
                    "decimal_scale": decimal_scale,
                    "bits_per_value": bits_per_value,
                    "payload": _pack_unsigned_bits(raw.astype(np.uint64).tolist(), bits_per_value),
                }

    for decimal_scale in (6, 5, 4, 3):
        factor = 10**decimal_scale
        scaled = values * factor
        reference = float(np.min(scaled))
        spread = float(np.max(scaled) - reference)
        if spread == 0.0:
            return {
                "reference_value": float(values[0]),
                "binary_scale": 0,
                "decimal_scale": 0,
                "bits_per_value": 0,
                "payload": b"",
            }
        for bits_per_value in (16, 20, 24, 28, 31):
            denominator = (1 << bits_per_value) - 1
            quantum = spread / denominator
            if quantum <= 0.0:
                continue
            binary_scale = math.ceil(math.log2(quantum))
            step = 2.0**binary_scale
            raw = np.rint((scaled - reference) / step).astype(np.int64)
            raw = np.clip(raw, 0, denominator)
            reconstructed = (reference + raw.astype(np.float64) * step) / factor
            tolerance = max(1e-6, float(np.max(np.abs(values))) * 1e-5)
            if float(np.max(np.abs(reconstructed - values))) <= tolerance:
                return {
                    "reference_value": reference,
                    "binary_scale": int(binary_scale),
                    "decimal_scale": decimal_scale,
                    "bits_per_value": bits_per_value,
                    "payload": _pack_unsigned_bits(raw.astype(np.uint64).tolist(), bits_per_value),
                }
    return None


def _build_grib2_simple_sections(values: np.ndarray) -> tuple[bytearray, bytearray, bytearray]:
    packed = _choose_simple_packing(values)
    if packed is None:
        raise ValueError("values cannot be represented accurately enough with simple packing")
    sec5 = bytearray(21)
    sec5[4] = 5
    sec5[5:9] = int(values.size).to_bytes(4, "big")
    sec5[9:11] = (0).to_bytes(2, "big")
    sec5[11:15] = struct.pack(">f", float(packed["reference_value"]))
    sec5[15:17] = _encode_sign_magnitude_i16(int(packed["binary_scale"]))
    sec5[17:19] = _encode_sign_magnitude_i16(int(packed["decimal_scale"]))
    sec5[19] = int(packed["bits_per_value"])
    sec5[20] = 0
    _set_section_length(sec5)

    sec6 = bytearray([0, 0, 0, 6, 6, 255])
    payload = packed["payload"]
    sec7 = bytearray(5 + len(payload))
    sec7[4] = 7
    sec7[5:] = payload
    _set_section_length(sec7)
    return sec5, sec6, sec7


def _build_grib2_ieee_sections(values: np.ndarray) -> tuple[bytearray, bytearray, bytearray]:
    sec5 = bytearray(21)
    sec5[4] = 5
    sec5[5:9] = int(values.size).to_bytes(4, "big")
    sec5[9:11] = (4).to_bytes(2, "big")
    sec5[11:15] = struct.pack(">f", 0.0)
    sec5[15:17] = _encode_sign_magnitude_i16(0)
    sec5[17:19] = _encode_sign_magnitude_i16(0)
    sec5[19] = 32
    sec5[20] = 0
    _set_section_length(sec5)

    sec6 = bytearray([0, 0, 0, 6, 6, 255])
    payload = b"".join(struct.pack(">f", float(value)) for value in values.tolist())
    sec7 = bytearray(5 + len(payload))
    sec7[4] = 7
    sec7[5:] = payload
    _set_section_length(sec7)
    return sec5, sec6, sec7


def _split_grib2_message(message: bytes) -> dict[str, object] | None:
    if len(message) < 20 or message[:4] != b"GRIB" or message[7] != 2:
        return None
    total_length = int.from_bytes(message[8:16], "big")
    if total_length != len(message) or message[-4:] != b"7777":
        return None
    sections: list[list[object]] = []
    cursor = 16
    while cursor + 5 <= len(message) - 4:
        if message[cursor : cursor + 4] == b"7777":
            break
        section_length = int.from_bytes(message[cursor : cursor + 4], "big")
        if section_length < 5 or cursor + section_length > len(message):
            return None
        section_number = message[cursor + 4]
        sections.append([section_number, bytearray(message[cursor : cursor + section_length])])
        cursor += section_length
    if cursor != len(message) - 4:
        return None
    return {"discipline": message[6], "sections": sections}


def _assemble_grib2_message(discipline: int, sections: list[list[object]]) -> bytes:
    total_length = 16 + sum(len(section) for _, section in sections) + 4
    out = bytearray()
    out.extend(b"GRIB")
    out.extend(b"\x00\x00")
    out.append(int(discipline) & 0xFF)
    out.append(2)
    out.extend(total_length.to_bytes(8, "big"))
    for _, section in sections:
        out.extend(section)
    out.extend(b"7777")
    return bytes(out)


def _find_section(sections: list[list[object]], number: int) -> bytearray | None:
    for section_number, section in sections:
        if section_number == number:
            return section
    return None


def _patch_grib2_section1(
    section: bytearray,
    *,
    center: int | None,
    subcenter: int | None,
    data_date: int | None,
    data_time: int | None,
) -> None:
    if len(section) < 21:
        return
    if center is not None:
        section[5:7] = int(center).to_bytes(2, "big")
    if subcenter is not None:
        section[7:9] = int(subcenter).to_bytes(2, "big")
    if data_date is not None:
        numeric = int(data_date)
        section[12:14] = (numeric // 10000).to_bytes(2, "big")
        section[14] = (numeric // 100) % 100
        section[15] = numeric % 100
    if data_time is not None:
        numeric = int(data_time)
        section[16] = (numeric // 100) % 100
        section[17] = numeric % 100


def _patch_grib2_section4(
    section: bytearray,
    *,
    category: int | None,
    number: int | None,
    step_units: int | None,
    forecast_time: int | None,
    level_type: int | None,
    level_value: float | None,
) -> bool:
    if len(section) < 34:
        return False
    template = int.from_bytes(section[7:9], "big")
    if template not in _GRIB2_PATCHABLE_PRODUCT_TEMPLATES:
        return False
    if category is not None:
        section[9] = int(category) & 0xFF
    if number is not None:
        section[10] = int(number) & 0xFF
    if step_units is not None:
        section[17] = int(step_units) & 0xFF
    if forecast_time is not None:
        section[18:22] = int(forecast_time).to_bytes(4, "big")
    if level_type is not None:
        section[22] = int(level_type) & 0xFF
    if level_value is not None:
        scale_factor, scaled_value = _encode_decimal_scaled_u32(float(level_value))
        section[23] = scale_factor & 0xFF
        section[24:28] = int(scaled_value).to_bytes(4, "big")
    return True


def _build_regular_ll_grib2(
    *,
    nx: int,
    ny: int,
    lat_first_deg: float,
    lon_first_deg: float,
    lat_last_deg: float,
    lon_last_deg: float,
    category: int,
    number: int,
    level_type: int,
    level_value: int,
    forecast_time: int,
    reference_value: float,
    values: list[float],
    center: int = 98,
    subcenter: int = 0,
    data_date: int = 20070323,
    data_time: int = 1200,
) -> bytes:
    year = data_date // 10000
    month = (data_date // 100) % 100
    day = data_date % 100
    hour = data_time // 100
    minute = data_time % 100

    sec1 = bytearray(21)
    sec1[4] = 1
    sec1[5:7] = center.to_bytes(2, "big")
    sec1[7:9] = subcenter.to_bytes(2, "big")
    sec1[9] = 4
    sec1[10] = 0
    sec1[11] = 1
    sec1[12:14] = year.to_bytes(2, "big")
    sec1[14] = month
    sec1[15] = day
    sec1[16] = hour
    sec1[17] = minute
    sec1[18] = 0
    sec1[19] = 0
    sec1[20] = 1
    _set_section_length(sec1)

    sec3 = bytearray(72)
    sec3[4] = 3
    sec3[5] = 0
    sec3[6:10] = (nx * ny).to_bytes(4, "big")
    sec3[10] = 0
    sec3[11] = 0
    sec3[12:14] = (0).to_bytes(2, "big")
    sec3[14] = 6
    sec3[30:34] = nx.to_bytes(4, "big")
    sec3[34:38] = ny.to_bytes(4, "big")
    sec3[46:50] = int(round(lat_first_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    sec3[50:54] = int(round(lon_first_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    sec3[55:59] = int(round(lat_last_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    sec3[59:63] = int(round(lon_last_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    di = abs((lon_last_deg - lon_first_deg) / max(nx - 1, 1))
    dj = abs((lat_first_deg - lat_last_deg) / max(ny - 1, 1))
    sec3[63:67] = int(round(di * 1_000_000)).to_bytes(4, "big")
    sec3[67:71] = int(round(dj * 1_000_000)).to_bytes(4, "big")
    sec3[71] = 0
    _set_section_length(sec3)

    sec4 = bytearray(34)
    sec4[4] = 4
    sec4[5:7] = (0).to_bytes(2, "big")
    sec4[7:9] = (0).to_bytes(2, "big")
    sec4[9] = category
    sec4[10] = number
    sec4[11] = 2
    sec4[17] = 1
    sec4[18:22] = int(forecast_time).to_bytes(4, "big")
    sec4[22] = level_type
    sec4[23] = 0
    sec4[24:28] = int(level_value).to_bytes(4, "big")
    _set_section_length(sec4)

    del reference_value
    sec5, sec6, sec7 = _build_grib2_simple_sections(np.asarray(values, dtype=np.float64))

    total = 16 + len(sec1) + len(sec3) + len(sec4) + len(sec5) + len(sec6) + len(sec7) + 4
    out = bytearray()
    out.extend(b"GRIB")
    out.extend(b"\x00\x00")
    out.append(0)
    out.append(2)
    out.extend(total.to_bytes(8, "big"))
    out.extend(sec1)
    out.extend(sec3)
    out.extend(sec4)
    out.extend(sec5)
    out.extend(sec6)
    out.extend(sec7)
    out.extend(b"7777")
    return bytes(out)


def _build_regular_ll_grib2_ieee(
    *,
    nx: int,
    ny: int,
    lat_first_deg: float,
    lon_first_deg: float,
    lat_last_deg: float,
    lon_last_deg: float,
    discipline: int,
    category: int,
    number: int,
    level_type: int,
    level_value: int,
    forecast_time: int,
    values: list[float],
    center: int = 98,
    subcenter: int = 0,
    data_date: int = 20070323,
    data_time: int = 1200,
) -> bytes:
    year = data_date // 10000
    month = (data_date // 100) % 100
    day = data_date % 100
    hour = data_time // 100
    minute = data_time % 100

    sec1 = bytearray(21)
    sec1[4] = 1
    sec1[5:7] = center.to_bytes(2, "big")
    sec1[7:9] = subcenter.to_bytes(2, "big")
    sec1[9] = 4
    sec1[10] = 0
    sec1[11] = 1
    sec1[12:14] = year.to_bytes(2, "big")
    sec1[14] = month
    sec1[15] = day
    sec1[16] = hour
    sec1[17] = minute
    sec1[18] = 0
    sec1[19] = 0
    sec1[20] = 1
    _set_section_length(sec1)

    sec3 = bytearray(72)
    sec3[4] = 3
    sec3[5] = 0
    sec3[6:10] = (nx * ny).to_bytes(4, "big")
    sec3[10] = 0
    sec3[11] = 0
    sec3[12:14] = (0).to_bytes(2, "big")
    sec3[14] = 6
    sec3[30:34] = nx.to_bytes(4, "big")
    sec3[34:38] = ny.to_bytes(4, "big")
    sec3[46:50] = int(round(lat_first_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    sec3[50:54] = int(round(lon_first_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    sec3[55:59] = int(round(lat_last_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    sec3[59:63] = int(round(lon_last_deg * 1_000_000)).to_bytes(4, "big", signed=True)
    di = abs((lon_last_deg - lon_first_deg) / max(nx - 1, 1))
    dj = abs((lat_first_deg - lat_last_deg) / max(ny - 1, 1))
    sec3[63:67] = int(round(di * 1_000_000)).to_bytes(4, "big")
    sec3[67:71] = int(round(dj * 1_000_000)).to_bytes(4, "big")
    sec3[71] = 0
    _set_section_length(sec3)

    sec4 = bytearray(34)
    sec4[4] = 4
    sec4[5:7] = (0).to_bytes(2, "big")
    sec4[7:9] = (0).to_bytes(2, "big")
    sec4[9] = category
    sec4[10] = number
    sec4[11] = 2
    sec4[17] = 1
    sec4[18:22] = int(forecast_time).to_bytes(4, "big")
    sec4[22] = level_type
    sec4[23] = 0
    sec4[24:28] = int(level_value).to_bytes(4, "big")
    _set_section_length(sec4)

    sec5 = bytearray(21)
    sec5[4] = 5
    sec5[5:9] = len(values).to_bytes(4, "big")
    sec5[9:11] = (4).to_bytes(2, "big")
    sec5[11:15] = struct.pack(">f", 0.0)
    sec5[15:17] = _encode_sign_magnitude_i16(0)
    sec5[17:19] = _encode_sign_magnitude_i16(0)
    sec5[19] = 32
    sec5[20] = 0
    _set_section_length(sec5)

    sec6 = bytearray([0, 0, 0, 6, 6, 255])
    payload = b"".join(struct.pack(">f", float(value)) for value in values)
    sec7 = bytearray(5 + len(payload))
    sec7[4] = 7
    sec7[5:] = payload
    _set_section_length(sec7)

    total = 16 + len(sec1) + len(sec3) + len(sec4) + len(sec5) + len(sec6) + len(sec7) + 4
    out = bytearray()
    out.extend(b"GRIB")
    out.extend(b"\x00\x00")
    out.append(int(discipline))
    out.append(2)
    out.extend(total.to_bytes(8, "big"))
    out.extend(sec1)
    out.extend(sec3)
    out.extend(sec4)
    out.extend(sec5)
    out.extend(sec6)
    out.extend(sec7)
    out.extend(b"7777")
    return bytes(out)


def _regular_ll_arrays(
    nx: int,
    ny: int,
    lat_first: float,
    lon_first: float,
    lat_last: float,
    lon_last: float,
) -> tuple[list[float], list[float], list[float], list[float]]:
    longitudes = np.linspace(lon_first, lon_last, nx, dtype=np.float64).tolist()
    latitudes_desc = np.linspace(lat_first, lat_last, ny, dtype=np.float64).tolist()
    latitudes = []
    lons = []
    for latitude in latitudes_desc:
        latitudes.extend([float(latitude)] * nx)
        lons.extend(float(value) for value in longitudes)
    return longitudes, latitudes_desc, latitudes, lons


def _sample_spec(sample_name: str) -> dict[str, object]:
    name = sample_name.lower()
    if name in {"grib2", "regular_ll_sfc_grib2"}:
        nx, ny = 16, 31
        values = [273.15] * (nx * ny)
        message = _build_regular_ll_grib2(
            nx=nx,
            ny=ny,
            lat_first_deg=60.0,
            lon_first_deg=0.0,
            lat_last_deg=-30.0,
            lon_last_deg=337.5,
            category=0,
            number=0,
            level_type=1,
            level_value=0,
            forecast_time=0,
            reference_value=273.15,
            values=values,
            data_date=20070323,
            data_time=1200,
        )
        distinct_lons, distinct_lats, latitudes, longitudes = _regular_ll_arrays(
            nx, ny, 60.0, 0.0, -30.0, 337.5
        )
        return {
            "message": message,
            "overrides": {
                "packingType": "grid_simple",
                "distinctLongitudes": distinct_lons,
                "distinctLatitudes": distinct_lats,
                "latitudes": latitudes,
                "longitudes": longitudes,
                "values": values,
            },
        }
    if name == "regular_ll_pl_grib2":
        spec = _sample_spec("regular_ll_sfc_grib2")
        spec["message"] = _build_regular_ll_grib2(
            nx=16,
            ny=31,
            lat_first_deg=60.0,
            lon_first_deg=0.0,
            lat_last_deg=-30.0,
            lon_last_deg=337.5,
            category=0,
            number=0,
            level_type=100,
            level_value=1000,
            forecast_time=0,
            reference_value=273.15,
            values=[273.15] * (16 * 31),
            data_date=20070323,
            data_time=1200,
        )
        return spec
    if name == "reduced_gg_pl_128_grib2":
        spec = _sample_spec("regular_ll_pl_grib2")
        spec["overrides"] = {
            **spec["overrides"],
            "gridType": "reduced_gg",
            "Nx": 2_147_483_647,
            "Ny": 256,
            "numberOfPoints": 88838,
            "values": [273.15] * 88838,
            "dataDate": 20100912,
            "dataTime": 1200,
        }
        return spec
    raise KeyError(sample_name)


def install(ns: dict[str, object]) -> None:
    codes_internal_error = ns["CodesInternalError"]
    invalid_index_error = ns["InvalidIndexError"]
    invalid_nearest_error = ns["InvalidNearestError"]
    function_not_implemented_error = ns["FunctionNotImplementedError"]
    value_cannot_be_missing_error = ns["ValueCannotBeMissingError"]
    codes_missing_long = ns["CODES_MISSING_LONG"]
    codes_missing_double = ns["CODES_MISSING_DOUBLE"]
    product_any = ns["CODES_PRODUCT_ANY"]
    product_grib = ns["CODES_PRODUCT_GRIB"]
    product_bufr = ns["CODES_PRODUCT_BUFR"]
    product_metar = ns["CODES_PRODUCT_METAR"]
    product_gts = ns["CODES_PRODUCT_GTS"]

    raw_codes_get = ns["codes_get"]
    raw_codes_get_long = ns["codes_get_long"]
    raw_codes_get_double = ns["codes_get_double"]
    raw_codes_get_string = ns["codes_get_string"]
    raw_codes_get_array = ns["codes_get_array"]
    raw_codes_get_double_array = ns["codes_get_double_array"]
    raw_codes_get_float_array = ns["codes_get_float_array"]
    raw_codes_get_long_array = ns["codes_get_long_array"]
    raw_codes_get_string_array = ns["codes_get_string_array"]
    raw_codes_get_size = ns["codes_get_size"]
    raw_codes_get_native_type = ns["codes_get_native_type"]
    raw_codes_get_string_length = ns["codes_get_string_length"]
    raw_codes_get_message = ns["codes_get_message"]
    raw_codes_get_message_size = ns["codes_get_message_size"]
    raw_codes_new_from_message = ns["codes_new_from_message"]
    raw_codes_clone = ns["codes_clone"]
    raw_codes_release = ns["codes_release"]
    raw_codes_write = ns["codes_write"]
    raw_codes_is_defined = ns["codes_is_defined"]
    raw_codes_is_missing = ns["codes_is_missing"]
    raw_codes_index_new_from_file = ns["codes_index_new_from_file"]
    raw_codes_index_add_file = ns["codes_index_add_file"]
    raw_codes_index_select = ns["codes_index_select"]
    raw_codes_index_select_long = ns["codes_index_select_long"]
    raw_codes_index_select_double = ns["codes_index_select_double"]
    raw_codes_index_select_string = ns["codes_index_select_string"]
    raw_codes_index_release = ns["codes_index_release"]
    raw_codes_new_from_index = ns["codes_new_from_index"]
    raw_codes_grib_iterator_new = ns["codes_grib_iterator_new"]
    raw_codes_grib_iterator_next = ns["codes_grib_iterator_next"]
    raw_codes_grib_iterator_delete = ns["codes_grib_iterator_delete"]
    raw_codes_grib_get_data = ns["codes_grib_get_data"]
    raw_codes_grib_nearest_new = ns["codes_grib_nearest_new"]
    raw_codes_grib_nearest_find = ns["codes_grib_nearest_find"]
    raw_codes_grib_nearest_delete = ns["codes_grib_nearest_delete"]
    raw_codes_grib_find_nearest = ns["codes_grib_find_nearest"]
    raw_codes_grib_find_nearest_multiple = ns["codes_grib_find_nearest_multiple"]

    def state_for(handle: object) -> dict[str, object]:
        key = id(handle)
        state = _HANDLE_STATE.get(key)
        if state is None or state.get("handle") is not handle:
            state = {"handle": handle, "overrides": {}}
            _HANDLE_STATE[key] = state
        return state

    def index_state_for(indexid: object) -> dict[str, object]:
        key = id(indexid)
        state = _INDEX_STATE.get(key)
        if state is None or state.get("index") is not indexid:
            state = {"index": indexid, "files": [], "keys": [], "selections": []}
            _INDEX_STATE[key] = state
        return state

    def override_for(handle: object, key: str) -> tuple[bool, object]:
        overrides = state_for(handle)["overrides"]
        if key in overrides:
            return True, overrides[key]
        return False, None

    def invalidate_materialized(handle: object) -> None:
        state = _HANDLE_STATE.get(id(handle))
        if state is not None:
            state.pop("serialized_message", None)

    def set_override(handle: object, key: str, value: object) -> None:
        state_for(handle)["overrides"][key] = value
        invalidate_materialized(handle)

    def base_get_long(handle: object, key: str, default: int | None = None) -> int | None:
        exists, value = override_for(handle, key)
        if exists and value is not _MISSING:
            return int(value)
        try:
            return int(raw_codes_get_long(handle, key))
        except Exception:
            return default

    def base_get_double(handle: object, key: str, default: float | None = None) -> float | None:
        exists, value = override_for(handle, key)
        if exists and value is not _MISSING:
            return float(value)
        try:
            return float(raw_codes_get_double(handle, key))
        except Exception:
            return default

    def base_get_string(handle: object, key: str, default: str | None = None) -> str | None:
        exists, value = override_for(handle, key)
        if exists and value is not _MISSING:
            return str(value)
        try:
            return str(raw_codes_get_string(handle, key))
        except Exception:
            return default

    def base_is_defined(handle: object, key: str) -> bool:
        exists, value = override_for(handle, key)
        if exists:
            return value is not _MISSING
        return raw_codes_is_defined(handle, key)

    def current_level_type_code(handle: object) -> int | None:
        level_name = base_get_string(handle, "typeOfLevel") or base_get_string(handle, "levelType")
        if level_name in _LEVEL_TYPE_CODES:
            return _LEVEL_TYPE_CODES[level_name]
        return base_get_long(handle, "typeOfFirstFixedSurface")

    def materialized_get(handle: object, key: str, getter):
        serialized = maybe_serialized_message(handle)
        if serialized is None or key not in _DERIVED_SERIALIZED_KEYS:
            return getter(handle, key)
        temp = raw_codes_new_from_message(serialized)
        try:
            return getter(temp, key)
        finally:
            raw_codes_release(temp)

    def infer_kind(handle: object, key: str) -> object:
        exists, value = override_for(handle, key)
        if exists:
            if value is _MISSING:
                return "missing"
            if isinstance(value, tuple):
                return str
            if isinstance(value, np.ndarray):
                return float
            if isinstance(value, str):
                return str
            if isinstance(value, int):
                return int
            if isinstance(value, float):
                return float
        try:
            return materialized_get(handle, key, raw_codes_get_native_type)
        except Exception:
            return None

    def value_from_handle(handle: object, key: str, getter) -> object:
        exists, override = override_for(handle, key)
        if exists:
            if override is _MISSING:
                kind = infer_kind(handle, key)
                if kind in (int, None):
                    return codes_missing_long
                if kind is float:
                    return codes_missing_double
                if kind is str:
                    return ""
                return tuple()
            return override
        return materialized_get(handle, key, getter)

    def array_to_requested(value: object, ktype: object | None) -> object:
        if isinstance(value, tuple):
            return tuple(str(item) for item in value)
        array = np.asarray(value)
        if ktype in (None, float):
            return array.astype(np.float64, copy=False)
        if ktype is int:
            return array.astype(np.int64, copy=False)
        return array.astype(np.float32, copy=False)

    def materialize_values_array(handle: object, value: object, dtype) -> np.ndarray:
        array = np.asarray(value, dtype=np.float64)
        if array.size and np.isnan(array).any():
            array = array.copy()
            array[np.isnan(array)] = codes_get_double(handle, "missingValue")
        return array.astype(dtype, copy=False)

    def handle_points(handle: object) -> list[tuple[float, float, float]] | None:
        lat_exists, latitudes = override_for(handle, "latitudes")
        lon_exists, longitudes = override_for(handle, "longitudes")
        val_exists, values = override_for(handle, "values")
        if val_exists:
            if not lat_exists:
                try:
                    latitudes = raw_codes_get_double_array(handle, "latitudes")
                    lat_exists = True
                except Exception:
                    lat_exists = False
            if not lon_exists:
                try:
                    longitudes = raw_codes_get_double_array(handle, "longitudes")
                    lon_exists = True
                except Exception:
                    lon_exists = False
        if lat_exists and lon_exists and val_exists:
            value_array = materialize_values_array(handle, values, np.float64)
            if len(latitudes) == len(longitudes) == len(value_array):
                return list(zip(latitudes, longitudes, value_array.tolist()))
        return None

    def great_circle_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
        return 6371.0 * 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(1.0 - a, 0.0)))

    def nearest_objects(handle: object, inlat: float, inlon: float, npoints: int) -> tuple[dict[str, float], ...] | None:
        points = handle_points(handle)
        if points is None:
            return None
        ranked = []
        for index, (lat, lon, value) in enumerate(points):
            ranked.append(
                {
                    "index": index,
                    "distance": great_circle_km(inlat, inlon, lat, lon),
                    "lat": lat,
                    "lon": lon,
                    "value": value,
                }
            )
        ranked.sort(key=lambda item: item["distance"])
        return tuple(ranked[: max(npoints, 1)])

    def copy_handle_state(source: object, target: object) -> None:
        source_state = _HANDLE_STATE.get(id(source))
        if source_state:
            cloned = copy.deepcopy({k: v for k, v in source_state.items() if k != "handle"})
            cloned["handle"] = target
            _HANDLE_STATE[id(target)] = cloned

    def apply_mapping(handle: object, mapping: dict[str, object]) -> None:
        existing = state_for(handle)["overrides"]
        for map_key, map_value in mapping.items():
            if map_key in existing and map_key not in {"shortName", "paramId", "parameterCategory", "parameterNumber", "discipline"}:
                continue
            if isinstance(map_value, str):
                set_override(handle, map_key, map_value)
            elif isinstance(map_value, int):
                set_override(handle, map_key, int(map_value))
            else:
                set_override(handle, map_key, map_value)
        if "typeOfLevel" in mapping and "levelType" not in existing:
            set_override(handle, "levelType", mapping["typeOfLevel"])

    def maybe_serialized_message(handle: object) -> bytes | None:
        state = _HANDLE_STATE.get(id(handle))
        if not state or not state.get("overrides"):
            return None
        cached = state.get("serialized_message")
        if cached is _SERIALIZED_UNAVAILABLE:
            return None
        if isinstance(cached, (bytes, bytearray)):
            return bytes(cached)
        try:
            if base_get_long(handle, "edition") != 2:
                state["serialized_message"] = _SERIALIZED_UNAVAILABLE
                return None

            parsed = _split_grib2_message(bytes(raw_codes_get_message(handle)))
            if parsed is None:
                state["serialized_message"] = _SERIALIZED_UNAVAILABLE
                return None

            sections = copy.deepcopy(parsed["sections"])
            sec1 = _find_section(sections, 1)
            sec4 = _find_section(sections, 4)
            sec5 = _find_section(sections, 5)
            sec6 = _find_section(sections, 6)
            sec7 = _find_section(sections, 7)
            if sec4 is None or sec5 is None or sec6 is None or sec7 is None:
                state["serialized_message"] = _SERIALIZED_UNAVAILABLE
                return None

            if sec1 is not None:
                _patch_grib2_section1(
                    sec1,
                    center=base_get_long(handle, "centre"),
                    subcenter=base_get_long(handle, "subCentre"),
                    data_date=base_get_long(handle, "dataDate"),
                    data_time=base_get_long(handle, "dataTime"),
                )

            product_override_keys = {
                "discipline",
                "forecastTime",
                "level",
                "levelType",
                "paramId",
                "parameterCategory",
                "parameterNumber",
                "shortName",
                "stepUnits",
                "typeOfLevel",
            }
            if any(key in state["overrides"] for key in product_override_keys):
                if not _patch_grib2_section4(
                    sec4,
                    category=base_get_long(handle, "parameterCategory"),
                    number=base_get_long(handle, "parameterNumber"),
                    step_units=base_get_long(handle, "stepUnits"),
                    forecast_time=base_get_long(handle, "forecastTime"),
                    level_type=current_level_type_code(handle),
                    level_value=base_get_double(handle, "level"),
                ):
                    state["serialized_message"] = _SERIALIZED_UNAVAILABLE
                    return None

            if "values" in state["overrides"]:
                values = materialize_values_array(handle, state["overrides"]["values"], np.float64)
                raw_values = np.asarray(raw_codes_get_double_array(handle, "values"), dtype=np.float64)
                if values.size != raw_values.size:
                    state["serialized_message"] = _SERIALIZED_UNAVAILABLE
                    return None
                if not np.isfinite(values).all():
                    state["serialized_message"] = _SERIALIZED_UNAVAILABLE
                    return None
                try:
                    new_sec5, new_sec6, new_sec7 = _build_grib2_simple_sections(values)
                except Exception:
                    new_sec5, new_sec6, new_sec7 = _build_grib2_ieee_sections(values)
                sec5[:] = new_sec5
                sec6[:] = new_sec6
                sec7[:] = new_sec7

            discipline = base_get_long(handle, "discipline", parsed["discipline"])
            serialized = _assemble_grib2_message(
                parsed["discipline"] if discipline is None else discipline,
                sections,
            )
            state["serialized_message"] = serialized
            return serialized
        except Exception:
            state["serialized_message"] = _SERIALIZED_UNAVAILABLE
            return None

    def unsupported_product(product_name: str):
        def _fn(*_args, **_kwargs):
            raise function_not_implemented_error(f"ecrust does not implement {product_name} messages yet")

        return _fn

    def codes_any_new_from_samples(samplename: str):
        return codes_new_from_samples(samplename, product_any)

    def codes_new_from_samples(samplename: str, product_kind: int):
        if product_kind not in (product_any, product_grib):
            if product_kind == product_bufr:
                return unsupported_product("BUFR")()
            if product_kind == product_metar:
                return unsupported_product("METAR")()
            if product_kind == product_gts:
                return unsupported_product("GTS")()
            raise codes_internal_error(f"unsupported product kind {product_kind}")
        try:
            spec = _sample_spec(samplename)
        except KeyError as exc:
            raise codes_internal_error(f"unknown sample '{samplename}'") from exc
        handle = raw_codes_new_from_message(spec["message"])
        state = state_for(handle)
        state["sample_name"] = samplename
        for key, value in spec["overrides"].items():
            if isinstance(value, list):
                set_override(handle, key, np.asarray(value, dtype=np.float64))
            else:
                set_override(handle, key, value)
        return handle

    def codes_grib_new_from_samples(samplename: str):
        return codes_new_from_samples(samplename, product_grib)

    def codes_set_long(msgid: object, key: str, value: int) -> None:
        set_override(msgid, key, int(value))
        if key == "paramId":
            mapping = _PARAMID_SET_MAPPINGS.get(int(value))
            if mapping is not None:
                apply_mapping(msgid, mapping)

    def codes_set_double(msgid: object, key: str, value: float) -> None:
        set_override(msgid, key, float(value))

    def codes_set_string(msgid: object, key: str, value: str) -> None:
        set_override(msgid, key, str(value))
        if key == "shortName":
            mapping = _SHORTNAME_SET_MAPPINGS.get(str(value))
            if mapping is not None:
                apply_mapping(msgid, {"shortName": str(value), **mapping})
        if key in {"typeOfLevel", "levelType"}:
            set_override(msgid, "typeOfLevel", str(value))
            set_override(msgid, "levelType", str(value))

    def codes_set_long_array(msgid: object, key: str, inarray) -> None:
        set_override(msgid, key, np.asarray(list(inarray), dtype=np.int64))

    def codes_set_double_array(msgid: object, key: str, inarray) -> None:
        set_override(msgid, key, np.asarray(list(inarray), dtype=np.float64))

    def codes_set_string_array(msgid: object, key: str, inarray) -> None:
        set_override(msgid, key, tuple(str(item) for item in inarray))

    def codes_set_values(gribid: object, values) -> None:
        array = np.asarray(list(values), dtype=np.float64)
        expected = np.asarray(codes_get_values(gribid), dtype=np.float64)
        if array.size != expected.size:
            raise codes_internal_error(
                f"expected {expected.size} values but received {array.size}"
            )
        set_override(gribid, "values", array)

    def codes_set(msgid: object, key: str, value) -> None:
        if isinstance(value, str):
            return codes_set_string(msgid, key, value)
        if isinstance(value, (list, tuple, np.ndarray)):
            return codes_set_array(msgid, key, value)
        if isinstance(value, int):
            return codes_set_long(msgid, key, value)
        if isinstance(value, float):
            return codes_set_double(msgid, key, value)
        raise TypeError(f"unsupported type for codes_set: {type(value)!r}")

    def codes_set_array(msgid: object, key: str, value) -> None:
        items = list(value)
        if not items:
            set_override(msgid, key, np.asarray([], dtype=np.float64))
            return
        if isinstance(items[0], str):
            return codes_set_string_array(msgid, key, items)
        if isinstance(items[0], int):
            return codes_set_long_array(msgid, key, items)
        return codes_set_double_array(msgid, key, items)

    def codes_set_missing(msgid: object, key: str) -> None:
        forbidden = {"forecastTime", "shortName", "paramId", "level", "typeOfLevel", "gridType", "missingValue"}
        if key in forbidden:
            raise value_cannot_be_missing_error("Value cannot be missing")
        set_override(msgid, key, _MISSING)

    def codes_set_key_vals(gribid: object, key_vals) -> None:
        if isinstance(key_vals, str):
            items = [item.strip() for item in key_vals.split(",") if item.strip()]
            if not items:
                raise codes_internal_error("Empty key/values argument")
            for item in items:
                if "=" not in item:
                    raise codes_internal_error(f"Invalid key/value pair '{item}'")
                key, value = item.split("=", 1)
                if value.replace(".", "", 1).isdigit():
                    if "." in value:
                        codes_set_double(gribid, key, float(value))
                    else:
                        codes_set_long(gribid, key, int(value))
                else:
                    codes_set_string(gribid, key, value)
            return
        if isinstance(key_vals, (list, tuple)):
            for item in key_vals:
                if not isinstance(item, str) or "=" not in item:
                    raise codes_internal_error(f"Invalid list/tuple element format '{item}'")
            return codes_set_key_vals(gribid, ",".join(key_vals))
        if isinstance(key_vals, dict):
            for key, value in key_vals.items():
                codes_set(gribid, key, value)
            return
        raise TypeError("Invalid argument type")

    def codes_get(msgid: object, key: str, ktype=None):
        if key == "totalLength":
            value = codes_get_message_size(msgid)
            if ktype is float:
                return float(value)
            if ktype is str:
                return str(value)
            return int(value)
        value = value_from_handle(msgid, key, raw_codes_get)
        if isinstance(value, (np.ndarray, tuple)):
            return array_to_requested(value, ktype)
        if ktype is int:
            return int(value)
        if ktype is float:
            return float(value)
        if ktype is str:
            return str(value)
        return value

    def codes_get_long(msgid: object, key: str) -> int:
        if key == "totalLength":
            return int(codes_get_message_size(msgid))
        value = value_from_handle(msgid, key, raw_codes_get_long)
        return int(value)

    def codes_get_double(msgid: object, key: str) -> float:
        value = value_from_handle(msgid, key, raw_codes_get_double)
        return float(value)

    def codes_get_string(msgid: object, key: str) -> str:
        value = value_from_handle(msgid, key, raw_codes_get_string)
        return str(value)

    def codes_get_size(msgid: object, key: str) -> int:
        exists, value = override_for(msgid, key)
        if exists:
            if value is _MISSING:
                return 1
            if isinstance(value, np.ndarray):
                return int(value.size)
            if isinstance(value, tuple):
                return len(value)
            return len(value) if isinstance(value, str) else 1
        if key in _DERIVED_SERIALIZED_KEYS:
            serialized = maybe_serialized_message(msgid)
            if serialized is not None:
                temp = raw_codes_new_from_message(serialized)
                try:
                    return raw_codes_get_size(temp, key)
                finally:
                    raw_codes_release(temp)
        return raw_codes_get_size(msgid, key)

    def codes_is_defined(msgid: object, key: str) -> bool:
        exists, value = override_for(msgid, key)
        if exists:
            return value is not _MISSING
        if key in _DERIVED_SERIALIZED_KEYS:
            serialized = maybe_serialized_message(msgid)
            if serialized is not None:
                temp = raw_codes_new_from_message(serialized)
                try:
                    return raw_codes_is_defined(temp, key)
                finally:
                    raw_codes_release(temp)
        return raw_codes_is_defined(msgid, key)

    def codes_is_missing(msgid: object, key: str) -> bool:
        exists, value = override_for(msgid, key)
        if exists:
            return value is _MISSING
        return raw_codes_is_missing(msgid, key)

    def codes_get_native_type(msgid: object, key: str):
        kind = infer_kind(msgid, key)
        if kind == "missing":
            return None
        return kind

    def codes_get_array(msgid: object, key: str, ktype=None):
        exists, value = override_for(msgid, key)
        if exists:
            if value is _MISSING:
                return np.asarray([], dtype=np.float64)
            if key == "values":
                if ktype is int:
                    return materialize_values_array(msgid, value, np.int64)
                if ktype in (None, float):
                    return materialize_values_array(msgid, value, np.float64)
                return materialize_values_array(msgid, value, np.float32)
            return array_to_requested(value, ktype)
        raw = raw_codes_get_array(msgid, key, ktype)
        if key == "values":
            if ktype is int:
                return materialize_values_array(msgid, raw, np.int64)
            if ktype in (None, float):
                return materialize_values_array(msgid, raw, np.float64)
            return materialize_values_array(msgid, raw, np.float32)
        return raw

    def codes_get_double_array(msgid: object, key: str):
        exists, value = override_for(msgid, key)
        if exists:
            if key == "values":
                return materialize_values_array(msgid, value, np.float64)
            return np.asarray(value, dtype=np.float64)
        raw = raw_codes_get_double_array(msgid, key)
        if key == "values":
            return materialize_values_array(msgid, raw, np.float64)
        return raw

    def codes_get_float_array(msgid: object, key: str):
        exists, value = override_for(msgid, key)
        if exists:
            if key == "values":
                return materialize_values_array(msgid, value, np.float32)
            return np.asarray(value, dtype=np.float32)
        raw = raw_codes_get_float_array(msgid, key)
        if key == "values":
            return materialize_values_array(msgid, raw, np.float32)
        return raw

    def codes_get_long_array(msgid: object, key: str):
        exists, value = override_for(msgid, key)
        if exists:
            if key == "values":
                return materialize_values_array(msgid, value, np.int64)
            return np.asarray(value, dtype=np.int64)
        raw = raw_codes_get_long_array(msgid, key)
        if key == "values":
            return materialize_values_array(msgid, raw, np.int64)
        return raw

    def codes_get_string_array(msgid: object, key: str):
        exists, value = override_for(msgid, key)
        if exists:
            if isinstance(value, tuple):
                return value
            if isinstance(value, np.ndarray):
                return tuple(str(item) for item in value.tolist())
            return (str(value),)
        return raw_codes_get_string_array(msgid, key)

    def codes_get_values(gribid: object, ktype=None):
        return codes_get_array(gribid, "values", ktype if ktype is not None else float)

    def codes_get_message(msgid: object):
        serialized = maybe_serialized_message(msgid)
        if serialized is not None:
            return serialized
        raw = raw_codes_get_message(msgid)
        return bytes(raw)

    def codes_get_message_size(msgid: object) -> int:
        serialized = maybe_serialized_message(msgid)
        if serialized is not None:
            return len(serialized)
        return raw_codes_get_message_size(msgid)

    def codes_get_string_length(msgid: object, key: str) -> int:
        exists, value = override_for(msgid, key)
        if exists:
            if value is _MISSING:
                return _STRING_BUFFER_SMALL
            return _string_buffer_length(str(value))
        if key in _DERIVED_SERIALIZED_KEYS:
            serialized = maybe_serialized_message(msgid)
            if serialized is not None:
                temp = raw_codes_new_from_message(serialized)
                try:
                    return raw_codes_get_string_length(temp, key)
                finally:
                    raw_codes_release(temp)
        return raw_codes_get_string_length(msgid, key)

    def codes_clone(msgid: object):
        clone = raw_codes_clone(msgid)
        copy_handle_state(msgid, clone)
        return clone

    def codes_release(msgid: object) -> None:
        _HANDLE_STATE.pop(id(msgid), None)
        raw_codes_release(msgid)

    def codes_write(msgid: object, fileobj) -> None:
        fileobj.write(codes_get_message(msgid))

    def codes_copy_namespace(gribid_src: object, namespace: str, gribid_dest: object) -> None:
        del gribid_src, namespace, gribid_dest
        return None

    def codes_dump(msgid: object, output_fileobj=sys.stdout, mode: str = "wmo", flags: int = 0) -> None:
        del flags
        keys = {}
        iterator = ns["codes_keys_iterator_new"](msgid)
        while ns["codes_keys_iterator_next"](iterator):
            key = ns["codes_keys_iterator_get_name"](iterator)
            keys[key] = codes_get(msgid, key)
        ns["codes_keys_iterator_delete"](iterator)
        for key, value in state_for(msgid)["overrides"].items():
            keys[key] = value

        if mode == "json":
            serializable = {}
            for key, value in keys.items():
                if value is _MISSING:
                    serializable[key] = None
                elif isinstance(value, np.ndarray):
                    serializable[key] = value.tolist()
                elif isinstance(value, tuple):
                    serializable[key] = list(value)
                else:
                    serializable[key] = value
            output_fileobj.write(json.dumps(serializable, indent=2, sort_keys=True))
            output_fileobj.write("\n")
            return

        for key in sorted(keys):
            value = keys[key]
            if value is _MISSING:
                rendered = "MISSING"
            elif isinstance(value, np.ndarray):
                rendered = np.array2string(value, threshold=8)
            else:
                rendered = str(value)
            output_fileobj.write(f"{key} = {rendered}\n")

    def codes_get_gaussian_latitudes(order: int):
        roots, _ = np.polynomial.legendre.leggauss(order * 2)
        return np.rad2deg(np.arcsin(roots[::-1])).astype(np.float64)

    def codes_grib_multi_new():
        return []

    def codes_grib_multi_append(ingribid: object, startsection: int, multigribid) -> None:
        multigribid.append({"startsection": startsection, "message": codes_get_message(ingribid)})

    def codes_grib_multi_write(multigribid, fileobj) -> None:
        for item in multigribid:
            fileobj.write(item["message"])

    def codes_grib_multi_release(gribid) -> None:
        gribid.clear()

    def codes_index_new_from_file(filename: str, keys) -> object:
        indexid = raw_codes_index_new_from_file(filename, list(keys))
        _INDEX_STATE[id(indexid)] = {
            "index": indexid,
            "files": [filename],
            "keys": list(keys),
            "selections": [],
        }
        return indexid

    def codes_index_add_file(indexid: object, filename: str) -> None:
        raw_codes_index_add_file(indexid, filename)
        index_state_for(indexid)["files"].append(filename)

    def codes_index_select(indexid: object, key: str, value) -> None:
        raw_codes_index_select(indexid, key, value)
        state = index_state_for(indexid)
        state["selections"] = [item for item in state["selections"] if item["key"] != key]
        state["selections"].append({"kind": "generic", "key": key, "value": value})

    def codes_index_select_long(indexid: object, key: str, value: int) -> None:
        raw_codes_index_select_long(indexid, key, value)
        state = index_state_for(indexid)
        state["selections"] = [item for item in state["selections"] if item["key"] != key]
        state["selections"].append({"kind": "long", "key": key, "value": int(value)})

    def codes_index_select_double(indexid: object, key: str, value: float) -> None:
        raw_codes_index_select_double(indexid, key, value)
        state = index_state_for(indexid)
        state["selections"] = [item for item in state["selections"] if item["key"] != key]
        state["selections"].append({"kind": "double", "key": key, "value": float(value)})

    def codes_index_select_string(indexid: object, key: str, value: str) -> None:
        raw_codes_index_select_string(indexid, key, value)
        state = index_state_for(indexid)
        state["selections"] = [item for item in state["selections"] if item["key"] != key]
        state["selections"].append({"kind": "string", "key": key, "value": str(value)})

    def codes_index_write(indexid: object, filename: str) -> None:
        state = _INDEX_STATE.get(id(indexid))
        if state is None:
            raise invalid_index_error("index metadata is not available")
        payload = {k: v for k, v in state.items() if k != "index"}
        Path(filename).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def codes_index_read(filename: str):
        payload = json.loads(Path(filename).read_text(encoding="utf-8"))
        files = payload.get("files") or []
        keys = payload.get("keys") or []
        if not files or not keys:
            raise invalid_index_error("serialized index is missing file or key metadata")
        indexid = codes_index_new_from_file(files[0], keys)
        for path in files[1:]:
            codes_index_add_file(indexid, path)
        for selection in payload.get("selections", []):
            kind = selection["kind"]
            if kind == "long":
                codes_index_select_long(indexid, selection["key"], selection["value"])
            elif kind == "double":
                codes_index_select_double(indexid, selection["key"], selection["value"])
            elif kind == "string":
                codes_index_select_string(indexid, selection["key"], selection["value"])
            else:
                codes_index_select(indexid, selection["key"], selection["value"])
        return indexid

    def codes_index_release(indexid: object) -> None:
        _INDEX_STATE.pop(id(indexid), None)
        raw_codes_index_release(indexid)

    def codes_new_from_index(indexid: object):
        return raw_codes_new_from_index(indexid)

    def codes_grib_iterator_new(gribid: object, mode: int):
        points = handle_points(gribid)
        if points is None:
            return raw_codes_grib_iterator_new(gribid, mode)
        return {"kind": "python_grib_iterator", "cursor": 0, "points": points}

    def codes_grib_iterator_next(iterid):
        if isinstance(iterid, dict) and iterid.get("kind") == "python_grib_iterator":
            if iterid["cursor"] >= len(iterid["points"]):
                return []
            point = iterid["points"][iterid["cursor"]]
            iterid["cursor"] += 1
            return point
        return raw_codes_grib_iterator_next(iterid)

    def codes_grib_iterator_delete(iterid) -> None:
        if isinstance(iterid, dict) and iterid.get("kind") == "python_grib_iterator":
            iterid["points"] = []
            return
        raw_codes_grib_iterator_delete(iterid)

    def codes_grib_get_data(gribid: object):
        points = handle_points(gribid)
        if points is None:
            return raw_codes_grib_get_data(gribid)
        return tuple({"lat": lat, "lon": lon, "value": value} for lat, lon, value in points)

    def codes_grib_nearest_new(msgid: object):
        points = handle_points(msgid)
        if points is None:
            return raw_codes_grib_nearest_new(msgid)
        return {"kind": "python_nearest"}

    def codes_grib_nearest_find(nid, gribid: object, inlat: float, inlon: float, flags: int, is_lsm: bool = False, npoints: int = 4):
        if isinstance(nid, dict) and nid.get("kind") == "python_nearest":
            matches = nearest_objects(gribid, inlat, inlon, npoints)
            if matches is None:
                raise invalid_nearest_error("no grid points are available")
            return matches
        return raw_codes_grib_nearest_find(nid, gribid, inlat, inlon, flags, is_lsm, npoints)

    def codes_grib_nearest_delete(nid) -> None:
        if isinstance(nid, dict) and nid.get("kind") == "python_nearest":
            nid.clear()
            return
        raw_codes_grib_nearest_delete(nid)

    def codes_grib_find_nearest(gribid: object, inlat: float, inlon: float, is_lsm: bool = False, npoints: int = 1):
        matches = nearest_objects(gribid, inlat, inlon, npoints)
        if matches is not None:
            return matches
        return raw_codes_grib_find_nearest(gribid, inlat, inlon, is_lsm, npoints)

    def codes_grib_find_nearest_multiple(gribid: object, is_lsm: bool, inlats, inlons):
        if len(inlats) != len(inlons):
            raise invalid_nearest_error("inlats and inlons must have the same length")
        matches = []
        for lat, lon in zip(inlats, inlons):
            nearest = codes_grib_find_nearest(gribid, lat, lon, is_lsm, 1)
            matches.append(nearest[0])
        return tuple(matches)

    ns["codes_any_new_from_samples"] = codes_any_new_from_samples
    ns["codes_new_from_samples"] = codes_new_from_samples
    ns["codes_grib_new_from_samples"] = codes_grib_new_from_samples
    ns["codes_bufr_new_from_samples"] = unsupported_product("BUFR")
    ns["codes_bufr_new_from_file"] = unsupported_product("BUFR")
    ns["codes_metar_new_from_file"] = unsupported_product("METAR")
    ns["codes_gts_new_from_file"] = unsupported_product("GTS")
    ns["codes_bufr_extract_headers"] = unsupported_product("BUFR")
    ns["codes_bufr_copy_data"] = unsupported_product("BUFR")
    ns["codes_bufr_key_is_coordinate"] = unsupported_product("BUFR")
    ns["codes_bufr_key_is_header"] = unsupported_product("BUFR")
    ns["codes_bufr_keys_iterator_new"] = unsupported_product("BUFR")
    ns["codes_bufr_keys_iterator_next"] = unsupported_product("BUFR")
    ns["codes_bufr_keys_iterator_get_name"] = unsupported_product("BUFR")
    ns["codes_bufr_keys_iterator_rewind"] = unsupported_product("BUFR")
    ns["codes_bufr_keys_iterator_delete"] = unsupported_product("BUFR")
    ns["codes_copy_namespace"] = codes_copy_namespace
    ns["codes_dump"] = codes_dump
    ns["codes_get_gaussian_latitudes"] = codes_get_gaussian_latitudes
    ns["codes_grib_multi_append"] = codes_grib_multi_append
    ns["codes_grib_multi_new"] = codes_grib_multi_new
    ns["codes_grib_multi_release"] = codes_grib_multi_release
    ns["codes_grib_multi_write"] = codes_grib_multi_write
    ns["codes_gts_header"] = unsupported_product("GTS")
    ns["codes_index_new_from_file"] = codes_index_new_from_file
    ns["codes_index_add_file"] = codes_index_add_file
    ns["codes_index_select"] = codes_index_select
    ns["codes_index_select_long"] = codes_index_select_long
    ns["codes_index_select_double"] = codes_index_select_double
    ns["codes_index_select_string"] = codes_index_select_string
    ns["codes_index_write"] = codes_index_write
    ns["codes_index_read"] = codes_index_read
    ns["codes_index_release"] = codes_index_release
    ns["codes_new_from_index"] = codes_new_from_index
    ns["codes_set"] = codes_set
    ns["codes_set_array"] = codes_set_array
    ns["codes_set_double"] = codes_set_double
    ns["codes_set_double_array"] = codes_set_double_array
    ns["codes_set_key_vals"] = codes_set_key_vals
    ns["codes_set_long"] = codes_set_long
    ns["codes_set_long_array"] = codes_set_long_array
    ns["codes_set_missing"] = codes_set_missing
    ns["codes_set_string"] = codes_set_string
    ns["codes_set_string_array"] = codes_set_string_array
    ns["codes_set_values"] = codes_set_values
    ns["codes_get"] = codes_get
    ns["codes_get_long"] = codes_get_long
    ns["codes_get_double"] = codes_get_double
    ns["codes_get_string"] = codes_get_string
    ns["codes_get_array"] = codes_get_array
    ns["codes_get_double_array"] = codes_get_double_array
    ns["codes_get_float_array"] = codes_get_float_array
    ns["codes_get_long_array"] = codes_get_long_array
    ns["codes_get_string_array"] = codes_get_string_array
    ns["codes_get_values"] = codes_get_values
    ns["codes_get_size"] = codes_get_size
    ns["codes_get_native_type"] = codes_get_native_type
    ns["codes_get_message"] = codes_get_message
    ns["codes_get_message_size"] = codes_get_message_size
    ns["codes_get_string_length"] = codes_get_string_length
    ns["codes_is_defined"] = codes_is_defined
    ns["codes_is_missing"] = codes_is_missing
    ns["codes_clone"] = codes_clone
    ns["codes_release"] = codes_release
    ns["codes_write"] = codes_write
    ns["codes_grib_iterator_new"] = codes_grib_iterator_new
    ns["codes_grib_iterator_next"] = codes_grib_iterator_next
    ns["codes_grib_iterator_delete"] = codes_grib_iterator_delete
    ns["codes_grib_get_data"] = codes_grib_get_data
    ns["codes_grib_nearest_new"] = codes_grib_nearest_new
    ns["codes_grib_nearest_find"] = codes_grib_nearest_find
    ns["codes_grib_nearest_delete"] = codes_grib_nearest_delete
    ns["codes_grib_find_nearest"] = codes_grib_find_nearest
    ns["codes_grib_find_nearest_multiple"] = codes_grib_find_nearest_multiple
