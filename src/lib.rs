mod compat;
mod grib;
mod grib2_compat;
mod grib_cache;
mod types;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use compat::{CodesError, CodesHandle as InnerCodesHandle, CodesValue};
use grib_cache::{
    cached_grib_file, clear_grib_file_cache, grib_file_cache_info, set_grib_file_cache_enabled,
    set_grib_file_cache_limits, CachedGribFile,
};
use numpy::{ndarray::ArrayView1, npyffi::flags, IntoPyArray, PyArray1, PyUntypedArrayMethods};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use pyo3::wrap_pyfunction;

#[cfg(all(target_os = "windows", not(feature = "system-allocator")))]
use mimalloc_windows::MiMalloc;

#[cfg(all(not(target_os = "windows"), feature = "mimalloc"))]
use mimalloc::MiMalloc;

#[cfg(any(
    all(target_os = "windows", not(feature = "system-allocator")),
    all(not(target_os = "windows"), feature = "mimalloc")
))]
#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

create_exception!(_ecrust, CodesInternalError, PyException);
create_exception!(_ecrust, KeyValueNotFoundError, CodesInternalError);
create_exception!(_ecrust, InvalidTypeError, CodesInternalError);
create_exception!(_ecrust, EndOfFileError, CodesInternalError);
create_exception!(_ecrust, FunctionNotImplementedError, CodesInternalError);
create_exception!(_ecrust, InvalidKeysIteratorError, CodesInternalError);
create_exception!(_ecrust, InvalidGribIteratorError, CodesInternalError);
create_exception!(_ecrust, InvalidIndexError, CodesInternalError);
create_exception!(_ecrust, InvalidNearestError, CodesInternalError);

const VERSION: &str = "0.1.0";
const CODES_PRODUCT_ANY: i32 = 0;
const CODES_PRODUCT_GRIB: i32 = 1;
const CODES_PRODUCT_BUFR: i32 = 2;
const CODES_PRODUCT_METAR: i32 = 3;
const CODES_PRODUCT_GTS: i32 = 4;
const CODES_MISSING_LONG: i64 = 2_147_483_647;
const CODES_MISSING_DOUBLE: f64 = -1.0e100;
const STRING_BUFFER_SMALL: usize = 256;
const STRING_BUFFER_LARGE: usize = 1025;
const FEATURE_SELECT_ALL: i32 = 0;
const FEATURE_SELECT_ENABLED: i32 = 1;
const FEATURE_SELECT_DISABLED: i32 = 2;
const ENABLED_FEATURES: &str = "RUST GRIB1 GRIB2 AEC JPG PNG MEMFS";
const DISABLED_FEATURES: &str = "BUFR GTS METAR NETCDF FORTRAN";

const fn allocator_name() -> &'static str {
    if cfg!(all(target_os = "windows", not(feature = "system-allocator")))
        || cfg!(all(not(target_os = "windows"), feature = "mimalloc"))
    {
        "mimalloc"
    } else {
        "system"
    }
}

#[pyclass(module = "ecrust._ecrust", name = "CodesHandle")]
#[derive(Clone)]
struct PyCodesHandle {
    inner: InnerCodesHandle,
}

#[pyclass(module = "ecrust._ecrust", name = "CodesKeysIterator")]
struct PyCodesKeysIterator {
    keys: Vec<String>,
    cursor: usize,
    current: Option<usize>,
}

#[pyclass(module = "ecrust._ecrust", name = "CodesGribIterator")]
struct PyCodesGribIterator {
    points: Vec<(f64, f64, f64)>,
    cursor: usize,
}

#[pyclass(module = "ecrust._ecrust", name = "CodesNearest")]
#[derive(Clone, Default)]
struct PyCodesNearest;

enum RequestedType {
    Native,
    Long,
    Double,
    Float32,
    String,
    Bytes,
}

#[derive(Clone, Copy)]
enum IndexValueType {
    String,
    Long,
    Double,
}

#[derive(Clone)]
struct IndexedKeySpec {
    key: String,
    value_type: IndexValueType,
}

#[derive(Clone)]
enum IndexSelectionValue {
    String(String),
    Long(i64),
    Double(f64),
}

#[pyclass(module = "ecrust._ecrust", name = "CodesIndex")]
struct PyCodesIndex {
    entries: Vec<PyCodesHandle>,
    key_specs: Vec<IndexedKeySpec>,
    selections: BTreeMap<String, IndexSelectionValue>,
    cursor: usize,
}

impl From<CodesError> for PyErr {
    fn from(value: CodesError) -> Self {
        match value {
            CodesError::MissingKey(key) => {
                KeyValueNotFoundError::new_err(format!("key '{key}' is not defined"))
            }
            CodesError::TypeMismatch {
                key,
                expected,
                actual,
            } => InvalidTypeError::new_err(format!(
                "key '{key}' cannot be read as {expected}; actual type is {actual}"
            )),
            CodesError::NoMessages(message) => EndOfFileError::new_err(message),
            CodesError::Engine(message) => CodesInternalError::new_err(message),
        }
    }
}

fn definitions_path_cell() -> &'static Mutex<String> {
    static CELL: OnceLock<Mutex<String>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new("/MEMFS/definitions".to_string()))
}

fn samples_path_cell() -> &'static Mutex<String> {
    static CELL: OnceLock<Mutex<String>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new("/MEMFS/samples".to_string()))
}

fn lock_string(cell: &'static Mutex<String>) -> PyResult<String> {
    cell.lock()
        .map(|value| value.clone())
        .map_err(|_| CodesInternalError::new_err("internal state lock poisoned"))
}

fn set_locked_string(cell: &'static Mutex<String>, value: &str) -> PyResult<()> {
    let mut guard = cell
        .lock()
        .map_err(|_| CodesInternalError::new_err("internal state lock poisoned"))?;
    *guard = value.to_string();
    Ok(())
}

fn file_path_from_obj(fileobj: &Bound<'_, PyAny>) -> PyResult<PathBuf> {
    if let Ok(path) = fileobj.extract::<PathBuf>() {
        return Ok(path);
    }
    if let Ok(path) = fileobj.extract::<String>() {
        return Ok(PathBuf::from(path));
    }
    if fileobj.hasattr("name")? {
        if let Ok(path) = fileobj.getattr("name")?.extract::<String>() {
            return Ok(PathBuf::from(path));
        }
    }
    Err(CodesInternalError::new_err(
        "expected a filesystem path or a file-like object with a .name attribute",
    ))
}

fn file_position(fileobj: &Bound<'_, PyAny>) -> PyResult<Option<u64>> {
    if fileobj.hasattr("tell")? {
        let pos = fileobj.call_method0("tell")?.extract::<u64>()?;
        Ok(Some(pos))
    } else {
        Ok(None)
    }
}

fn seek_file(fileobj: &Bound<'_, PyAny>, pos: u64) -> PyResult<()> {
    if fileobj.hasattr("seek")? {
        fileobj.call_method1("seek", (pos,))?;
    }
    Ok(())
}

fn handle_from_cached_message(
    cached: std::sync::Arc<CachedGribFile>,
    message: &grib::MessageDescriptor,
) -> PyResult<PyCodesHandle> {
    let inner =
        InnerCodesHandle::from_cached_message(cached, message.clone()).map_err(PyErr::from)?;
    Ok(PyCodesHandle { inner })
}

fn handle_from_message_bytes(raw_message: Vec<u8>) -> PyResult<PyCodesHandle> {
    let inner = InnerCodesHandle::from_bytes(&raw_message, 1).map_err(PyErr::from)?;
    Ok(PyCodesHandle { inner })
}

fn load_grib_handles_from_path(path: &Path) -> PyResult<Vec<PyCodesHandle>> {
    let cached = cached_grib_file(path).map_err(CodesInternalError::new_err)?;
    cached
        .inventory()
        .messages
        .iter()
        .map(|message| handle_from_cached_message(cached.clone(), message))
        .collect()
}

fn next_grib_handle_from_fileobj(fileobj: &Bound<'_, PyAny>) -> PyResult<Option<PyCodesHandle>> {
    let path = file_path_from_obj(fileobj)?;
    let start_pos = file_position(fileobj)?.unwrap_or(0);
    let cached = cached_grib_file(&path).map_err(CodesInternalError::new_err)?;
    let next_index = cached.inventory().messages.partition_point(|message| {
        message.offset_bytes.saturating_add(message.length_bytes) <= start_pos
    });
    let next_message = cached.inventory().messages.get(next_index);

    match next_message {
        Some(message) => {
            seek_file(fileobj, message.offset_bytes + message.length_bytes)?;
            let handle = handle_from_cached_message(cached.clone(), message)?;
            Ok(Some(handle))
        }
        None => Ok(None),
    }
}

fn requested_type(ktype: Option<&Bound<'_, PyAny>>) -> PyResult<RequestedType> {
    let Some(ktype) = ktype else {
        return Ok(RequestedType::Native);
    };
    let text = ktype.str()?.to_str()?.to_ascii_lowercase();
    if text.contains("float32") {
        Ok(RequestedType::Float32)
    } else if text.contains("float64") || text.contains("float") {
        Ok(RequestedType::Double)
    } else if text.contains("int") {
        Ok(RequestedType::Long)
    } else if text.contains("bytes") {
        Ok(RequestedType::Bytes)
    } else if text.contains("str") || text.contains("unicode") {
        Ok(RequestedType::String)
    } else {
        Ok(RequestedType::Native)
    }
}

fn value_to_pyobject(
    py: Python<'_>,
    value: CodesValue,
    requested: RequestedType,
) -> PyResult<PyObject> {
    match value {
        CodesValue::Long(v) => match requested {
            RequestedType::Double | RequestedType::Float32 => {
                Ok((v as f64).into_pyobject(py)?.into_any().unbind())
            }
            RequestedType::String => Ok(v.to_string().into_pyobject(py)?.into_any().unbind()),
            RequestedType::Bytes => Ok(PyBytes::new(py, v.to_string().as_bytes())
                .into_any()
                .unbind()),
            _ => Ok(v.into_pyobject(py)?.into_any().unbind()),
        },
        CodesValue::Double(v) => match requested {
            RequestedType::Long => Ok((v as i64).into_pyobject(py)?.into_any().unbind()),
            RequestedType::String => Ok(v.to_string().into_pyobject(py)?.into_any().unbind()),
            RequestedType::Bytes => Ok(PyBytes::new(py, v.to_string().as_bytes())
                .into_any()
                .unbind()),
            _ => Ok(v.into_pyobject(py)?.into_any().unbind()),
        },
        CodesValue::String(v) => match requested {
            RequestedType::Long => v
                .parse::<i64>()
                .map(|x| x.into_pyobject(py).expect("int").into_any().unbind())
                .map_err(|_| InvalidTypeError::new_err("cannot coerce string to int")),
            RequestedType::Double | RequestedType::Float32 => v
                .parse::<f64>()
                .map(|x| x.into_pyobject(py).expect("float").into_any().unbind())
                .map_err(|_| InvalidTypeError::new_err("cannot coerce string to float")),
            RequestedType::Bytes => Ok(PyBytes::new(py, v.as_bytes()).into_any().unbind()),
            _ => Ok(v.into_pyobject(py)?.into_any().unbind()),
        },
        CodesValue::DoubleArray(values) => array_to_pyobject(py, values, requested),
        CodesValue::Missing => Ok(py.None()),
    }
}

fn array_to_pyobject(
    py: Python<'_>,
    values: Vec<f64>,
    requested: RequestedType,
) -> PyResult<PyObject> {
    match requested {
        RequestedType::Long => Ok(values
            .into_iter()
            .map(|value| value as i64)
            .collect::<Vec<_>>()
            .into_pyarray(py)
            .into_any()
            .unbind()),
        RequestedType::Float32 => Ok(values
            .into_iter()
            .map(|value| value as f32)
            .collect::<Vec<_>>()
            .into_pyarray(py)
            .into_any()
            .unbind()),
        RequestedType::String | RequestedType::Bytes => Err(InvalidTypeError::new_err(
            "string and bytes array coercions are not implemented",
        )),
        RequestedType::Native | RequestedType::Double => {
            Ok(values.into_pyarray(py).into_any().unbind())
        }
    }
}

fn slice_to_pyobject(
    py: Python<'_>,
    values: &[f64],
    requested: RequestedType,
) -> PyResult<PyObject> {
    match requested {
        RequestedType::Long => Ok(values
            .iter()
            .map(|&value| value as i64)
            .collect::<Vec<_>>()
            .into_pyarray(py)
            .into_any()
            .unbind()),
        RequestedType::Float32 => Ok(values
            .iter()
            .map(|&value| value as f32)
            .collect::<Vec<_>>()
            .into_pyarray(py)
            .into_any()
            .unbind()),
        RequestedType::String | RequestedType::Bytes => Err(InvalidTypeError::new_err(
            "string and bytes array coercions are not implemented",
        )),
        RequestedType::Native | RequestedType::Double => {
            Ok(PyArray1::from_slice(py, values).into_any().unbind())
        }
    }
}

fn borrowed_values_array<'py>(
    msgid: Bound<'py, PyCodesHandle>,
) -> PyResult<Bound<'py, PyArray1<f64>>> {
    let container = msgid.clone().into_any();
    let array = {
        let handle = msgid.borrow();
        let values = handle.inner.values().map_err(PyErr::from)?;
        let view = ArrayView1::from(values);
        unsafe { PyArray1::borrow_from_array(&view, container) }
    };
    unsafe {
        (*array.as_array_ptr()).flags &= !flags::NPY_ARRAY_WRITEABLE;
    }
    Ok(array)
}

fn current_iterator_name(iterid: &PyCodesKeysIterator) -> PyResult<&str> {
    match iterid.current.and_then(|idx| iterid.keys.get(idx)) {
        Some(name) => Ok(name.as_str()),
        None => Err(InvalidKeysIteratorError::new_err(
            "iterator is not currently positioned on a key",
        )),
    }
}

fn message_points(handle: &PyCodesHandle) -> PyResult<Vec<(f64, f64, f64)>> {
    let latitudes = handle
        .inner
        .get_double_array("latitudes")
        .map_err(PyErr::from)?;
    let longitudes = handle
        .inner
        .get_double_array("longitudes")
        .map_err(PyErr::from)?;
    let values = handle.inner.values().map_err(PyErr::from)?;
    if latitudes.len() != longitudes.len() || latitudes.len() != values.len() {
        return Err(CodesInternalError::new_err(
            "latitudes, longitudes, and values arrays are inconsistent",
        ));
    }
    Ok(latitudes
        .into_iter()
        .zip(longitudes)
        .zip(values.iter().copied())
        .map(|((lat, lon), value)| (lat, lon, value))
        .collect())
}

fn haversine_distance_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let earth_radius_km = 6_371.0088_f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    earth_radius_km * c
}

fn nearest_matches(
    handle: &PyCodesHandle,
    inlat: f64,
    inlon: f64,
) -> PyResult<Vec<(usize, f64, f64, f64, f64)>> {
    let mut matches = message_points(handle)?
        .into_iter()
        .enumerate()
        .map(|(index, (lat, lon, value))| {
            (
                index,
                haversine_distance_km(inlat, inlon, lat, lon),
                lat,
                lon,
                value,
            )
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
            .then(Ordering::Equal)
    });
    Ok(matches)
}

fn nearest_dict(
    py: Python<'_>,
    index: usize,
    distance: f64,
    lat: f64,
    lon: f64,
    value: f64,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("lat", lat)?;
    dict.set_item("lon", lon)?;
    dict.set_item("value", value)?;
    dict.set_item("distance", distance)?;
    dict.set_item("index", index)?;
    Ok(dict.into_any().unbind())
}

fn tuple_from_objects(py: Python<'_>, values: Vec<PyObject>) -> PyResult<PyObject> {
    Ok(PyTuple::new(py, values)?.into_any().unbind())
}

fn tuple_from_strings(py: Python<'_>, values: Vec<String>) -> PyResult<PyObject> {
    let objects = values
        .into_iter()
        .map(|value| value.into_pyobject(py).expect("string").into_any().unbind())
        .collect::<Vec<_>>();
    tuple_from_objects(py, objects)
}

fn tuple_from_longs(py: Python<'_>, values: Vec<i64>) -> PyResult<PyObject> {
    let objects = values
        .into_iter()
        .map(|value| value.into_pyobject(py).expect("int").into_any().unbind())
        .collect::<Vec<_>>();
    tuple_from_objects(py, objects)
}

fn tuple_from_doubles(py: Python<'_>, values: Vec<f64>) -> PyResult<PyObject> {
    let objects = values
        .into_iter()
        .map(|value| value.into_pyobject(py).expect("float").into_any().unbind())
        .collect::<Vec<_>>();
    tuple_from_objects(py, objects)
}

fn string_buffer_length(value: &str) -> usize {
    if value.len() > 32 {
        STRING_BUFFER_LARGE
    } else {
        STRING_BUFFER_SMALL
    }
}

fn parse_index_key(key: &str) -> IndexedKeySpec {
    let (name, suffix) = key.split_once(':').unwrap_or((key, ""));
    let value_type = match suffix {
        "l" | "L" | "i" | "I" => IndexValueType::Long,
        "d" | "D" | "f" | "F" => IndexValueType::Double,
        _ => IndexValueType::String,
    };
    IndexedKeySpec {
        key: name.to_string(),
        value_type,
    }
}

fn key_spec_for<'a>(indexid: &'a PyCodesIndex, key: &str) -> Option<&'a IndexedKeySpec> {
    indexid.key_specs.iter().find(|spec| spec.key == key)
}

fn reset_index_cursor(indexid: &mut PyCodesIndex) {
    indexid.cursor = 0;
}

fn all_index_keys_selected(indexid: &PyCodesIndex) -> bool {
    indexid
        .key_specs
        .iter()
        .all(|spec| indexid.selections.contains_key(&spec.key))
}

fn index_entry_matches(handle: &PyCodesHandle, selection: &IndexSelectionValue, key: &str) -> bool {
    match selection {
        IndexSelectionValue::String(value) => handle
            .inner
            .get_string(key)
            .map(|candidate| candidate == *value)
            .unwrap_or(false),
        IndexSelectionValue::Long(value) => handle
            .inner
            .get_long(key)
            .map(|candidate| candidate == *value)
            .unwrap_or(false),
        IndexSelectionValue::Double(value) => handle
            .inner
            .get_double(key)
            .map(|candidate| (candidate - *value).abs() <= 1.0e-9)
            .unwrap_or(false),
    }
}

fn selection_from_py(
    indexid: &PyCodesIndex,
    key: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<IndexSelectionValue> {
    match key_spec_for(indexid, key).map(|spec| spec.value_type) {
        Some(IndexValueType::Long) => Ok(IndexSelectionValue::Long(value.extract::<i64>()?)),
        Some(IndexValueType::Double) => Ok(IndexSelectionValue::Double(value.extract::<f64>()?)),
        _ => {
            if let Ok(long_value) = value.extract::<i64>() {
                Ok(IndexSelectionValue::Long(long_value))
            } else if let Ok(double_value) = value.extract::<f64>() {
                Ok(IndexSelectionValue::Double(double_value))
            } else {
                Ok(IndexSelectionValue::String(value.extract::<String>()?))
            }
        }
    }
}

fn index_string_values(indexid: &PyCodesIndex, key: &str) -> Vec<String> {
    let mut values = indexid
        .entries
        .iter()
        .filter_map(|handle| handle.inner.get_string(key).ok())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn index_long_values(indexid: &PyCodesIndex, key: &str) -> Vec<i64> {
    let mut values = indexid
        .entries
        .iter()
        .filter_map(|handle| handle.inner.get_long(key).ok())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn index_double_values(indexid: &PyCodesIndex, key: &str) -> Vec<f64> {
    let mut values = indexid
        .entries
        .iter()
        .filter_map(|handle| handle.inner.get_double(key).ok())
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.total_cmp(right));
    values.dedup_by(|left, right| left.total_cmp(right) == Ordering::Equal);
    values
}

#[pyfunction]
#[pyo3(signature = (fileobj, headers_only=false))]
fn codes_grib_new_from_file(
    fileobj: &Bound<'_, PyAny>,
    headers_only: bool,
) -> PyResult<Option<PyCodesHandle>> {
    let _ = headers_only;
    next_grib_handle_from_fileobj(fileobj)
}

#[pyfunction]
#[pyo3(signature = (fileobj, product_kind, headers_only=false))]
fn codes_new_from_file(
    fileobj: &Bound<'_, PyAny>,
    product_kind: i32,
    headers_only: bool,
) -> PyResult<Option<PyCodesHandle>> {
    match product_kind {
        CODES_PRODUCT_GRIB | CODES_PRODUCT_ANY => codes_grib_new_from_file(fileobj, headers_only),
        CODES_PRODUCT_BUFR | CODES_PRODUCT_METAR | CODES_PRODUCT_GTS => Err(
            FunctionNotImplementedError::new_err("ecrust currently supports GRIB messages only"),
        ),
        _ => Err(CodesInternalError::new_err(format!(
            "unsupported product kind {product_kind}"
        ))),
    }
}

#[pyfunction]
#[pyo3(signature = (fileobj, headers_only=false))]
fn codes_any_new_from_file(
    fileobj: &Bound<'_, PyAny>,
    headers_only: bool,
) -> PyResult<Option<PyCodesHandle>> {
    codes_new_from_file(fileobj, CODES_PRODUCT_ANY, headers_only)
}

#[pyfunction]
fn codes_count_in_file(fileobj: &Bound<'_, PyAny>) -> PyResult<usize> {
    let path = file_path_from_obj(fileobj)?;
    let start_pos = file_position(fileobj)?.unwrap_or(0);
    let cached = cached_grib_file(&path).map_err(CodesInternalError::new_err)?;
    seek_file(fileobj, 0)?;
    let next_index = cached.inventory().messages.partition_point(|message| {
        message.offset_bytes.saturating_add(message.length_bytes) <= start_pos
    });
    Ok(cached.inventory().messages.len().saturating_sub(next_index))
}

#[pyfunction]
fn codes_new_from_message(message: &Bound<'_, PyAny>) -> PyResult<PyCodesHandle> {
    handle_from_message_bytes(message.extract::<Vec<u8>>()?)
}

#[pyfunction]
fn codes_get_message(py: Python<'_>, msgid: &PyCodesHandle) -> PyResult<PyObject> {
    let raw_message = msgid.inner.raw_message().map_err(PyErr::from)?;
    Ok(PyBytes::new(py, raw_message).into_any().unbind())
}

#[pyfunction]
fn codes_get_string_length(msgid: &PyCodesHandle, key: &str) -> PyResult<usize> {
    let value = msgid.inner.get_string(key).map_err(PyErr::from)?;
    Ok(string_buffer_length(&value))
}

#[pyfunction]
#[pyo3(signature = (msgid, key, ktype=None))]
fn codes_get(
    py: Python<'_>,
    msgid: Bound<'_, PyCodesHandle>,
    key: &str,
    ktype: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    let requested = requested_type(ktype)?;
    if key == "values" && matches!(requested, RequestedType::Native | RequestedType::Double) {
        return Ok(borrowed_values_array(msgid)?.into_any().unbind());
    }
    let value = msgid.borrow().inner.get(key).map_err(PyErr::from)?;
    value_to_pyobject(py, value, requested)
}

#[pyfunction]
fn codes_get_long(msgid: &PyCodesHandle, key: &str) -> PyResult<i64> {
    msgid.inner.get_long(key).map_err(PyErr::from)
}

#[pyfunction]
fn codes_get_double(msgid: &PyCodesHandle, key: &str) -> PyResult<f64> {
    msgid.inner.get_double(key).map_err(PyErr::from)
}

#[pyfunction]
fn codes_get_string(msgid: &PyCodesHandle, key: &str) -> PyResult<String> {
    msgid.inner.get_string(key).map_err(PyErr::from)
}

#[pyfunction]
fn codes_get_size(msgid: &PyCodesHandle, key: &str) -> PyResult<usize> {
    msgid.inner.get_size(key).map_err(PyErr::from)
}

#[pyfunction]
fn codes_is_defined(msgid: &PyCodesHandle, key: &str) -> bool {
    msgid.inner.contains_key(key)
}

#[pyfunction]
fn codes_is_missing(msgid: &PyCodesHandle, key: &str) -> bool {
    msgid.inner.is_missing(key)
}

#[pyfunction]
fn codes_get_native_type(py: Python<'_>, msgid: &PyCodesHandle, key: &str) -> PyResult<PyObject> {
    let value = msgid.inner.get(key).map_err(PyErr::from)?;
    let ty = match value {
        CodesValue::Long(_) => py.get_type::<PyInt>().into_any().unbind(),
        CodesValue::Double(_) | CodesValue::DoubleArray(_) => {
            py.get_type::<PyFloat>().into_any().unbind()
        }
        CodesValue::String(_) => py.get_type::<PyString>().into_any().unbind(),
        CodesValue::Missing => py.None(),
    };
    Ok(ty)
}

#[pyfunction]
#[pyo3(signature = (msgid, key, ktype=None))]
fn codes_get_array(
    py: Python<'_>,
    msgid: Bound<'_, PyCodesHandle>,
    key: &str,
    ktype: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    let requested = requested_type(ktype)?;
    if key == "values" {
        if matches!(requested, RequestedType::Native | RequestedType::Double) {
            return Ok(borrowed_values_array(msgid)?.into_any().unbind());
        }
        let handle = msgid.borrow();
        return slice_to_pyobject(py, handle.inner.values().map_err(PyErr::from)?, requested);
    }
    match msgid.borrow().inner.get(key).map_err(PyErr::from)? {
        CodesValue::DoubleArray(values) => array_to_pyobject(py, values, requested),
        other => Err(InvalidTypeError::new_err(format!(
            "key '{key}' is not an array; got {}",
            match other {
                CodesValue::Long(_) => "long",
                CodesValue::Double(_) => "double",
                CodesValue::String(_) => "string",
                CodesValue::DoubleArray(_) => "double_array",
                CodesValue::Missing => "missing",
            }
        ))),
    }
}

#[pyfunction]
fn codes_get_double_array(
    py: Python<'_>,
    msgid: Bound<'_, PyCodesHandle>,
    key: &str,
) -> PyResult<PyObject> {
    if key == "values" {
        return Ok(borrowed_values_array(msgid)?.into_any().unbind());
    }
    array_to_pyobject(
        py,
        msgid
            .borrow()
            .inner
            .get_double_array(key)
            .map_err(PyErr::from)?,
        RequestedType::Double,
    )
}

#[pyfunction]
fn codes_get_float_array(
    py: Python<'_>,
    msgid: Bound<'_, PyCodesHandle>,
    key: &str,
) -> PyResult<PyObject> {
    if key == "values" {
        let handle = msgid.borrow();
        return slice_to_pyobject(
            py,
            handle.inner.values().map_err(PyErr::from)?,
            RequestedType::Float32,
        );
    }
    array_to_pyobject(
        py,
        msgid
            .borrow()
            .inner
            .get_double_array(key)
            .map_err(PyErr::from)?,
        RequestedType::Float32,
    )
}

#[pyfunction]
fn codes_get_long_array(
    py: Python<'_>,
    msgid: Bound<'_, PyCodesHandle>,
    key: &str,
) -> PyResult<PyObject> {
    if key == "values" {
        let handle = msgid.borrow();
        return slice_to_pyobject(
            py,
            handle.inner.values().map_err(PyErr::from)?,
            RequestedType::Long,
        );
    }
    array_to_pyobject(
        py,
        msgid
            .borrow()
            .inner
            .get_double_array(key)
            .map_err(PyErr::from)?,
        RequestedType::Long,
    )
}

#[pyfunction]
fn codes_get_string_array(py: Python<'_>, msgid: &PyCodesHandle, key: &str) -> PyResult<PyObject> {
    let strings = match msgid.inner.get(key).map_err(PyErr::from)? {
        CodesValue::String(value) => vec![value],
        CodesValue::Long(value) => vec![value.to_string()],
        CodesValue::Double(value) => vec![value.to_string()],
        CodesValue::DoubleArray(values) => {
            values.into_iter().map(|value| value.to_string()).collect()
        }
        CodesValue::Missing => vec![String::new()],
    };
    tuple_from_strings(py, strings)
}

#[pyfunction]
fn codes_get_double_element(msgid: &PyCodesHandle, key: &str, index: usize) -> PyResult<f64> {
    if key == "values" {
        let values = msgid.inner.values().map_err(PyErr::from)?;
        return values.get(index).copied().ok_or_else(|| {
            CodesInternalError::new_err(format!("index {index} is out of bounds for key '{key}'"))
        });
    }
    let values = msgid.inner.get_double_array(key).map_err(PyErr::from)?;
    values.get(index).copied().ok_or_else(|| {
        CodesInternalError::new_err(format!("index {index} is out of bounds for key '{key}'"))
    })
}

#[pyfunction]
fn codes_get_double_elements(
    msgid: &PyCodesHandle,
    key: &str,
    indexes: Vec<usize>,
) -> PyResult<Vec<f64>> {
    let owned_values;
    let values = if key == "values" {
        msgid.inner.values().map_err(PyErr::from)?
    } else {
        owned_values = msgid.inner.get_double_array(key).map_err(PyErr::from)?;
        owned_values.as_slice()
    };
    indexes
        .into_iter()
        .map(|index| {
            values.get(index).copied().ok_or_else(|| {
                CodesInternalError::new_err(format!(
                    "index {index} is out of bounds for key '{key}'"
                ))
            })
        })
        .collect()
}

#[pyfunction]
fn codes_get_elements(msgid: &PyCodesHandle, key: &str, indexes: Vec<usize>) -> PyResult<Vec<f64>> {
    codes_get_double_elements(msgid, key, indexes)
}

#[pyfunction]
#[pyo3(signature = (gribid, ktype=None))]
fn codes_get_values(
    py: Python<'_>,
    gribid: Bound<'_, PyCodesHandle>,
    ktype: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    let requested = match requested_type(ktype)? {
        RequestedType::Native => RequestedType::Double,
        other => other,
    };
    if matches!(requested, RequestedType::Double) {
        return Ok(borrowed_values_array(gribid)?.into_any().unbind());
    }
    let handle = gribid.borrow();
    slice_to_pyobject(py, handle.inner.values().map_err(PyErr::from)?, requested)
}

#[pyfunction]
fn _benchmark_force_decode_values(msgid: &PyCodesHandle) -> PyResult<usize> {
    let values = msgid.inner.values().map_err(PyErr::from)?;
    Ok(values.len())
}

#[pyfunction]
fn _benchmark_decode_values_uncached(msgid: &PyCodesHandle) -> PyResult<usize> {
    msgid.inner.decode_value_count_uncached().map_err(PyErr::from)
}

#[pyfunction]
fn codes_get_message_offset(msgid: &PyCodesHandle) -> PyResult<i64> {
    msgid.inner.get_long("offset").map_err(PyErr::from)
}

#[pyfunction]
fn codes_get_offset(msgid: &PyCodesHandle) -> PyResult<i64> {
    codes_get_message_offset(msgid)
}

#[pyfunction]
fn codes_get_message_size(msgid: &PyCodesHandle) -> PyResult<i64> {
    msgid.inner.get_long("totalLength").map_err(PyErr::from)
}

#[pyfunction]
fn codes_clone(msgid: &PyCodesHandle) -> PyCodesHandle {
    msgid.clone()
}

#[pyfunction]
fn codes_release(_msgid: &PyCodesHandle) {}

#[pyfunction]
#[pyo3(signature = (msgid, namespace=None))]
fn codes_keys_iterator_new(msgid: &PyCodesHandle, namespace: Option<&str>) -> PyCodesKeysIterator {
    let _ = namespace;
    PyCodesKeysIterator {
        keys: msgid.inner.keys(),
        cursor: 0,
        current: None,
    }
}

#[pyfunction]
fn codes_keys_iterator_next(iterid: &mut PyCodesKeysIterator) -> bool {
    if iterid.cursor < iterid.keys.len() {
        iterid.current = Some(iterid.cursor);
        iterid.cursor += 1;
        true
    } else {
        iterid.current = None;
        false
    }
}

#[pyfunction]
fn codes_keys_iterator_get_name(iterid: &PyCodesKeysIterator) -> PyResult<String> {
    Ok(current_iterator_name(iterid)?.to_string())
}

#[pyfunction]
fn codes_keys_iterator_rewind(iterid: &mut PyCodesKeysIterator) {
    iterid.cursor = 0;
    iterid.current = None;
}

#[pyfunction]
fn codes_keys_iterator_delete(_iterid: &PyCodesKeysIterator) {}

#[pyfunction]
fn codes_skip_duplicates(_iterid: &mut PyCodesKeysIterator) {}

#[pyfunction]
fn codes_skip_coded(_iterid: &mut PyCodesKeysIterator) {}

#[pyfunction]
fn codes_skip_computed(iterid: &mut PyCodesKeysIterator) {
    let computed = [
        "values",
        "latitudes",
        "longitudes",
        "distinctLatitudes",
        "distinctLongitudes",
        "cfName",
        "cfVarName",
        "paramId",
        "gridDefinitionDescription",
        "centreDescription",
        "validityDate",
        "validityTime",
    ];
    iterid.keys.retain(|key| !computed.contains(&key.as_str()));
    iterid.cursor = 0;
    iterid.current = None;
}

#[pyfunction]
fn codes_skip_edition_specific(_iterid: &mut PyCodesKeysIterator) {}

#[pyfunction]
fn codes_skip_function(_iterid: &mut PyCodesKeysIterator) {}

#[pyfunction]
fn codes_skip_read_only(_iterid: &mut PyCodesKeysIterator) {}

#[pyfunction]
fn codes_grib_iterator_new(gribid: &PyCodesHandle, mode: i32) -> PyResult<PyCodesGribIterator> {
    let _ = mode;
    Ok(PyCodesGribIterator {
        points: message_points(gribid)?,
        cursor: 0,
    })
}

#[pyfunction]
fn codes_grib_iterator_next(
    py: Python<'_>,
    iterid: &mut PyCodesGribIterator,
) -> PyResult<PyObject> {
    if let Some((lat, lon, value)) = iterid.points.get(iterid.cursor).copied() {
        iterid.cursor += 1;
        Ok((lat, lon, value).into_pyobject(py)?.into_any().unbind())
    } else {
        Ok(PyList::empty(py).into_any().unbind())
    }
}

#[pyfunction]
fn codes_grib_iterator_delete(_iterid: &PyCodesGribIterator) {}

#[pyfunction]
fn codes_grib_get_data(py: Python<'_>, gribid: &PyCodesHandle) -> PyResult<PyObject> {
    let objects = message_points(gribid)?
        .into_iter()
        .map(|(lat, lon, value)| {
            let dict = PyDict::new(py);
            dict.set_item("lat", lat)?;
            dict.set_item("lon", lon)?;
            dict.set_item("value", value)?;
            Ok(dict.into_any().unbind())
        })
        .collect::<PyResult<Vec<_>>>()?;
    tuple_from_objects(py, objects)
}

#[pyfunction]
fn codes_grib_nearest_new(_msgid: &PyCodesHandle) -> PyCodesNearest {
    PyCodesNearest
}

#[pyfunction]
#[pyo3(signature = (nid, gribid, inlat, inlon, flags, is_lsm=false, npoints=4))]
fn codes_grib_nearest_find(
    py: Python<'_>,
    nid: &PyCodesNearest,
    gribid: &PyCodesHandle,
    inlat: f64,
    inlon: f64,
    flags: i32,
    is_lsm: bool,
    npoints: usize,
) -> PyResult<PyObject> {
    let _ = nid;
    let _ = flags;
    codes_grib_find_nearest(py, gribid, inlat, inlon, is_lsm, npoints)
}

#[pyfunction]
fn codes_grib_nearest_delete(_nid: &PyCodesNearest) {}

#[pyfunction]
#[pyo3(signature = (gribid, inlat, inlon, is_lsm=false, npoints=1))]
fn codes_grib_find_nearest(
    py: Python<'_>,
    gribid: &PyCodesHandle,
    inlat: f64,
    inlon: f64,
    is_lsm: bool,
    npoints: usize,
) -> PyResult<PyObject> {
    let _ = is_lsm;
    let limit = npoints.max(1);
    let objects = nearest_matches(gribid, inlat, inlon)?
        .into_iter()
        .take(limit)
        .map(|(index, distance, lat, lon, value)| {
            nearest_dict(py, index, distance, lat, lon, value)
        })
        .collect::<PyResult<Vec<_>>>()?;
    tuple_from_objects(py, objects)
}

#[pyfunction]
fn codes_grib_find_nearest_multiple(
    py: Python<'_>,
    gribid: &PyCodesHandle,
    is_lsm: bool,
    inlats: Vec<f64>,
    inlons: Vec<f64>,
) -> PyResult<PyObject> {
    let _ = is_lsm;
    if inlats.len() != inlons.len() {
        return Err(InvalidNearestError::new_err(
            "inlats and inlons must have the same length",
        ));
    }
    let objects = inlats
        .into_iter()
        .zip(inlons)
        .map(|(lat, lon)| {
            nearest_matches(gribid, lat, lon).and_then(|matches| {
                let (index, distance, match_lat, match_lon, value) = matches
                    .into_iter()
                    .next()
                    .ok_or_else(|| InvalidNearestError::new_err("no grid points are available"))?;
                nearest_dict(py, index, distance, match_lat, match_lon, value)
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    tuple_from_objects(py, objects)
}

#[pyfunction]
#[pyo3(signature = (filepath, product_kind, is_strict=true))]
fn codes_extract_offsets(filepath: &str, product_kind: i32, is_strict: bool) -> PyResult<Vec<u64>> {
    match product_kind {
        CODES_PRODUCT_GRIB | CODES_PRODUCT_ANY => {
            let cached =
                cached_grib_file(Path::new(filepath)).map_err(CodesInternalError::new_err)?;
            Ok(cached
                .inventory()
                .messages
                .iter()
                .map(|message| message.offset_bytes)
                .collect())
        }
        _ if is_strict => Err(FunctionNotImplementedError::new_err(
            "offset extraction is currently implemented for GRIB messages only",
        )),
        _ => Ok(Vec::new()),
    }
}

#[pyfunction]
#[pyo3(signature = (filepath, product_kind, is_strict=true))]
fn codes_extract_offsets_sizes(
    filepath: &str,
    product_kind: i32,
    is_strict: bool,
) -> PyResult<Vec<(u64, u64)>> {
    match product_kind {
        CODES_PRODUCT_GRIB | CODES_PRODUCT_ANY => {
            let cached =
                cached_grib_file(Path::new(filepath)).map_err(CodesInternalError::new_err)?;
            Ok(cached
                .inventory()
                .messages
                .iter()
                .map(|message| (message.offset_bytes, message.length_bytes))
                .collect())
        }
        _ if is_strict => Err(FunctionNotImplementedError::new_err(
            "offset extraction is currently implemented for GRIB messages only",
        )),
        _ => Ok(Vec::new()),
    }
}

#[pyfunction]
fn codes_write(py: Python<'_>, msgid: &PyCodesHandle, fileobj: &Bound<'_, PyAny>) -> PyResult<()> {
    let raw_message = msgid.inner.raw_message().map_err(PyErr::from)?;
    fileobj.call_method1("write", (PyBytes::new(py, raw_message),))?;
    Ok(())
}

#[pyfunction]
fn codes_index_new_from_file(filename: &str, keys: Vec<String>) -> PyResult<PyCodesIndex> {
    let entries = load_grib_handles_from_path(Path::new(filename))?;
    Ok(PyCodesIndex {
        entries,
        key_specs: keys.into_iter().map(|key| parse_index_key(&key)).collect(),
        selections: BTreeMap::new(),
        cursor: 0,
    })
}

#[pyfunction]
fn codes_index_add_file(indexid: &mut PyCodesIndex, filename: &str) -> PyResult<()> {
    let mut entries = load_grib_handles_from_path(Path::new(filename))?;
    indexid.entries.append(&mut entries);
    reset_index_cursor(indexid);
    Ok(())
}

#[pyfunction]
fn codes_grib_cache_clear() -> PyResult<()> {
    clear_grib_file_cache().map_err(CodesInternalError::new_err)
}

#[pyfunction]
fn codes_grib_cache_set_enabled(enabled: bool) -> PyResult<()> {
    set_grib_file_cache_enabled(enabled).map_err(CodesInternalError::new_err)
}

#[pyfunction]
fn codes_grib_cache_set_limits(max_entries: usize, max_bytes: usize) -> PyResult<()> {
    set_grib_file_cache_limits(max_entries, max_bytes).map_err(CodesInternalError::new_err)
}

#[pyfunction]
fn codes_grib_cache_info(py: Python<'_>) -> PyResult<PyObject> {
    let info = grib_file_cache_info().map_err(CodesInternalError::new_err)?;
    let dict = PyDict::new(py);
    dict.set_item("enabled", info.enabled)?;
    dict.set_item("entries", info.entries)?;
    dict.set_item("total_bytes", info.total_bytes)?;
    dict.set_item("max_entries", info.max_entries)?;
    dict.set_item("max_bytes", info.max_bytes)?;
    Ok(dict.into_any().unbind())
}

#[pyfunction]
fn codes_index_select(
    indexid: &mut PyCodesIndex,
    key: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let selection = selection_from_py(indexid, key, value)?;
    indexid.selections.insert(key.to_string(), selection);
    reset_index_cursor(indexid);
    Ok(())
}

#[pyfunction]
fn codes_index_select_long(indexid: &mut PyCodesIndex, key: &str, value: i64) {
    indexid
        .selections
        .insert(key.to_string(), IndexSelectionValue::Long(value));
    reset_index_cursor(indexid);
}

#[pyfunction]
fn codes_index_select_double(indexid: &mut PyCodesIndex, key: &str, value: f64) {
    indexid
        .selections
        .insert(key.to_string(), IndexSelectionValue::Double(value));
    reset_index_cursor(indexid);
}

#[pyfunction]
fn codes_index_select_string(indexid: &mut PyCodesIndex, key: &str, value: &str) {
    indexid.selections.insert(
        key.to_string(),
        IndexSelectionValue::String(value.to_string()),
    );
    reset_index_cursor(indexid);
}

#[pyfunction]
#[pyo3(signature = (indexid, key, ktype=None))]
fn codes_index_get(
    py: Python<'_>,
    indexid: &PyCodesIndex,
    key: &str,
    ktype: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    match requested_type(ktype)? {
        RequestedType::Long => tuple_from_longs(py, index_long_values(indexid, key)),
        RequestedType::Double | RequestedType::Float32 => {
            tuple_from_doubles(py, index_double_values(indexid, key))
        }
        _ => tuple_from_strings(py, index_string_values(indexid, key)),
    }
}

#[pyfunction]
fn codes_index_get_long(py: Python<'_>, indexid: &PyCodesIndex, key: &str) -> PyResult<PyObject> {
    tuple_from_longs(py, index_long_values(indexid, key))
}

#[pyfunction]
fn codes_index_get_double(py: Python<'_>, indexid: &PyCodesIndex, key: &str) -> PyResult<PyObject> {
    tuple_from_doubles(py, index_double_values(indexid, key))
}

#[pyfunction]
fn codes_index_get_string(py: Python<'_>, indexid: &PyCodesIndex, key: &str) -> PyResult<PyObject> {
    tuple_from_strings(py, index_string_values(indexid, key))
}

#[pyfunction]
fn codes_index_get_size(indexid: &PyCodesIndex, key: &str) -> usize {
    index_string_values(indexid, key).len()
}

#[pyfunction]
fn codes_new_from_index(indexid: &mut PyCodesIndex) -> Option<PyCodesHandle> {
    if !all_index_keys_selected(indexid) {
        return None;
    }
    let next = indexid
        .entries
        .iter()
        .enumerate()
        .skip(indexid.cursor)
        .find(|(_, handle)| {
            indexid
                .selections
                .iter()
                .all(|(key, selection)| index_entry_matches(handle, selection, key))
        })
        .map(|(idx, handle)| {
            indexid.cursor = idx + 1;
            handle.clone()
        });
    if next.is_none() {
        indexid.cursor = indexid.entries.len();
    }
    next
}

#[pyfunction]
fn codes_index_release(_indexid: &PyCodesIndex) {}

#[pyfunction]
fn codes_get_library_path(py: Python<'_>) -> PyResult<String> {
    py.import("ecrust._ecrust")?
        .getattr("__file__")?
        .extract::<String>()
        .map_err(Into::into)
}

#[pyfunction]
fn codes_definition_path() -> PyResult<String> {
    lock_string(definitions_path_cell())
}

#[pyfunction]
fn codes_samples_path() -> PyResult<String> {
    lock_string(samples_path_cell())
}

#[pyfunction]
fn codes_set_definitions_path(defs_path: &str) -> PyResult<()> {
    set_locked_string(definitions_path_cell(), defs_path)
}

#[pyfunction]
fn codes_set_samples_path(samples_path: &str) -> PyResult<()> {
    set_locked_string(samples_path_cell(), samples_path)
}

#[pyfunction]
fn codes_set_debug(dmode: i32) {
    let _ = dmode;
}

#[pyfunction]
#[pyo3(signature = (select=FEATURE_SELECT_ALL))]
fn codes_get_features(select: i32) -> PyResult<String> {
    match select {
        FEATURE_SELECT_ALL => Ok(format!("{ENABLED_FEATURES} {DISABLED_FEATURES}")),
        FEATURE_SELECT_ENABLED => Ok(ENABLED_FEATURES.to_string()),
        FEATURE_SELECT_DISABLED => Ok(DISABLED_FEATURES.to_string()),
        other => Err(CodesInternalError::new_err(format!(
            "unsupported feature selector {other}"
        ))),
    }
}

#[pyfunction]
fn codes_get_api_version() -> &'static str {
    VERSION
}

#[pyfunction]
fn codes_get_version_info(py: Python<'_>) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("eccodes", VERSION)?;
    dict.set_item("bindings", VERSION)?;
    dict.set_item("allocator", allocator_name())?;
    dict.set_item("target_os", std::env::consts::OS)?;
    dict.set_item("target_arch", std::env::consts::ARCH)?;
    Ok(dict.into_any().unbind())
}

#[pymodule]
fn _ecrust(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add("bindings_version", VERSION)?;
    m.add("CODES_PRODUCT_ANY", CODES_PRODUCT_ANY)?;
    m.add("CODES_PRODUCT_GRIB", CODES_PRODUCT_GRIB)?;
    m.add("CODES_PRODUCT_BUFR", CODES_PRODUCT_BUFR)?;
    m.add("CODES_PRODUCT_METAR", CODES_PRODUCT_METAR)?;
    m.add("CODES_PRODUCT_GTS", CODES_PRODUCT_GTS)?;
    m.add("CODES_MISSING_LONG", CODES_MISSING_LONG)?;
    m.add("CODES_MISSING_DOUBLE", CODES_MISSING_DOUBLE)?;
    m.add("CODES_FEATURES_ALL", FEATURE_SELECT_ALL)?;
    m.add("CODES_FEATURES_ENABLED", FEATURE_SELECT_ENABLED)?;
    m.add("CODES_FEATURES_DISABLED", FEATURE_SELECT_DISABLED)?;
    m.add("__allocator__", allocator_name())?;
    m.add("CodesInternalError", py.get_type::<CodesInternalError>())?;
    m.add(
        "KeyValueNotFoundError",
        py.get_type::<KeyValueNotFoundError>(),
    )?;
    m.add("InvalidTypeError", py.get_type::<InvalidTypeError>())?;
    m.add("EndOfFileError", py.get_type::<EndOfFileError>())?;
    m.add(
        "FunctionNotImplementedError",
        py.get_type::<FunctionNotImplementedError>(),
    )?;
    m.add(
        "InvalidKeysIteratorError",
        py.get_type::<InvalidKeysIteratorError>(),
    )?;
    m.add(
        "InvalidGribIteratorError",
        py.get_type::<InvalidGribIteratorError>(),
    )?;
    m.add("InvalidIndexError", py.get_type::<InvalidIndexError>())?;
    m.add("InvalidNearestError", py.get_type::<InvalidNearestError>())?;
    m.add_class::<PyCodesHandle>()?;
    m.add_class::<PyCodesKeysIterator>()?;
    m.add_class::<PyCodesGribIterator>()?;
    m.add_class::<PyCodesNearest>()?;
    m.add_class::<PyCodesIndex>()?;
    m.add_function(wrap_pyfunction!(codes_grib_new_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(codes_new_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(codes_any_new_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(codes_count_in_file, m)?)?;
    m.add_function(wrap_pyfunction!(codes_new_from_message, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_message, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_string_length, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_long, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_double, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_string, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_size, m)?)?;
    m.add_function(wrap_pyfunction!(codes_is_defined, m)?)?;
    m.add_function(wrap_pyfunction!(codes_is_missing, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_native_type, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_array, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_double_array, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_float_array, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_long_array, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_string_array, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_double_element, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_double_elements, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_elements, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_values, m)?)?;
    m.add_function(wrap_pyfunction!(_benchmark_force_decode_values, m)?)?;
    m.add_function(wrap_pyfunction!(_benchmark_decode_values_uncached, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_message_offset, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_message_size, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_offset, m)?)?;
    m.add_function(wrap_pyfunction!(codes_clone, m)?)?;
    m.add_function(wrap_pyfunction!(codes_release, m)?)?;
    m.add_function(wrap_pyfunction!(codes_keys_iterator_new, m)?)?;
    m.add_function(wrap_pyfunction!(codes_keys_iterator_next, m)?)?;
    m.add_function(wrap_pyfunction!(codes_keys_iterator_get_name, m)?)?;
    m.add_function(wrap_pyfunction!(codes_keys_iterator_rewind, m)?)?;
    m.add_function(wrap_pyfunction!(codes_keys_iterator_delete, m)?)?;
    m.add_function(wrap_pyfunction!(codes_skip_duplicates, m)?)?;
    m.add_function(wrap_pyfunction!(codes_skip_coded, m)?)?;
    m.add_function(wrap_pyfunction!(codes_skip_computed, m)?)?;
    m.add_function(wrap_pyfunction!(codes_skip_edition_specific, m)?)?;
    m.add_function(wrap_pyfunction!(codes_skip_function, m)?)?;
    m.add_function(wrap_pyfunction!(codes_skip_read_only, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_iterator_new, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_iterator_next, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_iterator_delete, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_get_data, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_nearest_new, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_nearest_find, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_nearest_delete, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_find_nearest, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_find_nearest_multiple, m)?)?;
    m.add_function(wrap_pyfunction!(codes_extract_offsets, m)?)?;
    m.add_function(wrap_pyfunction!(codes_extract_offsets_sizes, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_cache_clear, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_cache_set_enabled, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_cache_set_limits, m)?)?;
    m.add_function(wrap_pyfunction!(codes_grib_cache_info, m)?)?;
    m.add_function(wrap_pyfunction!(codes_write, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_new_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_add_file, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_select, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_select_long, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_select_double, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_select_string, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_get, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_get_long, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_get_double, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_get_string, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_get_size, m)?)?;
    m.add_function(wrap_pyfunction!(codes_new_from_index, m)?)?;
    m.add_function(wrap_pyfunction!(codes_index_release, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_library_path, m)?)?;
    m.add_function(wrap_pyfunction!(codes_definition_path, m)?)?;
    m.add_function(wrap_pyfunction!(codes_samples_path, m)?)?;
    m.add_function(wrap_pyfunction!(codes_set_definitions_path, m)?)?;
    m.add_function(wrap_pyfunction!(codes_set_samples_path, m)?)?;
    m.add_function(wrap_pyfunction!(codes_set_debug, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_features, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_api_version, m)?)?;
    m.add_function(wrap_pyfunction!(codes_get_version_info, m)?)?;
    Ok(())
}
