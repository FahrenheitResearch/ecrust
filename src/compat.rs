//! Modern GRIB-focused ecCodes compatibility surface.
//!
//! This crate intentionally targets the non-legacy GRIB workflows that the
//! workspace already supports today. It layers ecCodes-style handles and typed
//! key accessors over `wx-grib` without introducing a C dependency.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::grib::{CoordinateAxis, DecodedField, GribEdition, GribEngine, MessageDescriptor};
use crate::grib2_compat::lookup_grib2_compat_entry;
use crate::grib_cache::{cached_grib_file, CachedGribFile};
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};

pub type Result<T> = std::result::Result<T, CodesError>;

#[derive(Debug, Clone, PartialEq)]
pub enum CodesValue {
    Long(i64),
    Double(f64),
    String(String),
    DoubleArray(Vec<f64>),
    Missing,
}

impl CodesValue {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::Long(_) => "long",
            Self::Double(_) => "double",
            Self::String(_) => "string",
            Self::DoubleArray(_) => "double_array",
            Self::Missing => "missing",
        }
    }

    fn to_long(&self) -> Option<i64> {
        match self {
            Self::Long(value) => Some(*value),
            Self::Double(value) => Some(*value as i64),
            Self::String(value) => value.parse::<i64>().ok(),
            _ => None,
        }
    }

    fn to_double(&self) -> Option<f64> {
        match self {
            Self::Double(value) => Some(*value),
            Self::Long(value) => Some(*value as f64),
            Self::String(value) => value.parse::<f64>().ok(),
            _ => None,
        }
    }

    fn to_string_value(&self) -> Option<String> {
        match self {
            Self::String(value) => Some(value.clone()),
            Self::Long(value) => Some(value.to_string()),
            Self::Double(value) => Some(value.to_string()),
            Self::Missing => Some(String::new()),
            Self::DoubleArray(_) => None,
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Long(_) | Self::Double(_) | Self::Missing => 1,
            Self::String(value) => value.len(),
            Self::DoubleArray(values) => values.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodesError {
    Engine(String),
    MissingKey(String),
    TypeMismatch {
        key: String,
        expected: &'static str,
        actual: &'static str,
    },
    NoMessages(String),
}

impl fmt::Display for CodesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(message) => write!(f, "{message}"),
            Self::MissingKey(key) => write!(f, "key '{key}' is not defined"),
            Self::TypeMismatch {
                key,
                expected,
                actual,
            } => write!(
                f,
                "key '{key}' cannot be read as {expected}; actual type is {actual}"
            ),
            Self::NoMessages(message) => write!(f, "{message}"),
        }
    }
}

impl Error for CodesError {}

impl From<String> for CodesError {
    fn from(value: String) -> Self {
        Self::Engine(value)
    }
}

#[derive(Debug, Clone)]
enum MessageSource {
    CachedFile(Arc<CachedGribFile>),
    Owned(Arc<[u8]>),
    Empty,
}

impl MessageSource {
    fn bytes<'a>(
        &'a self,
        descriptor: &MessageDescriptor,
    ) -> std::result::Result<&'a [u8], String> {
        match self {
            Self::CachedFile(file) => file.message_bytes(descriptor),
            Self::Owned(bytes) => Ok(bytes.as_ref()),
            Self::Empty => Err("raw message bytes are unavailable".to_string()),
        }
    }
}

#[derive(Debug, Clone)]
struct DecodedHandleData {
    values: Vec<f64>,
    missing_count: usize,
    distinct_longitudes: Option<Vec<f64>>,
    distinct_latitudes: Option<Vec<f64>>,
}

impl DecodedHandleData {
    fn from_field(field: DecodedField) -> Self {
        let DecodedField {
            grid,
            x_axis,
            y_axis,
            missing_count,
            ..
        } = field;
        Self {
            values: grid.values,
            missing_count,
            distinct_longitudes: axis_values(&x_axis, "longitude"),
            distinct_latitudes: axis_values(&y_axis, "latitude"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CodesHandle {
    message_no: u64,
    descriptor: MessageDescriptor,
    value_count: Option<usize>,
    keys: BTreeMap<String, CodesValue>,
    source: MessageSource,
    decoded: Arc<OnceLock<std::result::Result<DecodedHandleData, String>>>,
}

impl CodesHandle {
    pub fn from_cached_message(
        cached: Arc<CachedGribFile>,
        descriptor: MessageDescriptor,
    ) -> Result<Self> {
        Self::from_message_source(MessageSource::CachedFile(cached), descriptor)
    }

    pub fn from_file(path: impl AsRef<Path>, message_no: u64) -> Result<Self> {
        let cached = cached_grib_file(path.as_ref()).map_err(CodesError::Engine)?;
        let descriptor = cached
            .message_by_number(message_no)
            .cloned()
            .ok_or_else(|| CodesError::Engine(format!("message {message_no} not found")))?;
        Self::from_message_source(MessageSource::CachedFile(cached), descriptor)
    }

    pub fn from_bytes(data: &[u8], message_no: u64) -> Result<Self> {
        let engine = GribEngine::new();
        let inventory = engine.scan_bytes(data).map_err(CodesError::Engine)?;
        let descriptor = inventory
            .messages
            .iter()
            .find(|message| message.message_no == message_no)
            .cloned()
            .ok_or_else(|| CodesError::Engine(format!("message {message_no} not found")))?;
        let start = descriptor.offset_bytes as usize;
        let end = start + descriptor.length_bytes as usize;
        let raw_message = data.get(start..end).ok_or_else(|| {
            CodesError::Engine(format!("message {message_no} bytes are out of range"))
        })?;
        Self::from_message_source(
            MessageSource::Owned(Arc::<[u8]>::from(raw_message.to_vec())),
            descriptor,
        )
    }

    pub fn from_decoded_field(field: DecodedField) -> Result<Self> {
        let descriptor = field.descriptor.clone();
        let value_count = Some(field.grid.values.len());
        let decoded = Arc::new(OnceLock::new());
        let _ = decoded.set(Ok(DecodedHandleData::from_field(field)));
        Ok(Self {
            message_no: descriptor.message_no,
            descriptor: descriptor.clone(),
            value_count,
            keys: build_static_keys(&descriptor, value_count),
            source: MessageSource::Empty,
            decoded,
        })
    }

    pub fn message_no(&self) -> u64 {
        self.message_no
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.keys.contains_key(key) || self.has_dynamic_key(key)
    }

    pub fn is_missing(&self, key: &str) -> bool {
        matches!(self.get(key), Ok(CodesValue::Missing))
    }

    pub fn get(&self, key: &str) -> Result<CodesValue> {
        if let Some(value) = self.dynamic_value(key)? {
            return Ok(value);
        }
        self.keys
            .get(key)
            .cloned()
            .ok_or_else(|| CodesError::MissingKey(key.to_string()))
    }

    pub fn get_long(&self, key: &str) -> Result<i64> {
        let value = self.get(key)?;
        value.to_long().ok_or_else(|| CodesError::TypeMismatch {
            key: key.to_string(),
            expected: "long",
            actual: value.kind_name(),
        })
    }

    pub fn get_double(&self, key: &str) -> Result<f64> {
        let value = self.get(key)?;
        value.to_double().ok_or_else(|| CodesError::TypeMismatch {
            key: key.to_string(),
            expected: "double",
            actual: value.kind_name(),
        })
    }

    pub fn get_string(&self, key: &str) -> Result<String> {
        let value = self.get(key)?;
        value
            .to_string_value()
            .ok_or_else(|| CodesError::TypeMismatch {
                key: key.to_string(),
                expected: "string",
                actual: value.kind_name(),
            })
    }

    pub fn get_double_array(&self, key: &str) -> Result<Vec<f64>> {
        let value = self.get(key)?;
        match value {
            CodesValue::DoubleArray(values) => Ok(values),
            other => Err(CodesError::TypeMismatch {
                key: key.to_string(),
                expected: "double_array",
                actual: other.kind_name(),
            }),
        }
    }

    pub fn get_values(&self) -> Result<Vec<f64>> {
        self.values().map(|values| values.to_vec())
    }

    pub fn values(&self) -> Result<&[f64]> {
        Ok(&self.decoded()?.values)
    }

    pub fn decode_value_count_uncached(&self) -> Result<usize> {
        match self.source.bytes(&self.descriptor) {
            Ok(raw_message) => {
                let field = GribEngine::new()
                    .decode_message(raw_message, self.descriptor.clone())
                    .map_err(CodesError::Engine)?;
                Ok(field.grid.values.len())
            }
            Err(_) => Ok(self.decoded()?.values.len()),
        }
    }

    pub fn raw_message(&self) -> Result<&[u8]> {
        self.source
            .bytes(&self.descriptor)
            .map_err(CodesError::Engine)
    }

    pub fn get_size(&self, key: &str) -> Result<usize> {
        match key {
            "values" => self
                .value_count
                .or_else(|| {
                    self.keys
                        .get("numberOfValues")
                        .and_then(CodesValue::to_long)
                        .map(|value| value as usize)
                })
                .ok_or_else(|| CodesError::MissingKey(key.to_string())),
            "distinctLatitudes" => self
                .decoded()?
                .distinct_latitudes
                .as_ref()
                .map(Vec::len)
                .ok_or_else(|| CodesError::MissingKey(key.to_string())),
            "distinctLongitudes" => self
                .decoded()?
                .distinct_longitudes
                .as_ref()
                .map(Vec::len)
                .ok_or_else(|| CodesError::MissingKey(key.to_string())),
            "latitudes" | "longitudes" => self
                .decoded()?
                .distinct_latitudes
                .as_ref()
                .zip(self.decoded()?.distinct_longitudes.as_ref())
                .map(|(latitudes, longitudes)| latitudes.len() * longitudes.len())
                .ok_or_else(|| CodesError::MissingKey(key.to_string())),
            _ => Ok(self.get(key)?.size()),
        }
    }

    pub fn keys(&self) -> Vec<String> {
        let mut names: BTreeSet<String> = self.keys.keys().cloned().collect();
        names.insert("values".to_string());
        names.insert("numberOfMissing".to_string());
        if self.supports_axis_keys() {
            names.insert("distinctLatitudes".to_string());
            names.insert("latitudes".to_string());
            names.insert("distinctLongitudes".to_string());
            names.insert("longitudes".to_string());
            names.insert("latitudeOfFirstGridPointInDegrees".to_string());
            names.insert("latitudeOfLastGridPointInDegrees".to_string());
            names.insert("longitudeOfFirstGridPointInDegrees".to_string());
            names.insert("longitudeOfLastGridPointInDegrees".to_string());
            names.insert("iDirectionIncrementInDegrees".to_string());
            names.insert("jDirectionIncrementInDegrees".to_string());
        }
        names.into_iter().collect()
    }

    fn has_dynamic_key(&self, key: &str) -> bool {
        match key {
            "values" | "numberOfMissing" => true,
            "distinctLatitudes"
            | "distinctLongitudes"
            | "latitudes"
            | "longitudes"
            | "latitudeOfFirstGridPointInDegrees"
            | "latitudeOfLastGridPointInDegrees"
            | "longitudeOfFirstGridPointInDegrees"
            | "longitudeOfLastGridPointInDegrees"
            | "iDirectionIncrementInDegrees"
            | "jDirectionIncrementInDegrees" => self.supports_axis_keys(),
            _ => false,
        }
    }

    fn latitudes(&self) -> Option<Vec<f64>> {
        let decoded = self.decoded().ok()?;
        let latitudes = decoded.distinct_latitudes.as_ref()?;
        let longitudes = decoded.distinct_longitudes.as_ref()?;
        let mut values = Vec::with_capacity(latitudes.len() * longitudes.len());
        for latitude in latitudes {
            values.extend(std::iter::repeat_n(*latitude, longitudes.len()));
        }
        Some(values)
    }

    fn longitudes(&self) -> Option<Vec<f64>> {
        let decoded = self.decoded().ok()?;
        let latitudes = decoded.distinct_latitudes.as_ref()?;
        let longitudes = decoded.distinct_longitudes.as_ref()?;
        let mut values = Vec::with_capacity(latitudes.len() * longitudes.len());
        for _ in latitudes {
            values.extend(longitudes.iter().copied());
        }
        Some(values)
    }

    fn supports_axis_keys(&self) -> bool {
        matches!(self.descriptor.grid_template, Some(0 | 40))
    }

    fn decoded(&self) -> Result<&DecodedHandleData> {
        let decoded = self.decoded.get_or_init(|| {
            let raw_message = self.source.bytes(&self.descriptor)?;
            let field = GribEngine::new().decode_message(raw_message, self.descriptor.clone())?;
            Ok(DecodedHandleData::from_field(field))
        });
        decoded
            .as_ref()
            .map_err(|err| CodesError::Engine(err.clone()))
    }

    fn dynamic_value(&self, key: &str) -> Result<Option<CodesValue>> {
        match key {
            "values" => Ok(Some(CodesValue::DoubleArray(self.get_values()?))),
            "numberOfMissing" => Ok(Some(CodesValue::Long(self.decoded()?.missing_count as i64))),
            "distinctLatitudes" => Ok(self
                .decoded()?
                .distinct_latitudes
                .clone()
                .map(CodesValue::DoubleArray)),
            "distinctLongitudes" => Ok(self
                .decoded()?
                .distinct_longitudes
                .clone()
                .map(CodesValue::DoubleArray)),
            "latitudes" => Ok(self.latitudes().map(CodesValue::DoubleArray)),
            "longitudes" => Ok(self.longitudes().map(CodesValue::DoubleArray)),
            "latitudeOfFirstGridPointInDegrees" => Ok(self
                .decoded()?
                .distinct_latitudes
                .as_ref()
                .and_then(|values| values.first().copied())
                .map(CodesValue::Double)),
            "latitudeOfLastGridPointInDegrees" => Ok(self
                .decoded()?
                .distinct_latitudes
                .as_ref()
                .and_then(|values| values.last().copied())
                .map(CodesValue::Double)),
            "longitudeOfFirstGridPointInDegrees" => Ok(self
                .decoded()?
                .distinct_longitudes
                .as_ref()
                .and_then(|values| values.first().copied())
                .map(CodesValue::Double)),
            "longitudeOfLastGridPointInDegrees" => Ok(self
                .decoded()?
                .distinct_longitudes
                .as_ref()
                .and_then(|values| values.last().copied())
                .map(CodesValue::Double)),
            "iDirectionIncrementInDegrees" => Ok(axis_increment(
                self.decoded()?.distinct_longitudes.as_deref(),
            )
            .map(CodesValue::Double)),
            "jDirectionIncrementInDegrees" => Ok(axis_increment(
                self.decoded()?.distinct_latitudes.as_deref(),
            )
            .map(CodesValue::Double)),
            _ => Ok(None),
        }
    }

    fn from_message_source(source: MessageSource, descriptor: MessageDescriptor) -> Result<Self> {
        let value_count = descriptor_value_count(&descriptor);
        Ok(Self {
            message_no: descriptor.message_no,
            descriptor: descriptor.clone(),
            value_count,
            keys: build_static_keys(&descriptor, value_count),
            source,
            decoded: Arc::new(OnceLock::new()),
        })
    }
}

fn descriptor_value_count(descriptor: &MessageDescriptor) -> Option<usize> {
    descriptor.nx.zip(descriptor.ny).and_then(|(nx, ny)| {
        usize::try_from(nx)
            .ok()?
            .checked_mul(usize::try_from(ny).ok()?)
    })
}

fn axis_increment(values: Option<&[f64]>) -> Option<f64> {
    let values = values?;
    if values.len() > 1 {
        Some((values[1] - values[0]).abs())
    } else {
        None
    }
}

fn build_static_keys(
    descriptor: &MessageDescriptor,
    value_count: Option<usize>,
) -> BTreeMap<String, CodesValue> {
    let mut keys = BTreeMap::new();
    let edition_number = match descriptor.edition {
        Some(GribEdition::Grib1) => 1,
        Some(GribEdition::Grib2) => 2,
        None => 0,
    };
    let level_type_name = compat_level_type_name(descriptor);
    let level_value = compat_level_value(descriptor);
    let short_name = compat_short_name(descriptor);
    let cf_var_name = compat_cf_var_name(descriptor, &short_name);
    let units = compat_units(descriptor);
    let grid_type = compat_grid_type(descriptor);
    let grid_description = grid_description(&grid_type).to_string();
    let reference_time = descriptor
        .reference_time
        .as_deref()
        .and_then(parse_reference_time);

    insert_long(&mut keys, "edition", edition_number);
    insert_long(&mut keys, "editionNumber", edition_number);
    insert_long(&mut keys, "offset", descriptor.offset_bytes as i64);
    insert_long(&mut keys, "totalLength", descriptor.length_bytes as i64);
    if let Some(value_count) = value_count {
        insert_long(&mut keys, "numberOfPoints", value_count as i64);
        insert_long(&mut keys, "numberOfValues", value_count as i64);
        insert_long(&mut keys, "numberOfDataPoints", value_count as i64);
    }
    if let Some(nx) = descriptor.nx {
        insert_long(&mut keys, "Nx", nx as i64);
    }
    if let Some(ny) = descriptor.ny {
        insert_long(&mut keys, "Ny", ny as i64);
    }
    insert_string(&mut keys, "shortName", short_name);
    insert_string(&mut keys, "name", compat_name(descriptor));
    insert_string(&mut keys, "units", units);
    insert_string(&mut keys, "typeOfLevel", level_type_name.to_string());
    insert_string(&mut keys, "levelType", level_type_name.to_string());
    insert_double(&mut keys, "level", level_value);
    insert_string(&mut keys, "gridType", grid_type);
    insert_string(&mut keys, "gridDefinitionDescription", grid_description);
    insert_string(
        &mut keys,
        "dataType",
        compat_data_type(descriptor).to_string(),
    );
    insert_string(
        &mut keys,
        "packingType",
        compat_packing_type(descriptor).to_string(),
    );
    insert_string(&mut keys, "stepType", "instant".to_string());
    insert_double(&mut keys, "missingValue", 9999.0);
    insert_long(&mut keys, "messageNumber", descriptor.message_no as i64);

    if let Some(center) = descriptor.center {
        insert_long(&mut keys, "centre", center as i64);
        insert_string(
            &mut keys,
            "centreDescription",
            centre_description(center).to_string(),
        );
    }
    if let Some(subcenter) = descriptor.subcenter {
        insert_long(&mut keys, "subCentre", subcenter as i64);
    }
    if let Some(discipline) = descriptor.discipline {
        insert_long(&mut keys, "discipline", discipline as i64);
    }
    if let Some(category) = descriptor.category {
        insert_long(&mut keys, "parameterCategory", category as i64);
    }
    if let Some(number) = descriptor.parameter_number {
        insert_long(&mut keys, "parameterNumber", number as i64);
    }
    if let Some(template) = descriptor.grid_template {
        insert_long(&mut keys, "gridDefinitionTemplateNumber", template as i64);
    }
    if let Some(template) = descriptor.product_template {
        insert_long(
            &mut keys,
            "productDefinitionTemplateNumber",
            template as i64,
        );
    }
    if let Some(template) = descriptor.data_template {
        insert_long(
            &mut keys,
            "dataRepresentationTemplateNumber",
            template as i64,
        );
    }
    if let Some(bits_per_value) = descriptor.bits_per_value {
        insert_long(&mut keys, "bitsPerValue", bits_per_value as i64);
    }
    if edition_number == 1 {
        if let Some(table_version) = descriptor.table_version {
            insert_long(&mut keys, "table2Version", table_version as i64);
        }
        if let Some(indicator) = descriptor.parameter_number {
            insert_long(&mut keys, "indicatorOfParameter", indicator as i64);
        }
    }
    if let Some(process_id) = descriptor.generating_process_identifier {
        insert_long(&mut keys, "generatingProcessIdentifier", process_id as i64);
    }
    if let Some(param_id) = compat_param_id(descriptor) {
        insert_long(&mut keys, "paramId", param_id as i64);
    }

    let cf_name = compat_cf_name(descriptor);
    if !cf_name.is_empty() {
        insert_string(&mut keys, "cfName", cf_name.to_string());
    }
    if !cf_var_name.is_empty() {
        insert_string(&mut keys, "cfVarName", cf_var_name);
    }

    if let Some(forecast_time) = descriptor.forecast_time_value {
        insert_long(&mut keys, "forecastTime", forecast_time as i64);
        insert_long(&mut keys, "endStep", forecast_time as i64);
        insert_string(&mut keys, "stepRange", forecast_time.to_string());
    }
    if let Some(unit_code) = descriptor
        .forecast_time_unit
        .as_deref()
        .and_then(time_unit_code_from_name)
    {
        insert_long(&mut keys, "stepUnits", unit_code);
    }

    if let Some(scan_mode) = descriptor.scan_mode {
        insert_long(
            &mut keys,
            "iScansNegatively",
            if scan_mode & 0x80 != 0 { 1 } else { 0 },
        );
        insert_long(
            &mut keys,
            "jScansPositively",
            if scan_mode & 0x40 != 0 { 1 } else { 0 },
        );
        insert_long(
            &mut keys,
            "jPointsAreConsecutive",
            if scan_mode & 0x20 != 0 { 1 } else { 0 },
        );
        insert_long(
            &mut keys,
            "alternativeRowScanning",
            if scan_mode & 0x10 != 0 { 1 } else { 0 },
        );
    }

    if let Some(datetime) = reference_time {
        insert_long(&mut keys, "dataDate", compact_date(datetime));
        insert_long(&mut keys, "dataTime", compact_time(datetime));
    }

    if let (Some(datetime), Some(forecast_time), Some(unit_code)) = (
        reference_time,
        descriptor.forecast_time_value,
        descriptor
            .forecast_time_unit
            .as_deref()
            .and_then(time_unit_code_from_name),
    ) {
        let valid =
            datetime + Duration::seconds(step_unit_seconds(unit_code) * forecast_time as i64);
        insert_long(&mut keys, "validityDate", compact_date(valid));
        insert_long(&mut keys, "validityTime", compact_time(valid));
    }

    keys
}

#[derive(Debug, Clone)]
pub struct CodesFile {
    path: PathBuf,
    message_numbers: Vec<u64>,
}

impl CodesFile {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let cached = cached_grib_file(path.as_ref()).map_err(CodesError::Engine)?;
        let inventory = cached.inventory();
        let message_numbers = inventory
            .messages
            .iter()
            .map(|message| message.message_no)
            .collect::<Vec<_>>();
        if message_numbers.is_empty() {
            return Err(CodesError::NoMessages(format!(
                "no GRIB messages found in '{}'",
                path.as_ref().display()
            )));
        }
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            message_numbers,
        })
    }

    pub fn len(&self) -> usize {
        self.message_numbers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.message_numbers.is_empty()
    }

    pub fn message_numbers(&self) -> &[u64] {
        &self.message_numbers
    }

    pub fn first(&self) -> Result<CodesHandle> {
        let first = self.message_numbers.first().copied().ok_or_else(|| {
            CodesError::NoMessages(format!("no messages in '{}'", self.path.display()))
        })?;
        self.message(first)
    }

    pub fn message(&self, message_no: u64) -> Result<CodesHandle> {
        CodesHandle::from_file(&self.path, message_no)
    }
}

#[derive(Debug, Clone)]
pub struct CodesKeysIterator {
    keys: Vec<String>,
    index: usize,
}

impl Iterator for CodesKeysIterator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.keys.get(self.index).cloned();
        if item.is_some() {
            self.index += 1;
        }
        item
    }
}

pub fn codes_grib_new_from_file(path: impl AsRef<Path>) -> Result<CodesHandle> {
    CodesFile::open(path)?.first()
}

pub fn codes_grib_new_from_file_at(path: impl AsRef<Path>, message_no: u64) -> Result<CodesHandle> {
    CodesHandle::from_file(path, message_no)
}

pub fn codes_grib_new_from_bytes(data: &[u8]) -> Result<CodesHandle> {
    CodesHandle::from_bytes(data, 1)
}

pub fn codes_grib_new_from_bytes_at(data: &[u8], message_no: u64) -> Result<CodesHandle> {
    CodesHandle::from_bytes(data, message_no)
}

pub fn codes_count_in_file(path: impl AsRef<Path>) -> Result<usize> {
    Ok(CodesFile::open(path)?.len())
}

pub fn codes_is_defined(handle: &CodesHandle, key: &str) -> bool {
    handle.contains_key(key)
}

pub fn codes_is_missing(handle: &CodesHandle, key: &str) -> bool {
    handle.is_missing(key)
}

pub fn codes_get_size(handle: &CodesHandle, key: &str) -> Result<usize> {
    handle.get_size(key)
}

pub fn codes_get_long(handle: &CodesHandle, key: &str) -> Result<i64> {
    handle.get_long(key)
}

pub fn codes_get_double(handle: &CodesHandle, key: &str) -> Result<f64> {
    handle.get_double(key)
}

pub fn codes_get_string(handle: &CodesHandle, key: &str) -> Result<String> {
    handle.get_string(key)
}

pub fn codes_get_double_array(handle: &CodesHandle, key: &str) -> Result<Vec<f64>> {
    handle.get_double_array(key)
}

pub fn codes_get_values(handle: &CodesHandle) -> Vec<f64> {
    handle.get_values().unwrap_or_default()
}

pub fn codes_keys_iterator_new(handle: &CodesHandle) -> CodesKeysIterator {
    CodesKeysIterator {
        keys: handle.keys(),
        index: 0,
    }
}

pub fn codes_release(_handle: CodesHandle) {}

fn insert_long(map: &mut BTreeMap<String, CodesValue>, key: &str, value: i64) {
    map.insert(key.to_string(), CodesValue::Long(value));
}

fn insert_double(map: &mut BTreeMap<String, CodesValue>, key: &str, value: f64) {
    map.insert(key.to_string(), CodesValue::Double(value));
}

fn insert_string(map: &mut BTreeMap<String, CodesValue>, key: &str, value: String) {
    map.insert(key.to_string(), CodesValue::String(value));
}

fn axis_values(axis: &Option<CoordinateAxis>, expected_name: &str) -> Option<Vec<f64>> {
    axis.as_ref()
        .filter(|axis| axis.name == expected_name)
        .map(|axis| axis.values.clone())
}

fn compat_level_value(descriptor: &MessageDescriptor) -> f64 {
    let value = descriptor.level_value.unwrap_or(0.0);
    if descriptor.edition == Some(GribEdition::Grib2)
        && descriptor.level_type == Some(100)
        && value >= 100.0
    {
        value / 100.0
    } else {
        value
    }
}

fn compat_units(descriptor: &MessageDescriptor) -> String {
    if descriptor.edition == Some(GribEdition::Grib1) {
        if let Some(units) = grib1_compat_units(descriptor) {
            return units.to_string();
        }
    } else {
        let level_value = compat_level_value(descriptor);
        if let Some(entry) = lookup_grib2_compat_entry(descriptor, level_value) {
            return entry.units.to_string();
        }
    }
    descriptor.units.clone().unwrap_or_else(|| "?".to_string())
}

fn compat_param_id(descriptor: &MessageDescriptor) -> Option<u32> {
    if descriptor.edition == Some(GribEdition::Grib1) {
        let parameter = descriptor.parameter_number?;
        let center = descriptor.center.unwrap_or(0);
        let table_version = descriptor.table_version.unwrap_or(0);
        return Some(match (table_version, center, parameter) {
            (172, _, parameter) => 172_000 + parameter as u32,
            (228, _, parameter) => 228_000 + parameter as u32,
            (1, 96, 112) => 3_112,
            _ => parameter as u32,
        });
    }

    let level_type = descriptor.level_type.unwrap_or(255);
    let level_value_f64 = compat_level_value(descriptor);
    let level_value = level_value_f64 as u32;

    if let Some(entry) = lookup_grib2_compat_entry(descriptor, level_value_f64) {
        return Some(entry.param_id);
    }

    if let (Some(discipline), Some(category), Some(number)) = (
        descriptor.discipline,
        descriptor.category,
        descriptor.parameter_number,
    ) {
        return Some(match (discipline, category, number) {
            (0, 0, 0) if level_type == 103 && level_value == 2 => 167,
            (0, 0, 0) => 130,
            (0, 0, 6) if level_type == 103 && level_value == 2 => 168,
            (0, 0, 6) => 3017,
            (0, 1, 1) => 157,
            (0, 1, 0) => 133,
            (0, 1, 3) => 3054,
            (0, 2, 2) if level_type == 103 && level_value == 10 => 165,
            (0, 2, 2) if level_type == 103 && level_value == 100 => 228246,
            (0, 2, 2) => 131,
            (0, 2, 3) if level_type == 103 && level_value == 10 => 166,
            (0, 2, 3) if level_type == 103 && level_value == 100 => 228247,
            (0, 2, 3) => 132,
            (0, 2, 8) => 135,
            (0, 2, 10) => 3041,
            (0, 3, 0) => 134,
            (0, 3, 5) => 156,
            (0, 6, 1) => 228164,
            (0, 6, 3) => 3073,
            (0, 6, 4) => 3074,
            (0, 6, 5) => 3075,
            (0, 7, 6) => 59,
            (0, 7, 7) => 228001,
            (0, 19, 0) => 3020,
            (2, 0, 0) => 172,
            (2, 0, 1) => 244,
            (0, 1, 8) => 228228,
            _ => discipline as u32 * 100_000 + category as u32 * 1_000 + number as u32,
        });
    }
    descriptor.parameter_number.map(u32::from)
}

fn compat_short_name(descriptor: &MessageDescriptor) -> String {
    if descriptor.edition == Some(GribEdition::Grib1) {
        if let Some(name) = grib1_compat_short_name(descriptor) {
            return name.to_string();
        }
    } else {
        let level_value = compat_level_value(descriptor);
        if let Some(entry) = lookup_grib2_compat_entry(descriptor, level_value) {
            return entry.short_name.to_string();
        }
    }

    let base = match (
        descriptor.discipline,
        descriptor.category,
        descriptor.parameter_number,
    ) {
        (Some(discipline), Some(category), Some(number)) => {
            let mapped = eccodes_short_name(discipline, category, number);
            if mapped.is_empty() {
                descriptor.variable.as_str()
            } else {
                mapped
            }
        }
        _ => descriptor.variable.as_str(),
    };

    match (
        base,
        descriptor.level_type,
        descriptor.level_value.map(|v| v as u32),
    ) {
        ("t", Some(103), Some(2)) => "2t".to_string(),
        ("dpt", Some(103), Some(2)) => "2d".to_string(),
        ("u", Some(103), Some(10)) => "10u".to_string(),
        ("v", Some(103), Some(10)) => "10v".to_string(),
        ("u", Some(103), Some(100)) => "100u".to_string(),
        ("v", Some(103), Some(100)) => "100v".to_string(),
        _ => base.to_string(),
    }
}

fn compat_cf_name(descriptor: &MessageDescriptor) -> &'static str {
    match (
        descriptor.discipline,
        descriptor.category,
        descriptor.parameter_number,
    ) {
        (Some(discipline), Some(category), Some(number)) => {
            eccodes_cf_name(discipline, category, number)
        }
        _ => "",
    }
}

fn compat_cf_var_name(descriptor: &MessageDescriptor, short_name: &str) -> String {
    let base = match (
        descriptor.discipline,
        descriptor.category,
        descriptor.parameter_number,
    ) {
        (Some(discipline), Some(category), Some(number)) => {
            let mapped = eccodes_cf_var_name(discipline, category, number);
            if mapped.is_empty() {
                short_name
            } else {
                mapped
            }
        }
        _ => short_name,
    };

    match (
        base,
        descriptor.level_type,
        descriptor.level_value.map(|v| v as u32),
    ) {
        ("t", Some(103), Some(2)) => "t2m".to_string(),
        ("dpt", Some(103), Some(2)) => "d2m".to_string(),
        ("u", Some(103), Some(10)) => "u10".to_string(),
        ("v", Some(103), Some(10)) => "v10".to_string(),
        ("u", Some(103), Some(100)) => "u100".to_string(),
        ("v", Some(103), Some(100)) => "v100".to_string(),
        _ => base.to_string(),
    }
}

fn compat_name(descriptor: &MessageDescriptor) -> String {
    if descriptor.edition == Some(GribEdition::Grib1) {
        if let Some(name) = grib1_compat_name(descriptor) {
            return name.to_string();
        }
    } else {
        let level_value = compat_level_value(descriptor);
        if let Some(entry) = lookup_grib2_compat_entry(descriptor, level_value) {
            return entry.name.to_string();
        }
    }
    descriptor
        .parameter_name
        .clone()
        .unwrap_or_else(|| descriptor.variable.clone())
}

fn compat_level_type_name(descriptor: &MessageDescriptor) -> &'static str {
    match descriptor.edition {
        Some(GribEdition::Grib1) => match descriptor.level_type {
            Some(level_type) => grib1_level_type_name(level_type),
            None => "unknown",
        },
        _ => match descriptor.level_type {
            Some(100) => {
                if descriptor.level_value.unwrap_or(0.0) >= 100.0 {
                    "isobaricInhPa"
                } else {
                    "isobaricInPa"
                }
            }
            Some(level_type) => grib2_level_type_name(level_type),
            None => "unknown",
        },
    }
}

fn grib1_compat_short_name(descriptor: &MessageDescriptor) -> Option<&'static str> {
    let table_version = descriptor.table_version.unwrap_or(0);
    let center = descriptor.center.unwrap_or(0);
    let parameter = descriptor.parameter_number?;
    match (table_version, center, parameter) {
        (128, _, 39) => Some("swvl1"),
        (128, _, 40) => Some("swvl2"),
        (128, _, 41) => Some("swvl3"),
        (128, _, 42) => Some("swvl4"),
        (128, _, 43) => Some("slt"),
        (128, _, 129) => Some("z"),
        (128, _, 130) => Some("t"),
        (128, _, 131) => Some("u"),
        (128, _, 132) => Some("v"),
        (128, _, 133) => Some("q"),
        (128, _, 134) => Some("sp"),
        (128, _, 135) => Some("w"),
        (128, _, 139) => Some("stl1"),
        (128, _, 151) => Some("msl"),
        (128, _, 157) => Some("r"),
        (128, _, 164) => Some("tcc"),
        (128, _, 165) => Some("10u"),
        (128, _, 166) => Some("10v"),
        (128, _, 167) => Some("2t"),
        (128, _, 168) => Some("2d"),
        (128, _, 170) => Some("stl2"),
        (128, _, 172) => Some("lsm"),
        (128, _, 183) => Some("stl3"),
        (128, _, 228) => Some("tp"),
        (128, _, 235) => Some("skt"),
        (128, _, 236) => Some("stl4"),
        (172, _, 228) => Some("tprate"),
        (228, _, 82) => Some("aco2rec"),
        (1, 96, 112) => Some("nlwrs"),
        (_, _, 11) => Some("t"),
        (_, _, 33) => Some("u"),
        (_, _, 34) => Some("v"),
        (_, _, 52) => Some("r"),
        _ => None,
    }
}

fn grib1_compat_name(descriptor: &MessageDescriptor) -> Option<&'static str> {
    let table_version = descriptor.table_version.unwrap_or(0);
    let center = descriptor.center.unwrap_or(0);
    let parameter = descriptor.parameter_number?;
    match (table_version, center, parameter) {
        (128, _, 39) => Some("Volumetric soil water layer 1"),
        (128, _, 40) => Some("Volumetric soil water layer 2"),
        (128, _, 41) => Some("Volumetric soil water layer 3"),
        (128, _, 42) => Some("Volumetric soil water layer 4"),
        (128, _, 43) => Some("Soil type"),
        (128, _, 129) => Some("Geopotential"),
        (128, _, 130) => Some("Temperature"),
        (128, _, 131) => Some("U Component of Wind"),
        (128, _, 132) => Some("V Component of Wind"),
        (128, _, 133) => Some("Specific Humidity"),
        (128, _, 134) => Some("Surface Pressure"),
        (128, _, 135) => Some("Vertical Velocity"),
        (128, _, 139) => Some("Soil temperature level 1"),
        (128, _, 151) => Some("Mean Sea Level Pressure"),
        (128, _, 157) => Some("Relative Humidity"),
        (128, _, 164) => Some("Total Cloud Cover"),
        (128, _, 165) => Some("10 metre U wind component"),
        (128, _, 166) => Some("10 metre V wind component"),
        (128, _, 167) => Some("2 metre temperature"),
        (128, _, 168) => Some("2 metre dewpoint temperature"),
        (128, _, 170) => Some("Soil temperature level 2"),
        (128, _, 172) => Some("Land-sea mask"),
        (128, _, 183) => Some("Soil temperature level 3"),
        (128, _, 228) => Some("Total precipitation"),
        (128, _, 235) => Some("Skin temperature"),
        (128, _, 236) => Some("Soil temperature level 4"),
        (172, _, 228) => Some("Time-mean total precipitation rate"),
        (228, _, 82) => Some("Accumulated Carbon Dioxide Ecosystem Respiration"),
        (1, 96, 112) => Some("Net long-wave radiation flux (surface)"),
        _ => None,
    }
}

fn grib1_compat_units(descriptor: &MessageDescriptor) -> Option<&'static str> {
    let table_version = descriptor.table_version.unwrap_or(0);
    let center = descriptor.center.unwrap_or(0);
    let parameter = descriptor.parameter_number?;
    match (table_version, center, parameter) {
        (128, _, 39) | (128, _, 40) | (128, _, 41) | (128, _, 42) => Some("m**3 m**-3"),
        (128, _, 43) => Some("(Code table 4.213)"),
        (128, _, 129) => Some("m^2/s^2"),
        (128, _, 130)
        | (128, _, 139)
        | (128, _, 167)
        | (128, _, 168)
        | (128, _, 170)
        | (128, _, 183)
        | (128, _, 235)
        | (128, _, 236) => Some("K"),
        (128, _, 131) | (128, _, 132) | (128, _, 135) | (128, _, 165) | (128, _, 166) => {
            Some("m/s")
        }
        (128, _, 133) => Some("kg/kg"),
        (128, _, 134) | (128, _, 151) => Some("Pa"),
        (128, _, 157) | (128, _, 164) => Some("%"),
        (128, _, 172) => Some("0-1"),
        (128, _, 228) => Some("m"),
        (172, _, 228) => Some("m s**-1"),
        (228, _, 82) => Some("kg m**-2"),
        (1, 96, 112) => Some("W m**-2"),
        _ => None,
    }
}

fn compat_grid_type(descriptor: &MessageDescriptor) -> String {
    match descriptor.grid_template {
        Some(0) => "regular_ll".to_string(),
        Some(1) => "rotated_ll".to_string(),
        Some(10) => "mercator".to_string(),
        Some(20) => "polar_stereographic".to_string(),
        Some(30) => "lambert".to_string(),
        Some(40) => "regular_gg".to_string(),
        Some(41) => "reduced_gg".to_string(),
        Some(50) | Some(51) => "sh".to_string(),
        Some(90) => "space_view".to_string(),
        Some(template) => format!("unknown_{template}"),
        None => "unknown".to_string(),
    }
}

fn compat_data_type(descriptor: &MessageDescriptor) -> &'static str {
    match descriptor.forecast_time_value {
        Some(0) | None => "an",
        Some(_) => "fc",
    }
}

fn compat_packing_type(descriptor: &MessageDescriptor) -> &'static str {
    match descriptor.edition {
        Some(GribEdition::Grib1) => "grid_simple",
        Some(GribEdition::Grib2) => match descriptor.data_template {
            Some(0) => "grid_simple",
            Some(2) => "grid_complex",
            Some(3) => "grid_complex_spatial_differencing",
            Some(4) => "grid_ieee",
            Some(40) => "grid_jpeg",
            Some(42) => "grid_ccsds",
            _ => "unknown",
        },
        None => "unknown",
    }
}

fn parse_reference_time(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn compact_date(value: DateTime<Utc>) -> i64 {
    (value.year() as i64) * 10_000 + (value.month() as i64) * 100 + value.day() as i64
}

fn compact_time(value: DateTime<Utc>) -> i64 {
    (value.hour() as i64) * 100 + value.minute() as i64
}

fn time_unit_code_from_name(name: &str) -> Option<i64> {
    match name {
        "minute" => Some(0),
        "hour" => Some(1),
        "day" => Some(2),
        "3 hours" => Some(10),
        "6 hours" => Some(11),
        "12 hours" => Some(12),
        "second" => Some(13),
        _ => None,
    }
}

fn step_unit_seconds(code: i64) -> i64 {
    match code {
        0 => 60,
        1 => 3_600,
        2 => 86_400,
        3 => 2_592_000,
        4 => 31_536_000,
        10 => 10_800,
        11 => 21_600,
        12 => 43_200,
        13 => 1,
        _ => 3_600,
    }
}

fn centre_description(code: u16) -> &'static str {
    match code {
        7 => "US National Weather Service - NCEP",
        34 => "Japanese Meteorological Agency - Tokyo",
        46 => "Brazilian Space Agency - INPE",
        54 => "Canadian Meteorological Service - Montreal",
        74 => "UK Meteorological Office - Exeter",
        78 | 79 => "Deutscher Wetterdienst",
        85 => "French Weather Service - Toulouse",
        98 => "European Centre for Medium-Range Weather Forecasts",
        _ => "Unknown",
    }
}

fn grid_description(grid_type: &str) -> &'static str {
    match grid_type {
        "regular_ll" => "Latitude/longitude. Also called equidistant cylindrical, or Plate Carree",
        "rotated_ll" => "Rotated Latitude/longitude",
        "mercator" => "Mercator",
        "polar_stereographic" => "Polar stereographic projection",
        "lambert" => "Lambert conformal",
        "regular_gg" => "Gaussian latitude/longitude",
        "reduced_gg" => "Reduced Gaussian latitude/longitude",
        "sh" => "Spherical harmonic coefficients",
        "space_view" => "Space view perspective or orthographic",
        _ => "Unknown grid type",
    }
}

fn grib2_level_type_name(level_type: u8) -> &'static str {
    match level_type {
        1 => "surface",
        2 => "cloudBase",
        3 => "cloudTop",
        4 => "isothermZero",
        6 => "maxWind",
        7 => "tropopause",
        8 => "nominalTop",
        10 => "entireAtmosphere",
        20 => "isothermal",
        100 => "isobaricInhPa",
        101 => "meanSea",
        102 => "heightAboveSea",
        103 => "heightAboveGround",
        104 => "sigma",
        105 => "hybrid",
        106 => "depthBelowLand",
        107 => "theta",
        108 => "pressureFromGround",
        109 => "potentialVorticity",
        111 => "eta",
        114 => "snowLayer",
        117 => "mixedLayer",
        118 => "hybridHeight",
        150 => "generalisedHeight",
        160 => "depthBelowSea",
        200 => "entireAtmosphere",
        _ => "unknown",
    }
}

fn grib1_level_type_name(level_type: u8) -> &'static str {
    match level_type {
        1 => "surface",
        2 => "cloudBase",
        3 => "cloudTop",
        4 => "isothermZero",
        5 => "adiabaticCondensation",
        6 => "maxWind",
        7 => "tropopause",
        8 => "nominalTop",
        20 => "isothermal",
        100 => "isobaricInhPa",
        101 => "meanSea",
        102 => "heightAboveSeaLevel",
        103 | 105 => "heightAboveGround",
        104 => "sigma",
        106 | 111 => "depthBelowLand",
        112 => "depthBelowLandLayer",
        107 | 113 => "isentropic",
        108 | 116 => "pressureFromGround",
        109 => "hybrid",
        117 => "potentialVorticity",
        200 => "entireAtmosphere",
        204 => "highestTroposphericFreezing",
        212 => "lowCloudBottom",
        213 => "lowCloudTop",
        214 => "lowCloudLayer",
        222 => "middleCloudBottom",
        223 => "middleCloudTop",
        224 => "middleCloudLayer",
        232 => "highCloudBottom",
        233 => "highCloudTop",
        234 => "highCloudLayer",
        _ => "unknown",
    }
}

fn eccodes_short_name(discipline: u8, category: u8, number: u8) -> &'static str {
    match (discipline, category, number) {
        (0, 0, 0) => "t",
        (0, 0, 2) => "pt",
        (0, 0, 4) => "tmax",
        (0, 0, 5) => "tmin",
        (0, 0, 6) => "dpt",
        (0, 0, 10) => "aptmp",
        (0, 1, 0) => "q",
        (0, 1, 1) => "r",
        (0, 1, 3) => "pwat",
        (0, 1, 8) => "tp",
        (0, 1, 10) => "acpcp",
        (0, 1, 13) => "rwmr",
        (0, 1, 19) => "prate",
        (0, 1, 22) => "tciwv",
        (0, 2, 0) => "wdir",
        (0, 2, 1) => "ws",
        (0, 2, 2) => "u",
        (0, 2, 3) => "v",
        (0, 2, 8) => "w",
        (0, 2, 9) => "wz",
        (0, 2, 10) => "absv",
        (0, 2, 12) => "d",
        (0, 2, 22) => "gust",
        (0, 3, 0) => "sp",
        (0, 3, 1) => "msl",
        (0, 3, 4) => "gph",
        (0, 3, 5) => "gh",
        (0, 3, 9) => "gha",
        (0, 4, 7) => "dswrf",
        (0, 4, 8) => "uswrf",
        (0, 4, 9) => "nswrs",
        (0, 5, 3) => "dlwrf",
        (0, 5, 4) => "ulwrf",
        (0, 5, 5) => "nlwrs",
        (0, 6, 1) => "tcc",
        (0, 6, 3) => "lcc",
        (0, 6, 4) => "mcc",
        (0, 6, 5) => "hcc",
        (0, 6, 6) => "cbase",
        (0, 6, 7) => "ct",
        (0, 7, 6) => "cape",
        (0, 7, 7) => "cin",
        (0, 7, 8) => "hlcy",
        (0, 7, 10) => "lftx",
        (0, 14, 0) => "tco3",
        (0, 19, 1) => "vis",
        (2, 0, 0) => "lsm",
        (2, 0, 1) => "sfcr",
        (2, 0, 2) => "st",
        (2, 0, 5) => "sm",
        (2, 0, 9) => "soilw",
        (2, 3, 0) => "slt",
        (10, 0, 3) => "swh",
        (10, 0, 4) => "mwp",
        (10, 0, 5) => "mwd",
        _ => "",
    }
}

fn eccodes_cf_name(discipline: u8, category: u8, number: u8) -> &'static str {
    match (discipline, category, number) {
        (0, 0, 0) => "air_temperature",
        (0, 0, 2) => "air_potential_temperature",
        (0, 0, 4) => "air_temperature",
        (0, 0, 5) => "air_temperature",
        (0, 0, 6) => "dew_point_temperature",
        (0, 1, 0) => "specific_humidity",
        (0, 1, 1) => "relative_humidity",
        (0, 1, 3) => "atmosphere_mass_content_of_water_vapor",
        (0, 1, 8) => "precipitation_amount",
        (0, 1, 19) => "precipitation_flux",
        (0, 2, 0) => "wind_from_direction",
        (0, 2, 1) => "wind_speed",
        (0, 2, 2) => "eastward_wind",
        (0, 2, 3) => "northward_wind",
        (0, 2, 8) => "lagrangian_tendency_of_air_pressure",
        (0, 2, 10) => "atmosphere_absolute_vorticity",
        (0, 3, 0) => "air_pressure",
        (0, 3, 1) => "air_pressure_at_mean_sea_level",
        (0, 3, 5) => "geopotential_height",
        (0, 6, 1) => "cloud_area_fraction",
        (0, 6, 3) => "low_type_cloud_area_fraction",
        (0, 6, 4) => "medium_type_cloud_area_fraction",
        (0, 6, 5) => "high_type_cloud_area_fraction",
        (0, 7, 6) => "atmosphere_convective_available_potential_energy",
        (0, 7, 7) => "atmosphere_convective_inhibition",
        (2, 0, 0) => "land_binary_mask",
        (2, 0, 2) => "soil_temperature",
        (10, 0, 3) => "sea_surface_wave_significant_height",
        _ => "",
    }
}

fn eccodes_cf_var_name(discipline: u8, category: u8, number: u8) -> &'static str {
    match (discipline, category, number) {
        (0, 0, 0) => "t",
        (0, 0, 6) => "dpt",
        (0, 1, 0) => "q",
        (0, 1, 1) => "r",
        (0, 1, 8) => "tp",
        (0, 2, 2) => "u",
        (0, 2, 3) => "v",
        (0, 3, 0) => "sp",
        (0, 3, 1) => "msl",
        (0, 3, 5) => "gh",
        (0, 6, 1) => "tcc",
        (0, 7, 6) => "cape",
        (0, 7, 7) => "cin",
        (2, 0, 0) => "lsm",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct SimplePackSpec {
        center: u16,
        discipline: u8,
        category: u8,
        number: u8,
        level_type: u8,
        level_value: u32,
        forecast_time: u32,
        reference_value: f32,
        binary_scale: i16,
        decimal_scale: i16,
        bits_per_value: u8,
        payload: Vec<u8>,
    }

    #[test]
    fn exposes_ecmwf_style_keys_for_a_modern_grib2_message() {
        let bytes = build_test_grib2_message(SimplePackSpec {
            center: 98,
            discipline: 0,
            category: 0,
            number: 0,
            level_type: 103,
            level_value: 2,
            forecast_time: 6,
            reference_value: 1.0,
            binary_scale: 0,
            decimal_scale: 0,
            bits_per_value: 8,
            payload: vec![0, 1, 2, 3],
        });

        let handle = codes_grib_new_from_bytes(&bytes).expect("synthetic message should decode");

        assert_eq!(codes_get_string(&handle, "shortName").unwrap(), "2t");
        assert_eq!(codes_get_long(&handle, "paramId").unwrap(), 167);
        assert_eq!(
            codes_get_string(&handle, "typeOfLevel").unwrap(),
            "heightAboveGround"
        );
        assert_eq!(codes_get_double(&handle, "level").unwrap(), 2.0);
        assert_eq!(codes_get_string(&handle, "gridType").unwrap(), "regular_ll");
        assert_eq!(codes_get_long(&handle, "forecastTime").unwrap(), 6);
        assert_eq!(codes_get_long(&handle, "endStep").unwrap(), 6);
        assert_eq!(codes_get_long(&handle, "dataDate").unwrap(), 20260316);
        assert_eq!(codes_get_long(&handle, "dataTime").unwrap(), 1800);
        assert_eq!(codes_get_long(&handle, "validityDate").unwrap(), 20260317);
        assert_eq!(codes_get_long(&handle, "validityTime").unwrap(), 0);
        assert_eq!(codes_get_long(&handle, "numberOfPoints").unwrap(), 4);
        assert_eq!(
            codes_get_string(&handle, "centreDescription").unwrap(),
            "European Centre for Medium-Range Weather Forecasts"
        );

        let values = codes_get_values(&handle);
        assert_eq!(values, vec![1.0, 2.0, 3.0, 4.0]);

        let distinct_lons = codes_get_double_array(&handle, "distinctLongitudes").unwrap();
        let distinct_lats = codes_get_double_array(&handle, "distinctLatitudes").unwrap();
        let longitudes = codes_get_double_array(&handle, "longitudes").unwrap();
        let latitudes = codes_get_double_array(&handle, "latitudes").unwrap();

        assert_eq!(distinct_lons, vec![100.0, 101.0]);
        assert_eq!(distinct_lats, vec![41.0, 40.0]);
        assert_eq!(longitudes, vec![100.0, 101.0, 100.0, 101.0]);
        assert_eq!(latitudes, vec![41.0, 41.0, 40.0, 40.0]);
        assert_eq!(codes_get_size(&handle, "values").unwrap(), 4);
        assert!(codes_is_defined(&handle, "latitudes"));
        assert!(!codes_is_defined(&handle, "notARealKey"));
    }

    #[test]
    fn scans_files_and_iterates_keys() {
        let bytes = [
            build_test_grib2_message(SimplePackSpec {
                center: 7,
                discipline: 0,
                category: 2,
                number: 2,
                level_type: 103,
                level_value: 10,
                forecast_time: 0,
                reference_value: 10.0,
                binary_scale: 0,
                decimal_scale: 0,
                bits_per_value: 8,
                payload: vec![0, 1, 2, 3],
            }),
            build_test_grib2_message(SimplePackSpec {
                center: 7,
                discipline: 0,
                category: 2,
                number: 3,
                level_type: 103,
                level_value: 10,
                forecast_time: 1,
                reference_value: 20.0,
                binary_scale: 0,
                decimal_scale: 0,
                bits_per_value: 8,
                payload: vec![0, 1, 2, 3],
            }),
        ]
        .concat();

        let temp = std::env::temp_dir().join("wx_eccodes_two_messages.grib2");
        std::fs::write(&temp, bytes).expect("temp GRIB should be writable");

        let file = CodesFile::open(&temp).expect("temp GRIB should scan");
        assert_eq!(file.len(), 2);
        assert_eq!(file.message_numbers(), &[1, 2]);
        assert_eq!(codes_count_in_file(&temp).unwrap(), 2);

        let first = file.first().expect("first message should decode");
        assert_eq!(codes_get_string(&first, "shortName").unwrap(), "10u");

        let iterated = codes_keys_iterator_new(&first).collect::<Vec<_>>();
        assert!(iterated.contains(&"shortName".to_string()));
        assert!(iterated.contains(&"values".to_string()));

        std::fs::remove_file(&temp).expect("temp GRIB should be removable");
    }

    #[test]
    fn exposes_isobaric_dewpoint_as_dpt() {
        let bytes = build_test_grib2_message(SimplePackSpec {
            center: 7,
            discipline: 0,
            category: 0,
            number: 6,
            level_type: 100,
            level_value: 50000,
            forecast_time: 0,
            reference_value: 0.0,
            binary_scale: 0,
            decimal_scale: 0,
            bits_per_value: 8,
            payload: vec![0, 1, 2, 3],
        });

        let handle = codes_grib_new_from_bytes(&bytes).expect("synthetic message should decode");

        assert_eq!(codes_get_string(&handle, "shortName").unwrap(), "dpt");
        assert_eq!(codes_get_long(&handle, "paramId").unwrap(), 3017);
        assert_eq!(codes_get_string(&handle, "cfVarName").unwrap(), "dpt");
    }

    #[test]
    fn exposes_100m_wind_as_100u() {
        let bytes = build_test_grib2_message(SimplePackSpec {
            center: 98,
            discipline: 0,
            category: 2,
            number: 2,
            level_type: 103,
            level_value: 100,
            forecast_time: 0,
            reference_value: 0.0,
            binary_scale: 0,
            decimal_scale: 0,
            bits_per_value: 8,
            payload: vec![0, 1, 2, 3],
        });

        let handle = codes_grib_new_from_bytes(&bytes).expect("synthetic message should decode");

        assert_eq!(codes_get_string(&handle, "shortName").unwrap(), "100u");
        assert_eq!(codes_get_long(&handle, "paramId").unwrap(), 228246);
        assert_eq!(codes_get_string(&handle, "cfVarName").unwrap(), "u100");
    }

    #[test]
    fn exposes_ncep_surface_geopotential_height_as_orog() {
        let bytes = build_test_grib2_message(SimplePackSpec {
            center: 7,
            discipline: 0,
            category: 3,
            number: 5,
            level_type: 1,
            level_value: 0,
            forecast_time: 0,
            reference_value: 0.0,
            binary_scale: 0,
            decimal_scale: 0,
            bits_per_value: 8,
            payload: vec![0, 1, 2, 3],
        });

        let handle = codes_grib_new_from_bytes(&bytes).expect("synthetic message should decode");

        assert_eq!(codes_get_string(&handle, "shortName").unwrap(), "orog");
        assert_eq!(codes_get_long(&handle, "paramId").unwrap(), 228002);
    }

    #[test]
    fn exposes_ncep_local_cloud_mixing_ratio() {
        let bytes = build_test_grib2_message(SimplePackSpec {
            center: 7,
            discipline: 0,
            category: 1,
            number: 22,
            level_type: 100,
            level_value: 50000,
            forecast_time: 0,
            reference_value: 0.0,
            binary_scale: 0,
            decimal_scale: 0,
            bits_per_value: 8,
            payload: vec![0, 1, 2, 3],
        });

        let handle = codes_grib_new_from_bytes(&bytes).expect("synthetic message should decode");

        assert_eq!(codes_get_string(&handle, "shortName").unwrap(), "clwmr");
        assert_eq!(codes_get_long(&handle, "paramId").unwrap(), 260018);
    }

    fn build_test_grib2_message(spec: SimplePackSpec) -> Vec<u8> {
        let section1 = build_section1(spec.center);
        let section3 = build_section3();
        let section4 = build_section4(
            spec.category,
            spec.number,
            spec.level_type,
            spec.level_value,
            spec.forecast_time,
        );
        let template = if spec.bits_per_value == 32 || spec.bits_per_value == 64 {
            4
        } else {
            0
        };
        let section5 = build_section5(
            template,
            4,
            spec.reference_value,
            spec.binary_scale,
            spec.decimal_scale,
            spec.bits_per_value,
        );
        let section6 = vec![0, 0, 0, 6, 6, 255];
        let section7 = build_section7(&spec.payload);

        let total_length = 16
            + section1.len()
            + section3.len()
            + section4.len()
            + section5.len()
            + section6.len()
            + section7.len()
            + 4;

        let mut out = Vec::with_capacity(total_length);
        out.extend_from_slice(b"GRIB");
        out.extend_from_slice(&[0, 0]);
        out.push(spec.discipline);
        out.push(2);
        out.extend_from_slice(&(total_length as u64).to_be_bytes());
        out.extend_from_slice(&section1);
        out.extend_from_slice(&section3);
        out.extend_from_slice(&section4);
        out.extend_from_slice(&section5);
        out.extend_from_slice(&section6);
        out.extend_from_slice(&section7);
        out.extend_from_slice(b"7777");
        out
    }

    fn build_section1(center: u16) -> Vec<u8> {
        let mut sec = vec![0u8; 21];
        sec[0..4].copy_from_slice(&(21u32).to_be_bytes());
        sec[4] = 1;
        sec[5..7].copy_from_slice(&center.to_be_bytes());
        sec[7..9].copy_from_slice(&(0u16).to_be_bytes());
        sec[9] = 28;
        sec[10] = 0;
        sec[11] = 1;
        sec[12..14].copy_from_slice(&(2026u16).to_be_bytes());
        sec[14] = 3;
        sec[15] = 16;
        sec[16] = 18;
        sec[17] = 0;
        sec[18] = 0;
        sec[19] = 0;
        sec[20] = 1;
        sec
    }

    fn build_section3() -> Vec<u8> {
        let mut sec = vec![0u8; 72];
        sec[0..4].copy_from_slice(&(72u32).to_be_bytes());
        sec[4] = 3;
        sec[5] = 0;
        sec[6..10].copy_from_slice(&(4u32).to_be_bytes());
        sec[10] = 0;
        sec[11] = 0;
        sec[12..14].copy_from_slice(&(0u16).to_be_bytes());
        sec[14] = 6;
        sec[30..34].copy_from_slice(&(2u32).to_be_bytes());
        sec[34..38].copy_from_slice(&(2u32).to_be_bytes());
        sec[46..50].copy_from_slice(&(41_000_000u32).to_be_bytes());
        sec[50..54].copy_from_slice(&(100_000_000u32).to_be_bytes());
        sec[55..59].copy_from_slice(&(40_000_000u32).to_be_bytes());
        sec[59..63].copy_from_slice(&(101_000_000u32).to_be_bytes());
        sec[63..67].copy_from_slice(&(1_000_000u32).to_be_bytes());
        sec[67..71].copy_from_slice(&(1_000_000u32).to_be_bytes());
        sec[71] = 0;
        sec
    }

    fn build_section4(
        category: u8,
        number: u8,
        level_type: u8,
        level_value: u32,
        forecast_time: u32,
    ) -> Vec<u8> {
        let mut sec = vec![0u8; 34];
        sec[0..4].copy_from_slice(&(34u32).to_be_bytes());
        sec[4] = 4;
        sec[5..7].copy_from_slice(&(0u16).to_be_bytes());
        sec[7..9].copy_from_slice(&(0u16).to_be_bytes());
        sec[9] = category;
        sec[10] = number;
        sec[11] = 2;
        sec[17] = 1;
        sec[18..22].copy_from_slice(&forecast_time.to_be_bytes());
        sec[22] = level_type;
        sec[23] = 0;
        sec[24..28].copy_from_slice(&level_value.to_be_bytes());
        sec
    }

    fn build_section5(
        template: u16,
        num_points: u32,
        reference_value: f32,
        binary_scale: i16,
        decimal_scale: i16,
        bits_per_value: u8,
    ) -> Vec<u8> {
        let mut sec = vec![0u8; 21];
        sec[0..4].copy_from_slice(&(21u32).to_be_bytes());
        sec[4] = 5;
        sec[5..9].copy_from_slice(&num_points.to_be_bytes());
        sec[9..11].copy_from_slice(&template.to_be_bytes());
        sec[11..15].copy_from_slice(&reference_value.to_be_bytes());
        sec[15..17].copy_from_slice(&binary_scale.to_be_bytes());
        sec[17..19].copy_from_slice(&decimal_scale.to_be_bytes());
        sec[19] = bits_per_value;
        sec[20] = 0;
        sec
    }

    fn build_section7(payload: &[u8]) -> Vec<u8> {
        let mut sec = vec![0u8; 5 + payload.len()];
        sec[0..4].copy_from_slice(&((5 + payload.len()) as u32).to_be_bytes());
        sec[4] = 7;
        sec[5..].copy_from_slice(payload);
        sec
    }
}
