use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::UNIX_EPOCH;

use crate::grib::{GribEngine, GribInventory, MessageDescriptor};

const DEFAULT_MAX_ENTRIES: usize = 32;
const DEFAULT_MAX_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedFileSignature {
    len: u64,
    modified_unix_nanos: Option<u128>,
}

#[derive(Debug)]
pub struct CachedGribFile {
    signature: CachedFileSignature,
    inventory: GribInventory,
    bytes: Arc<[u8]>,
}

impl CachedGribFile {
    pub fn inventory(&self) -> &GribInventory {
        &self.inventory
    }

    pub fn message_by_number(&self, message_no: u64) -> Option<&MessageDescriptor> {
        let index = usize::try_from(message_no.checked_sub(1)?).ok()?;
        self.inventory
            .messages
            .get(index)
            .filter(|message| message.message_no == message_no)
            .or_else(|| {
                self.inventory
                    .messages
                    .iter()
                    .find(|message| message.message_no == message_no)
            })
    }

    pub fn message_bytes<'a>(&'a self, message: &MessageDescriptor) -> Result<&'a [u8], String> {
        let start = usize::try_from(message.offset_bytes)
            .map_err(|_| "message offset does not fit in usize".to_string())?;
        let length = usize::try_from(message.length_bytes)
            .map_err(|_| "message length does not fit in usize".to_string())?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| "message byte range overflowed".to_string())?;
        self.bytes.get(start..end).ok_or_else(|| {
            format!(
                "message {} bytes are out of range for cached file",
                message.message_no
            )
        })
    }

    fn len_bytes(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Clone, Copy)]
pub struct GribFileCacheInfo {
    pub enabled: bool,
    pub entries: usize,
    pub total_bytes: usize,
    pub max_entries: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy)]
struct GribFileCacheConfig {
    enabled: bool,
    max_entries: usize,
    max_bytes: usize,
}

struct CacheEntry {
    file: Arc<CachedGribFile>,
    bytes_len: usize,
    last_access: u64,
}

struct GribFileCacheState {
    entries: HashMap<PathBuf, CacheEntry>,
    total_bytes: usize,
    access_counter: u64,
    config: GribFileCacheConfig,
}

fn cache_state() -> &'static Mutex<GribFileCacheState> {
    static CACHE: OnceLock<Mutex<GribFileCacheState>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(GribFileCacheState {
            entries: HashMap::new(),
            total_bytes: 0,
            access_counter: 0,
            config: GribFileCacheConfig {
                enabled: parse_enabled_env(),
                max_entries: parse_usize_env("ECRUST_GRIB_CACHE_MAX_ENTRIES", DEFAULT_MAX_ENTRIES),
                max_bytes: parse_usize_env("ECRUST_GRIB_CACHE_MAX_BYTES", DEFAULT_MAX_BYTES),
            },
        })
    })
}

fn parse_enabled_env() -> bool {
    env::var("ECRUST_GRIB_CACHE")
        .ok()
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "0" | "false" | "off" | "no" | "disabled")
        })
        .unwrap_or(true)
}

fn parse_usize_env(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn next_access_tick(state: &mut GribFileCacheState) -> u64 {
    state.access_counter = state.access_counter.wrapping_add(1);
    state.access_counter
}

fn remove_entry(state: &mut GribFileCacheState, key: &Path) {
    if let Some(entry) = state.entries.remove(key) {
        state.total_bytes = state.total_bytes.saturating_sub(entry.bytes_len);
    }
}

fn evict_to_limits(state: &mut GribFileCacheState) {
    while state.entries.len() > state.config.max_entries
        || state.total_bytes > state.config.max_bytes
    {
        let Some((oldest_key, _)) = state
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
        else {
            break;
        };
        let oldest_key = oldest_key.clone();
        remove_entry(state, &oldest_key);
    }
}

fn cached_file_signature(path: &Path) -> Result<(PathBuf, CachedFileSignature), String> {
    let metadata =
        fs::metadata(path).map_err(|err| format!("failed to stat '{}': {err}", path.display()))?;
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_nanos());
    Ok((
        canonical,
        CachedFileSignature {
            len: metadata.len(),
            modified_unix_nanos,
        },
    ))
}

fn load_grib_file(
    path: &Path,
    signature: CachedFileSignature,
) -> Result<Arc<CachedGribFile>, String> {
    let bytes =
        fs::read(path).map_err(|err| format!("failed to read '{}': {err}", path.display()))?;
    let inventory = GribEngine::new().scan_bytes(&bytes)?;
    Ok(Arc::new(CachedGribFile {
        signature,
        inventory,
        bytes: Arc::<[u8]>::from(bytes),
    }))
}

pub fn cached_grib_file(path: &Path) -> Result<Arc<CachedGribFile>, String> {
    let (cache_key, signature) = cached_file_signature(path)?;
    {
        let mut state = cache_state()
            .lock()
            .map_err(|_| "internal state lock poisoned".to_string())?;
        if !state.config.enabled {
            drop(state);
            return load_grib_file(&cache_key, signature);
        }
        let tick = next_access_tick(&mut state);
        if let Some(entry) = state.entries.get_mut(&cache_key) {
            if entry.file.signature == signature {
                entry.last_access = tick;
                return Ok(Arc::clone(&entry.file));
            }
        }
        remove_entry(&mut state, &cache_key);
    }

    let loaded = load_grib_file(&cache_key, signature)?;
    let loaded_bytes_len = loaded.len_bytes();

    let mut state = cache_state()
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;
    if !state.config.enabled {
        return Ok(loaded);
    }
    let tick = next_access_tick(&mut state);
    if let Some(entry) = state.entries.get_mut(&cache_key) {
        if entry.file.signature == signature {
            entry.last_access = tick;
            return Ok(Arc::clone(&entry.file));
        }
    }
    remove_entry(&mut state, &cache_key);
    state.total_bytes = state.total_bytes.saturating_add(loaded_bytes_len);
    state.entries.insert(
        cache_key,
        CacheEntry {
            file: Arc::clone(&loaded),
            bytes_len: loaded_bytes_len,
            last_access: tick,
        },
    );
    evict_to_limits(&mut state);
    Ok(loaded)
}

pub fn clear_grib_file_cache() -> Result<(), String> {
    let mut state = cache_state()
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;
    state.entries.clear();
    state.total_bytes = 0;
    Ok(())
}

pub fn grib_file_cache_info() -> Result<GribFileCacheInfo, String> {
    let state = cache_state()
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;
    Ok(GribFileCacheInfo {
        enabled: state.config.enabled,
        entries: state.entries.len(),
        total_bytes: state.total_bytes,
        max_entries: state.config.max_entries,
        max_bytes: state.config.max_bytes,
    })
}

pub fn set_grib_file_cache_enabled(enabled: bool) -> Result<(), String> {
    let mut state = cache_state()
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;
    state.config.enabled = enabled;
    if !enabled {
        state.entries.clear();
        state.total_bytes = 0;
    }
    Ok(())
}

pub fn set_grib_file_cache_limits(max_entries: usize, max_bytes: usize) -> Result<(), String> {
    let mut state = cache_state()
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;
    state.config.max_entries = max_entries.max(1);
    state.config.max_bytes = max_bytes.max(1);
    evict_to_limits(&mut state);
    Ok(())
}
