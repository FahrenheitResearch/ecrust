# Agent Context

This file is a handoff snapshot for review and follow-on agent work.

## Handoff Summary

- The biggest speed win in the latest pass was replacing the packed-bit byte-window reader with a buffered bit-reservoir reader in `BitReader`.
- Windows release builds now default to `mimalloc`; the explicit `system-allocator` override was materially slower on the HRRR workload, while Linux/WSL stayed best on the system allocator by default.
- Release-readiness verification is stronger now: there is a dedicated drop-in surface test module plus a cross-platform GitHub Actions CI workflow that builds the extension and runs the parity suite on Linux and Windows.
- Release-readiness verification now also includes pinned official ECMWF, GFS, and RRFS subsets, assembled from published `.idx` / `.index` sidecars via HTTP range requests so the parity suite covers real model files without pulling the full upstream GRIBs.
- Linux validation is now complete on Ubuntu 24.04 WSL2 using ext4-hosted repo and data copies, and `ecrust` is still well ahead of ecCodes on both the single full HRRR file and the 4-copy multi-file run.
- `scripts/benchmark_hrrr_value_phases.py` now measures true streaming decode-only time; the older "decode-only" numbers were actually decode-plus-cache-retention because they held all decoded HRRR arrays alive at once.
- The focused and parallel benchmark JSON now includes `ecrust_runtime` with allocator and target metadata, so reruns are attributable to a concrete build variant.
- Native non-WSL Linux is still worth validating before treating the WSL ratios as the final portable baseline.

## Current Status

- `ecrust` is now clean on the existing real-world parity corpus.
- Full-file HRRR metadata parity is fixed for `shortName` and `paramId`.
- `codes_get_values` and `codes_get_double_array(..., "values")` now expose read-only zero-copy NumPy views backed by the cached Rust values buffer.
- After the buffered `BitReader` rewrite, the isolated single-file HRRR values path is now faster than ecCodes end-to-end on both Windows and Ubuntu 24.04 WSL2.
- On the latest verified 4-copy HRRR reruns on both Windows and Ubuntu 24.04 WSL2, `ecrust` is substantially faster than ecCodes at 1, 2, and 4 workers.
- The focused decode benchmark now streams uncached decode work one message at a time, so it no longer folds full-file retained-cache cost into the "decode-only" phase.
- `codes_get_version_info()` and the benchmark scripts now report allocator and target metadata.
- On Windows, a plain release build now uses `mimalloc` by default; on non-Windows, the default remains the system allocator unless `--features mimalloc` is explicitly enabled.
- A new `tests/test_dropin_surface.py` file now verifies top-level drop-in exports, GRIB/ANY file dispatch, string/list `codes_set_key_vals` behavior, generic setter/dump coverage, and clone semantics.
- `.github/workflows/ci.yml` now builds with `maturin develop --release` and runs `pytest -q` on Linux and Windows.
- The parity suite now directly covers pinned official ECMWF, GFS, and RRFS near-surface GRIB2 subsets in addition to the cfgrib sample corpus.
- `.github/workflows/release.yml` now builds Linux `x86_64` manylinux wheels, Windows `x64` wheels, and an sdist on tags or manual dispatch, uploads tagged artifacts to the GitHub release, and can publish to TestPyPI/PyPI.

## Recent Changes

- Added bounded shared GRIB file cache and Python cache controls.
- Added hardened workflow benchmarks in `scripts/benchmark_workflows.py`.
- Made file-backed handles lazy so message construction does not eagerly decode values.
- Removed a major multi-message slowdown by reusing cached file bytes and inventory.
- Added a GRIB2 compatibility table in `src/grib2_compat.rs` and routed `src/compat.rs` through it.
- Fixed HRRR/NCEP metadata mappings so the full HRRR pressure-level file now matches ecCodes on `shortName` and `paramId` for all messages.
- Added `scripts/benchmark_parallel_hrrr.py` to measure multi-file throughput with full HRRR copies.
- Added `scripts/benchmark_hrrr_value_phases.py` to split single-file HRRR timing into end-to-end, decode-only, and cached-export phases.
- Precomputed GRIB scaling factors once per message instead of recomputing `powi` per decoded value.
- Reworked GRIB2 template 2/3 no-missing decode so group expansion, spatial differencing, and float scaling stream directly into the final `Vec<f64>` instead of materializing a full raw integer buffer first.
- Replaced `BitReader::read_bits`' per-bit loop with a byte-window extractor so packed reads no longer shift one bit at a time.
- Added focused `BitReader` regression tests for unaligned reads in `src/grib.rs`.
- Tightened template-3 order-1/order-2 spatial differencing loops so seed handling is hoisted out of the inner recurrence path.
- Removed the extra Python-side copy for native/double `"values"` access by returning borrowed read-only NumPy arrays tied to the handle lifetime.
- Routed `codes_get_double_element` and `codes_get_double_elements` for `"values"` through the cached Rust slice instead of cloning the full values array first.
- Replaced the byte-window `BitReader` with a buffered bit-reservoir reader so repeated packed-field reads no longer rebuild a fresh chunk per value.
- Added allocator/target reporting to `codes_get_version_info()` and benchmark JSON.
- Added optional allocator selection, defaulting Windows release builds to `mimalloc` while leaving non-Windows builds on the system allocator unless explicitly overridden.
- Added drop-in API surface tests covering module exports, file dispatch, generic setters, JSON dump behavior, and clone independence.
- Added GitHub Actions CI for Linux and Windows release builds plus the parity test suite.
- Added official operational ECMWF, GFS, and RRFS parity fixtures that range-download selected GRIB messages from published `.idx` / `.index` sidecars instead of fetching full source files.
- Added a dedicated release workflow plus a short release checklist in `README.md`.

## Key Files

- [src/lib.rs](/C:/Users/drew/ecrust/src/lib.rs)
- [src/compat.rs](/C:/Users/drew/ecrust/src/compat.rs)
- [src/grib.rs](/C:/Users/drew/ecrust/src/grib.rs)
- [src/grib_cache.rs](/C:/Users/drew/ecrust/src/grib_cache.rs)
- [src/grib2_compat.rs](/C:/Users/drew/ecrust/src/grib2_compat.rs)
- [.github/workflows/ci.yml](/C:/Users/drew/ecrust/.github/workflows/ci.yml)
- [scripts/benchmark_workflows.py](/C:/Users/drew/ecrust/scripts/benchmark_workflows.py)
- [scripts/benchmark_parallel_hrrr.py](/C:/Users/drew/ecrust/scripts/benchmark_parallel_hrrr.py)
- [scripts/benchmark_hrrr_value_phases.py](/C:/Users/drew/ecrust/scripts/benchmark_hrrr_value_phases.py)
- [tests/test_dropin_surface.py](/C:/Users/drew/ecrust/tests/test_dropin_surface.py)
- [tests/test_ecrust.py](/C:/Users/drew/ecrust/tests/test_ecrust.py)

Start here for the current hot path:

- `BitReader::read_bits` and `BitReader::align_to_octet` in `src/grib.rs`
- `unpack_complex_no_missing` in `src/grib.rs`
- `borrowed_values_array`, `codes_get_values`, and the `"values"` array getters in `src/lib.rs`

## Benchmarks

### Broad Real-World Corpus

Command:

```powershell
python scripts\benchmark_workflows.py
```

Latest result:

- Fresh single-pass: `ecrust/eccodes = 0.099x`
- Warm repeated-pass: `0.144x`
- Fresh single-pass, ecrust cache disabled: `0.365x`
- Strict cold per file: `0.036x`
- Digest, message count, and value count all match

### Single Full HRRR 3D File

File used:

- `C:\Users\drew\AppData\Local\Temp\ecrust_hrrr_bench\hrrr.t00z.wrfprsf00.grib2`

Command:

```powershell
python scripts\benchmark_hrrr_value_phases.py --json
```

Latest focused timing over all 708 messages on this Windows machine on `2026-03-30` with the default Windows `mimalloc` build:

- End-to-end:
  - `eccodes 8.25s`
  - `ecrust 5.04s`
  - ratio `0.61x`
- `ecrust` decode-only: `4.11s`
- `ecrust` cached export only: `0.57s`
- Cached exported arrays report `writeable = false`
- Digest, message count, and value count all match
- `ecrust_runtime.allocator = "mimalloc"`

Metadata parity on this file:

- `708/708` messages match ecCodes on `shortName`
- `708/708` messages match ecCodes on `paramId`

Interpretation:

- The single-file HRRR path on this machine is now faster than ecCodes end-to-end.
- The phase split still shows the bulk of the work is decode, not NumPy export.
- The zero-copy values export path is working and materially cheaper than the decode itself.

### Multi-File HRRR Throughput

Command:

```powershell
python scripts\benchmark_parallel_hrrr.py --json
```

Setup:

- Uses 4 full-file HRRR copies under `C:\Users\drew\AppData\Local\Temp\ecrust_hrrr_parallel`
- Each run reads all messages, keys, and values for each file

Latest verified wall-clock results on `2026-03-30` with the default Windows `mimalloc` build:

- 1 worker:
  - `eccodes 39.46s`
  - `ecrust 15.55s`
  - ratio `0.39x`
- 2 workers:
  - `eccodes 21.50s`
  - `ecrust 9.37s`
  - ratio `0.44x`
- 4 workers:
  - `eccodes 12.14s`
  - `ecrust 5.07s`
  - ratio `0.42x`

Interpretation:

- `ecrust` does parallelize cleanly across files.
- On this rerun, `ecrust` is faster than ecCodes at every tested worker count.
- The Windows allocator change widened the multi-file lead materially on top of the earlier decode-path wins.
- The exact Windows wall-clock numbers still move around with machine state, so compare ratios and rerun on Linux before drawing broad scaling conclusions.

### Linux / WSL HRRR Validation

Environment used:

- Ubuntu 24.04 on WSL2
- Repo copy under `/home/drew/ecrust-linux-work`
- HRRR source copy under `/home/drew/ecrust-hrrr-bench`
- Benchmarks run from the Linux filesystem, not `/mnt/c`

Single-file focused timing over all 708 messages on `2026-03-30` with the default Linux system allocator build:

- End-to-end:
  - `eccodes 8.08s`
  - `ecrust 4.47s`
  - ratio `0.55x`
- `ecrust` decode-only: `2.95s`
- `ecrust` cached export only: `0.41s`
- Cached exported arrays report `writeable = false`
- Digest, message count, and value count all match
- `ecrust_runtime.allocator = "system"`

Multi-file 4-copy throughput on `2026-03-30` with the default Linux system allocator build:

- 1 worker:
  - `eccodes 35.20s`
  - `ecrust 15.72s`
  - ratio `0.45x`
- 2 workers:
  - `eccodes 17.91s`
  - `ecrust 8.30s`
  - ratio `0.46x`
- 4 workers:
  - `eccodes 10.32s`
  - `ecrust 4.72s`
  - ratio `0.46x`

Interpretation:

- The Windows lead was not a Windows-only artifact; the same workload is decisively faster under Linux/WSL too.
- Linux/WSL still widens the end-to-end gap materially versus the Windows reruns, even though the exact wall-clock ratios move with machine state.
- The decode-only benchmark is now directionally sane on Linux because it no longer retains every decoded HRRR array at once.
- WSL is good evidence, but native non-WSL Linux is still worth one more validation pass before treating the ratios as final.

### Allocator Comparison

Windows comparison on `2026-03-30`:

- Explicit `system-allocator` override:
  - Single-file end-to-end `7.99s`
  - 4-copy multi-file ratios vs ecCodes: `0.59x`, `0.73x`, `0.66x`
- Default `mimalloc` build:
  - Single-file end-to-end `5.04s`
  - 4-copy multi-file ratios vs ecCodes: `0.39x`, `0.44x`, `0.42x`

Linux / WSL comparison on `2026-03-30`:

- Default system allocator:
  - Single-file end-to-end `4.47s`
  - 4-copy multi-file ratios vs ecCodes: `0.45x`, `0.46x`, `0.46x`
- Opt-in `mimalloc` build:
  - Single-file end-to-end `3.83s`
  - 4-copy multi-file ratios vs ecCodes: `0.47x`, `0.51x`, `0.48x`

Interpretation:

- `mimalloc` is the right default on Windows for this workload.
- On Linux/WSL, `mimalloc` helps the single-file case a bit but does not improve the saturated multi-file run consistently enough to justify changing the default yet.

## Correctness Notes

- HRRR values were already matching in aggregate checks before the metadata work.
- The fixed parity issue was mapping logic, not raw decoded values.
- The benchmark corpus parity is still clean after the HRRR mapping, decode-path, and zero-copy export changes.
- The automated parity suite now includes direct official ECMWF, GFS, and RRFS checks on 2 m temperature/dewpoint-or-RH and 10 m wind messages, not just the cfgrib sample corpus and local optional subsets.
- `codes_get_values` and `codes_get_double_array(..., "values")` now return read-only NumPy arrays backed by the handle-owned Rust buffer.
- `codes_get_float_array(..., "values")` and `codes_get_long_array(..., "values")` still allocate converted arrays by design.
- The parallel HRRR benchmark still matches ecCodes on digest, message count, and value count for every worker count tested.
- On Windows local dev, Python tests and benchmarks use `python\ecrust\_ecrust.cp313-win_amd64.pyd`; after `cargo build --release`, copy `target\release\_ecrust.dll` onto that `.pyd` or you will benchmark a stale binary.
- `scripts/benchmark_hrrr_value_phases.py` now uses `_benchmark_decode_values_uncached` for its decode-only phase; older decode-only results from before this change are not directly comparable because they retained the full decoded HRRR file in memory.
- On Ubuntu 24.04 WSL with Rust `1.94.1`, `maturin develop --release` hit a rustc warning-emission ICE; `RUSTFLAGS="-Awarnings" maturin develop --release` worked around it.
- `scripts/benchmark_hrrr_value_phases.py` and `scripts/benchmark_parallel_hrrr.py` now emit `ecrust_runtime` with allocator and target metadata.
- Windows plain release builds now use `mimalloc` by default; use `cargo build --release --features system-allocator` for A/B comparison against the system allocator.
- Non-Windows builds still default to the system allocator; opt into `--features mimalloc` only when doing allocator experiments.

## Tests

Latest local verification:

```powershell
cargo test bit_reader -- --nocapture
cargo build --release
Copy-Item target\release\_ecrust.dll python\ecrust\_ecrust.cp313-win_amd64.pyd -Force
python -m pytest -q
python scripts\benchmark_hrrr_value_phases.py --json
python scripts\benchmark_parallel_hrrr.py --json
python scripts\benchmark_workflows.py
```

Latest Python test result:

- `22 passed`

Latest Linux/WSL verification:

```powershell
wsl.exe -e bash -lc 'export PATH=$HOME/.cargo/bin:$PATH; . /home/drew/.venvs/ecrust-linux/bin/activate; cd /home/drew/ecrust-linux-work; RUSTFLAGS="-Awarnings" maturin develop --release'
wsl.exe -e bash -lc '. /home/drew/.venvs/ecrust-linux/bin/activate; cd /home/drew/ecrust-linux-work; python scripts/benchmark_hrrr_value_phases.py --source /home/drew/ecrust-hrrr-bench/hrrr.t00z.wrfprsf00.grib2 --json'
wsl.exe -e bash -lc '. /home/drew/.venvs/ecrust-linux/bin/activate; cd /home/drew/ecrust-linux-work; python scripts/benchmark_parallel_hrrr.py --source /home/drew/ecrust-hrrr-bench/hrrr.t00z.wrfprsf00.grib2 --copy-dir /home/drew/ecrust-hrrr-parallel --json'
wsl.exe -e bash -lc 'export PATH=$HOME/.cargo/bin:$PATH; . /home/drew/.venvs/ecrust-linux/bin/activate; cd /home/drew/ecrust-linux-work; RUSTFLAGS="-Awarnings" maturin develop --release --features mimalloc'
```

## Reference Repo

- `cfrust` was useful as design inspiration for lazy decode, but not as a direct source for the NCEP/HRRR concept mappings.

## Next Likely Targets

- Validate the new single-file and multi-file lead on native non-WSL Linux before treating the WSL ratios as the final portable baseline.
- If more throughput still matters, inspect memory-bandwidth behavior and float-scaling/cache locality at saturation rather than assuming the next win is still in bit unpacking.
- If another decode pass is needed, inspect float scaling and cache locality in `unpack_complex_no_missing` now that bit extraction is much cheaper.
- Expand GRIB2 compatibility coverage beyond the current real-world corpus and HRRR tuples so additional models do not need one-off fixes.
- If multi-file throughput remains a priority, validate whether native non-WSL Linux tells the same allocator story as WSL before trying more allocator work.
- Before tagging a public release, run the new GitHub Actions matrix once against the repo and confirm the ecCodes wheel/install story on both runner OSes stays green.
- Before the first public PyPI push, run `.github/workflows/release.yml` with `publish_to=testpypi` and verify install/import on both Windows and Linux.
- If broader model portability matters more than more HRRR tuning, reuse the same official-fixture pattern for the next model families that matter for release confidence, such as NAM, GDAS, or GEFS.

## Next Agent Checklist

1. If you touch Rust code on Windows, rebuild and refresh the Windows extension module:
   `cargo build --release`
   `Copy-Item target\release\_ecrust.dll python\ecrust\_ecrust.cp313-win_amd64.pyd -Force`
2. If you need the pre-change Windows allocator baseline, build it explicitly:
   `cargo build --release --features system-allocator`
3. If you need to rerun the focused Windows HRRR benchmark:
   `python scripts\benchmark_hrrr_value_phases.py --json`
4. If you need to rerun the Windows multi-file HRRR benchmark:
   `python scripts\benchmark_parallel_hrrr.py --json`
5. For Linux/WSL reruns, use ext4-hosted repo/data copies and the current Rust workaround:
   `wsl.exe -e bash -lc 'export PATH=$HOME/.cargo/bin:$PATH; . /home/drew/.venvs/ecrust-linux/bin/activate; cd /home/drew/ecrust-linux-work; RUSTFLAGS="-Awarnings" maturin develop --release'`
6. For Linux allocator experiments, opt in explicitly:
   `wsl.exe -e bash -lc 'export PATH=$HOME/.cargo/bin:$PATH; . /home/drew/.venvs/ecrust-linux/bin/activate; cd /home/drew/ecrust-linux-work; RUSTFLAGS="-Awarnings" maturin develop --release --features mimalloc'`
7. If the goal is portability rather than another Windows-specific micro-optimization, move next to native non-WSL Linux or decode-path locality work before changing the allocator story again.
8. Before a public GitHub release, watch the first `.github/workflows/ci.yml` run and fix any runner-specific install issues rather than assuming local Windows + WSL results cover them.

## Review Focus

- Validate whether the stronger WSL Linux lead also holds on native non-WSL Linux and whether it tells the same allocator story as WSL.
- Check whether the next decode limit is now float scaling, cache locality, or memory bandwidth rather than bit extraction.
- Review the borrowed-array lifetime and read-only semantics around the `"values"` getters for API safety and documentation clarity.
- Review `src/grib2_compat.rs` for maintainability: it is effective for the current corpus and HRRR, but it is not yet a general replacement for all ecCodes GRIB2 definitions.
