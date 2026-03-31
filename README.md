# ecrust

Rust-native Python package that targets the low-level `eccodes` import surface
for modern GRIB workflows.

Current benchmark and handoff state for follow-on work lives in `AGENT_CONTEXT.md`.
The latest Windows HRRR benchmark there shows `ecrust` ahead of ecCodes on both
the single-file end-to-end values path and the 4-copy multi-file run.
GitHub CI for release verification now lives in `.github/workflows/ci.yml`.

Goal:

- replace `import eccodes` with `import ecrust`
- keep the common `codes_*` handle/get/release flow unchanged
- avoid a native ecCodes C-library dependency

For the supported GRIB workflows, `ecrust` is a pure-Rust path and does not
require linking against the ecCodes C library.

Current implemented surface:

- `codes_grib_new_from_file`
- `codes_new_from_file`
- `codes_any_new_from_file`
- `codes_count_in_file`
- `codes_get`, `codes_get_long`, `codes_get_double`, `codes_get_string`
- `codes_get_array`, `codes_get_double_array`, `codes_get_long_array`
- `codes_get_values`
- `codes_is_defined`, `codes_is_missing`
- `codes_get_size`, `codes_get_offset`, `codes_get_message_offset`, `codes_get_message_size`
- `codes_get_native_type`
- `codes_keys_iterator_*`
- `codes_skip_*`
- `codes_clone`, `codes_release`

Performance note:

- `codes_get_values` and native/double `"values"` array access now return
  read-only NumPy arrays backed by Rust-owned memory to avoid an extra copy on
  the hot path.
- `codes_get_version_info()` and the benchmark scripts now report allocator and
  target metadata so performance reruns are attributable to a concrete build
  variant.

Windows dev note:

- Local Python tests and benchmarks load `python\ecrust\_ecrust.cp313-win_amd64.pyd`.
  After `cargo build --release`, copy `target\release\_ecrust.dll` over that
  file before rerunning Python-side checks.
- Plain Windows release builds now default to `mimalloc` because the system
  allocator was materially slower on the HRRR workload. Use
  `cargo build --release --features system-allocator` only for A/B comparison.

Linux / WSL dev note:

- For Linux validation, run benchmarks from the distro filesystem rather than
  `/mnt/c`, and pass explicit `--source` / `--copy-dir` paths to the benchmark
  scripts.
- Non-Windows builds still default to the system allocator. Use
  `maturin develop --release --features mimalloc` or
  `cargo build --release --features mimalloc` only when comparing allocators.
- On Ubuntu 24.04 WSL with Rust `1.94.1`, `maturin develop --release` hit a
  rustc warning-emission ICE. `RUSTFLAGS="-Awarnings" maturin develop --release`
  worked around it.

Verification note:

- The local parity suite now includes focused drop-in API coverage in
  `tests/test_dropin_surface.py`, and the repository includes a Linux/Windows
  GitHub Actions workflow that builds the extension and runs `pytest -q`.
- The parity suite now also includes pinned official operational ECMWF, GFS,
  and RRFS near-surface GRIB2 subsets, assembled from published `.idx` /
  `.index` sidecars via HTTP range requests so CI verifies real model data
  without downloading full upstream files.

Current scope:

- GRIB1 and GRIB2 decode
- modern GRIB metadata and value access
- GRIB2 mutation and write-back for common sample and real-world workflows:
  `codes_set_*`, `codes_get_message`, and `codes_write` now repack common GRIB2
  messages and patch original section metadata instead of only exposing shadow
  state through getters
- expanded real-world parity coverage against the cfgrib sample corpus plus
  pinned official ECMWF/GFS/RRFS operational subsets, with broad modern GRIB
  message compatibility checks and mutation round-trip tests
- pure-Rust decode path, including JPEG2000 and CCSDS/AEC support through Rust crates

Current non-goals:

- BUFR, METAR, and GTS support
- full GRIB write/repack parity across every grid/packing/template combination
- the full long-tail of every historical `eccodes` binding function

Release checklist:

1. Set the final GitHub repository URL in package metadata once the repo is online.
2. Push the branch and confirm `.github/workflows/ci.yml` is green on Linux and Windows.
3. Configure trusted publishing for this repo on TestPyPI and PyPI, or provide the equivalent publish credentials expected by `.github/workflows/release.yml`.
4. Run `.github/workflows/release.yml` with `publish_to=testpypi` for a dry-run package upload.
5. Verify `pip install ecrust` from the published TestPyPI build on Windows and Linux.
6. Push a `vX.Y.Z` tag that matches the package version to build Linux `x86_64` manylinux wheels, Windows `x64` wheels, and an sdist, upload them to the GitHub release, and publish to PyPI.

Known remaining GRIB gap:

- GRIB1 spectral-complex packing (`spherical_harmonics.grib`) is still not implemented
