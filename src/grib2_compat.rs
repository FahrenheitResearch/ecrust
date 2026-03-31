use crate::grib::MessageDescriptor;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Grib2CompatEntry {
    pub short_name: &'static str,
    pub param_id: u32,
    pub name: &'static str,
    pub units: &'static str,
}

pub(crate) fn lookup_grib2_compat_entry(
    descriptor: &MessageDescriptor,
    level_value: f64,
) -> Option<Grib2CompatEntry> {
    let center = descriptor.center?;
    let discipline = descriptor.discipline?;
    let category = descriptor.category?;
    let number = descriptor.parameter_number?;
    let level_type = descriptor.level_type.unwrap_or(255);

    match (
        center,
        discipline,
        category,
        number,
        level_type,
        level_value,
    ) {
        (7, 0, 2, 2, 103, 10.0) => {
            return Some(entry("10u", 165, "10 metre U wind component", "m s**-1"));
        }
        (7, 0, 2, 2, 103, 80.0) => {
            return Some(entry("u", 131, "U component of wind", "m s**-1"));
        }
        (7, 0, 2, 3, 103, 10.0) => {
            return Some(entry("10v", 166, "10 metre V wind component", "m s**-1"));
        }
        (7, 0, 2, 3, 103, 80.0) => {
            return Some(entry("v", 132, "V component of wind", "m s**-1"));
        }
        _ => {}
    }

    match (center, discipline, category, number, level_type) {
        (7, 0, 0, 0, 1) => Some(entry("t", 130, "Temperature", "K")),
        (7, 0, 0, 0, 100) => Some(entry("t", 130, "Temperature", "K")),
        (7, 0, 0, 0, 103) => Some(entry("2t", 167, "2 metre temperature", "K")),
        (7, 0, 0, 2, 103) => Some(entry("pt", 3, "Potential temperature", "K")),
        (7, 0, 0, 6, 100) => Some(entry("dpt", 3017, "Dew point temperature", "K")),
        (7, 0, 0, 6, 103) => Some(entry("2d", 168, "2 metre dewpoint temperature", "K")),
        (7, 0, 0, 10, 1) => Some(entry(
            "slhtf",
            260002,
            "Surface latent heat net flux",
            "W m**-2",
        )),
        (7, 0, 0, 11, 1) => Some(entry(
            "ishf",
            231,
            "Instantaneous surface sensible heat net flux",
            "W m**-2",
        )),
        (7, 0, 1, 0, 100) => Some(entry("q", 133, "Specific humidity", "kg kg**-1")),
        (7, 0, 1, 0, 103) => Some(entry(
            "2sh",
            174096,
            "2 metre specific humidity",
            "kg kg**-1",
        )),
        (7, 0, 1, 1, 4) => Some(entry("r", 157, "Relative humidity", "%")),
        (7, 0, 1, 1, 100) => Some(entry("r", 157, "Relative humidity", "%")),
        (7, 0, 1, 1, 103) => Some(entry("2r", 260242, "2 metre relative humidity", "%")),
        (7, 0, 1, 1, 204) => Some(entry("r", 157, "Relative humidity", "%")),
        (7, 0, 1, 3, 200) => Some(entry("pwat", 3054, "Precipitable water", "kg m**-2")),
        (7, 0, 1, 7, 1) => Some(entry("prate", 3059, "Precipitation rate", "kg m**-2 s**-1")),
        (7, 0, 1, 8, 1) => Some(entry("tp", 228228, "Total Precipitation", "kg m**-2")),
        (7, 0, 1, 11, 1) => Some(entry("sde", 3066, "Snow depth", "m")),
        (7, 0, 1, 13, 1) => Some(entry(
            "sdwe",
            260056,
            "Water equivalent of accumulated snow depth (deprecated)",
            "kg m**-2",
        )),
        (7, 0, 1, 22, 100) => Some(entry("clwmr", 260018, "Cloud mixing ratio", "kg kg**-1")),
        (7, 0, 1, 24, 100) => Some(entry("rwmr", 260020, "Rain mixing ratio", "kg kg**-1")),
        (7, 0, 1, 25, 100) => Some(entry("snmr", 260021, "Snow mixing ratio", "kg kg**-1")),
        (7, 0, 1, 29, 1) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 1, 31, 10) => Some(entry("hail", 260027, "Hail", "m")),
        (7, 0, 1, 31, 104) => Some(entry("hail", 260027, "Hail", "m")),
        (7, 0, 1, 32, 100) => Some(entry("grle", 260028, "Graupel (snow pellets)", "kg kg**-1")),
        (7, 0, 1, 33, 1) => Some(entry(
            "crain",
            260029,
            "Categorical rain",
            "(Code table 4.222)",
        )),
        (7, 0, 1, 34, 1) => Some(entry(
            "cfrzr",
            260030,
            "Categorical freezing rain",
            "(Code table 4.222)",
        )),
        (7, 0, 1, 35, 1) => Some(entry(
            "cicep",
            260031,
            "Categorical ice pellets",
            "(Code table 4.222)",
        )),
        (7, 0, 1, 36, 1) => Some(entry(
            "csnow",
            260032,
            "Categorical snow",
            "(Code table 4.222)",
        )),
        (7, 0, 1, 37, 1) => Some(entry(
            "cpr",
            260033,
            "Convective precipitation rate",
            "kg m**-2 s**-1",
        )),
        (7, 0, 1, 39, 1) => Some(entry("cpofp", 260035, "Percent frozen precipitation", "%")),
        (7, 0, 1, 42, 1) => Some(entry("snowc", 260038, "Snow cover", "%")),
        (7, 0, 1, 74, 200) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 1, 82, 100) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 1, 193, 1) => Some(entry(
            "cfrzr",
            260030,
            "Categorical freezing rain",
            "(Code table 4.222)",
        )),
        (7, 0, 1, 196, 1) => Some(entry(
            "cpr",
            260033,
            "Convective precipitation rate",
            "kg m**-2 s**-1",
        )),
        (7, 0, 1, 225, 1) => Some(entry("frzr", 260288, "Freezing Rain", "kg m**-2")),
        (7, 0, 1, 227, 1) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 1, 242, 10) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 2, 1, 103) => Some(entry(
            "max_10si",
            237207,
            "Time-maximum 10 metre wind speed",
            "m s**-1",
        )),
        (7, 0, 2, 2, 100) => Some(entry("u", 131, "U component of wind", "m s**-1")),
        (7, 0, 2, 3, 100) => Some(entry("v", 132, "V component of wind", "m s**-1")),
        (7, 0, 2, 8, 100) => Some(entry("w", 135, "Vertical velocity", "Pa s**-1")),
        (7, 0, 2, 9, 104) => Some(entry(
            "wz",
            260238,
            "Geometric vertical velocity",
            "m s**-1",
        )),
        (7, 0, 2, 10, 100) => Some(entry("absv", 3041, "Absolute vorticity", "s**-1")),
        (7, 0, 2, 12, 103) => Some(entry(
            "max_vo",
            237138,
            "Time-maximum vorticity (relative)",
            "s**-1",
        )),
        (7, 0, 2, 15, 103) => Some(entry("vucsh", 3045, "Vertical u-component shear", "s**-1")),
        (7, 0, 2, 16, 103) => Some(entry("vvcsh", 3046, "Vertical v-component shear", "s**-1")),
        (7, 0, 2, 22, 1) => Some(entry("gust", 260065, "Wind speed (gust)", "m s**-1")),
        (7, 0, 2, 27, 103) => Some(entry("ustm", 260070, "U-component storm motion", "m s**-1")),
        (7, 0, 2, 28, 103) => Some(entry("vstm", 260071, "V-component storm motion", "m s**-1")),
        (7, 0, 2, 30, 1) => Some(entry("fricv", 260073, "Frictional velocity", "m s**-1")),
        (7, 0, 2, 220, 108) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 2, 221, 108) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 2, 222, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 2, 223, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 3, 0, 1) => Some(entry("sp", 134, "Surface pressure", "Pa")),
        (7, 0, 3, 0, 2) => Some(entry("pcdb", 231045, "Pressure at cloud base", "Pa")),
        (7, 0, 3, 0, 3) => Some(entry("pres", 54, "Pressure", "Pa")),
        (7, 0, 3, 0, 4) => Some(entry("pres", 54, "Pressure", "Pa")),
        (7, 0, 3, 0, 204) => Some(entry("pres", 54, "Pressure", "Pa")),
        (7, 0, 3, 1, 101) => Some(entry("prmsl", 260074, "Pressure reduced to MSL", "Pa")),
        (7, 0, 3, 5, 1) => Some(entry("orog", 228002, "Orography", "m")),
        (7, 0, 3, 5, 2) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 3) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 4) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 5) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 14) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 20) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 100) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 204) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 215) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 5, 247) => Some(entry("gh", 156, "Geopotential height", "gpm")),
        (7, 0, 3, 18, 1) => Some(entry("blh", 159, "Boundary layer height", "m")),
        (7, 0, 3, 198, 101) => Some(entry("mslma", 260323, "MSLP (MAPS System Reduction)", "Pa")),
        (7, 0, 3, 200, 108) => Some(entry(
            "plpl",
            260325,
            "Pressure of level from which parcel was lifted",
            "Pa",
        )),
        (7, 0, 3, 205, 20) => Some(entry("layth", 260330, "Layer Thickness", "m")),
        (7, 0, 4, 7, 1) => Some(entry(
            "sdswrf",
            260087,
            "Surface downward short-wave radiation flux",
            "W m**-2",
        )),
        (7, 0, 4, 8, 1) => Some(entry(
            "suswrf",
            260088,
            "Surface upward short-wave radiation flux",
            "W m**-2",
        )),
        (7, 0, 4, 8, 8) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 4, 199, 1) => Some(entry(
            "cfnsf",
            260345,
            "Cloud Forcing Net Solar Flux",
            "W m**-2",
        )),
        (7, 0, 4, 200, 1) => Some(entry(
            "vbdsf",
            260346,
            "Visible Beam Downward Solar Flux",
            "W m**-2",
        )),
        (7, 0, 4, 201, 1) => Some(entry(
            "vddsf",
            260347,
            "Visible Diffuse Downward Solar Flux",
            "W m**-2",
        )),
        (7, 0, 5, 3, 1) => Some(entry(
            "sdlwrf",
            260097,
            "Surface downward long-wave radiation flux",
            "W m**-2",
        )),
        (7, 0, 5, 4, 1) => Some(entry(
            "sulwrf",
            260098,
            "Surface upward long-wave radiation flux",
            "W m**-2",
        )),
        (7, 0, 5, 4, 8) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 6, 1, 10) => Some(entry("tcc", 228164, "Total Cloud Cover", "%")),
        (7, 0, 6, 3, 214) => Some(entry("lcc", 3073, "Low cloud cover", "%")),
        (7, 0, 6, 4, 224) => Some(entry("mcc", 3074, "Medium cloud cover", "%")),
        (7, 0, 6, 5, 234) => Some(entry("hcc", 3075, "High cloud cover", "%")),
        (7, 0, 7, 6, 1) => Some(entry(
            "cape",
            59,
            "Convective available potential energy",
            "J kg**-1",
        )),
        (7, 0, 7, 6, 103) => Some(entry(
            "cape",
            59,
            "Convective available potential energy",
            "J kg**-1",
        )),
        (7, 0, 7, 6, 108) => Some(entry(
            "cape",
            59,
            "Convective available potential energy",
            "J kg**-1",
        )),
        (7, 0, 7, 7, 1) => Some(entry("cin", 228001, "Convective inhibition", "J kg**-1")),
        (7, 0, 7, 7, 108) => Some(entry("cin", 228001, "Convective inhibition", "J kg**-1")),
        (7, 0, 7, 8, 103) => Some(entry(
            "hlcy",
            260125,
            "Storm relative helicity",
            "m**2 s**-2",
        )),
        (7, 0, 7, 10, 100) => Some(entry("lftx", 260127, "Surface lifted index", "K")),
        (7, 0, 7, 193, 108) => Some(entry("4lftx", 260128, "Best (4-layer) lifted index", "K")),
        (7, 0, 7, 199, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 7, 200, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 7, 204, 1) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 7, 205, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 7, 206, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 15, 3, 10) => Some(entry(
            "veril",
            260136,
            "Vertically-integrated liquid",
            "kg m**-1",
        )),
        (7, 0, 16, 3, 3) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 16, 195, 20) => Some(entry("refd", 260389, "Derived radar reflectivity", "dB")),
        (7, 0, 16, 195, 103) => Some(entry("refd", 260389, "Derived radar reflectivity", "dB")),
        (7, 0, 16, 196, 10) => Some(entry(
            "refc",
            260390,
            "Maximum/Composite radar reflectivity",
            "dB",
        )),
        (7, 0, 16, 198, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 17, 192, 10) => Some(entry("ltng", 260391, "Lightning", "dimensionless")),
        (7, 0, 19, 0, 1) => Some(entry("vis", 3020, "Visibility", "m")),
        (7, 0, 20, 0, 103) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 20, 1, 200) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 0, 20, 102, 200) => Some(entry("unknown", 0, "unknown", "unknown")),
        (7, 1, 0, 5, 1) => Some(entry(
            "bgrun",
            260174,
            "Baseflow-groundwater runoff",
            "kg m**-2",
        )),
        (7, 1, 0, 6, 1) => Some(entry("ssrun", 260175, "Storm surface runoff", "kg m**-2")),
        (7, 2, 0, 0, 1) => Some(entry("lsm", 172, "Land-sea mask", "(0 - 1)")),
        (7, 2, 0, 1, 1) => Some(entry("fsr", 244, "Forecast surface roughness", "m")),
        (7, 2, 0, 2, 106) => Some(entry("st", 228139, "Soil temperature", "K")),
        (7, 2, 0, 10, 1) => Some(entry("gflux", 260186, "Ground heat flux", "W m**-2")),
        (7, 2, 0, 13, 1) => Some(entry(
            "cnwat",
            260189,
            "Plant canopy surface water",
            "kg m**-2",
        )),
        (7, 2, 0, 192, 106) => Some(entry(
            "soilw",
            260185,
            "Volumetric soil moisture content",
            "Proportion",
        )),
        (7, 2, 0, 194, 106) => Some(entry("mstav", 260187, "Moisture availability", "%")),
        (7, 2, 0, 198, 1) => Some(entry("vgtyp", 260439, "Vegetation Type", "Integer(0-13)")),
        (7, 3, 192, 1, 8) => Some(entry(
            "SBT123",
            7001305,
            "Simulated Brightness Temperature for GOES 12, Channel 3",
            "K",
        )),
        (7, 3, 192, 2, 8) => Some(entry(
            "SBT124",
            7001306,
            "Simulated Brightness Temperature for GOES 12, Channel 4",
            "K",
        )),
        (7, 3, 192, 7, 8) => Some(entry(
            "SBT113",
            7001301,
            "Simulated Brightness Temperature for GOES 11, Channel 3",
            "K",
        )),
        (7, 3, 192, 8, 8) => Some(entry(
            "SBT114",
            7001302,
            "Simulated Brightness Temperature for GOES 11, Channel 4",
            "K",
        )),
        (7, 10, 2, 0, 1) => Some(entry("ci", 31, "Sea ice area fraction", "(0 - 1)")),
        (8, 10, 0, 5, 1) => Some(entry(
            "shww",
            140234,
            "Significant height of wind waves",
            "m",
        )),
        (80, 0, 0, 0, 103) => Some(entry("2t", 167, "2 metre temperature", "K")),
        (98, 0, 0, 0, 100) => Some(entry("t", 130, "Temperature", "K")),
        (98, 0, 0, 0, 103) => Some(entry("2t", 167, "2 metre temperature", "K")),
        (98, 0, 0, 0, 105) => Some(entry("t", 130, "Temperature", "K")),
        (98, 0, 2, 2, 103) => Some(entry("10u", 165, "10 metre U wind component", "m s**-1")),
        _ => None,
    }
}

const fn entry(
    short_name: &'static str,
    param_id: u32,
    name: &'static str,
    units: &'static str,
) -> Grib2CompatEntry {
    Grib2CompatEntry {
        short_name,
        param_id,
        name,
        units,
    }
}
