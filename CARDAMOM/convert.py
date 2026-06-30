"""Convert the CARDAMOM Carbon-Water-Energy Reanalysis (ORNL DAAC ds 2492) into
CF-compliant, per-variable netCDF4 files for ILAMB benchmarking.

Source dataset
--------------
Bilir, T.E., A.A. Bloom, N.C. Parazoo, J. Liu, and R.K. Braghiere. 2026.
CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021.
ORNL DAAC, Oak Ridge, Tennessee, USA. https://doi.org/10.3334/ORNLDAAC/2492

A single file, ``CARDAMOM_satellite_constrained_terrestrial_biosphere_reanalysis.nc4``,
holds 315 variables on a 4 deg lat x 5 deg lon global grid, monthly Jan-2001 to
Dec-2021. Most quantities are stored as four ensemble statistics (median, mean,
25th and 75th percentiles); here we extract the *median* member. Fill value is
-9999.0.

Formatting choices (per ILAMB-Data guidelines)
----------------------------------------------
* CF-1.11 metadata, noleap calendar, ``days since 1850-01-01`` with time bounds.
* Native 4x5 grid is kept -- the source is coarser than 0.5 deg, so upsampling
  would fabricate resolution; ILAMB regrids internally at comparison time.
* Variable names/units follow CMIP/MIP conventions (gpp, nbp, cVeg, ... in
  kg m-2 s-1 for fluxes and kg m-2 for pools).

The ensemble-statistic suffix used by the source file is auto-detected, so this
script does not hard-code whether the median member is e.g. ``GPP_MEDIAN`` or
``GPP_MED``.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import cftime
import numpy as np
import xarray as xr

# ----------------------------------------------------------------------------
# configuration
# ----------------------------------------------------------------------------
DOI = "10.3334/ORNLDAAC/2492"
SOURCE_FILE = "CARDAMOM_satellite_constrained_terrestrial_biosphere_reanalysis.nc4"
# Canonical Earthdata Cloud HTTPS endpoint (from CMR collection
# C4219648699-ORNL_CLOUD); requires a valid Earthdata Login.
REMOTE_URL = (
    "https://data.ornldaac.earthdata.nasa.gov/protected/cms/"
    "CARDAMOM_Gridded_Fluxes/data/" + SOURCE_FILE
)
INSTITUTION = "Jet Propulsion Laboratory, California Institute of Technology"
FILL = -9999.0
OUT_SOURCE = "CARDAMOM"  # sub-directory label inside DATA/<var>/

# Tokens (lower-case) that identify the ensemble member we want, best first.
MEDIAN_TOKENS = ["median", "med", "p50", "q50", "50", "50pc", "pct50"]
MEAN_TOKENS = ["mean", "avg"]

SECONDS_PER_DAY = 86400.0
G_PER_KG = 1000.0
FLUX = 1.0 / (G_PER_KG * SECONDS_PER_DAY)  # g m-2 d-1   -> kg m-2 s-1
WFLUX = 1.0 / SECONDS_PER_DAY              # kg m-2 d-1  -> kg m-2 s-1
POOL = 1.0 / G_PER_KG                      # g m-2       -> kg m-2

# ----------------------------------------------------------------------------
# ILAMB output variable definitions.
#   terms : list of (source_base_name, coefficient) summed together
#   factor: multiplicative unit conversion applied to the (summed) result
# Sign convention: ILAMB nbp positive = carbon uptake by land; the source NBE is
# net biospheric exchange (positive = flux to the atmosphere), so nbp = -NBE.
# ----------------------------------------------------------------------------
VARDEFS = {
    # --- carbon fluxes (g m-2 d-1 -> kg m-2 s-1) ------------------------------
    "gpp": dict(terms=[("gpp", 1.0)], factor=FLUX, units="kg m-2 s-1",
                long_name="Gross Primary Productivity", standard_name="gross_primary_productivity_of_biomass_expressed_as_carbon"),
    "ra": dict(terms=[("resp_auto", 1.0)], factor=FLUX, units="kg m-2 s-1",
               long_name="Autotrophic Respiration", standard_name="plant_respiration_carbon_flux"),
    "rh": dict(terms=[("rh_co2", 1.0)], factor=FLUX, units="kg m-2 s-1",
               long_name="Heterotrophic Respiration", standard_name="heterotrophic_respiration_carbon_flux"),
    "reco": dict(terms=[("resp_auto", 1.0), ("rh_co2", 1.0)], factor=FLUX, units="kg m-2 s-1",
                 long_name="Ecosystem Respiration"),
    "npp": dict(terms=[("gpp", 1.0), ("resp_auto", -1.0)], factor=FLUX, units="kg m-2 s-1",
                long_name="Net Primary Productivity", standard_name="net_primary_productivity_of_biomass_expressed_as_carbon"),
    "nbp": dict(terms=[("NBE", -1.0)], factor=FLUX, units="kg m-2 s-1",
                long_name="Net Biospheric Production (land carbon uptake)", standard_name="surface_net_downward_mass_flux_of_carbon_dioxide_expressed_as_carbon_due_to_all_land_processes"),
    "nee": dict(terms=[("NEP", -1.0)], factor=FLUX, units="kg m-2 s-1",
                long_name="Net Ecosystem Exchange", standard_name="surface_upward_mass_flux_of_carbon_dioxide_expressed_as_carbon_due_to_emission_from_net_ecosystem_productivity"),
    "fFire": dict(terms=[("f_total", 1.0)], factor=FLUX, units="kg m-2 s-1",
                  long_name="Carbon Mass Flux into Atmosphere Due to CO2 Emission from Fire",
                  standard_name="surface_upward_mass_flux_of_carbon_dioxide_expressed_as_carbon_due_to_emission_from_fires"),
    # --- carbon pools (g m-2 -> kg m-2) --------------------------------------
    "cVeg": dict(terms=[("C_fol", 1.0), ("C_lab", 1.0), ("C_roo", 1.0), ("C_woo", 1.0)],
                 factor=POOL, units="kg m-2", long_name="Carbon Mass in Vegetation",
                 standard_name="vegetation_carbon_content"),
    "cSoil": dict(terms=[("C_som", 1.0)], factor=POOL, units="kg m-2",
                  long_name="Carbon Mass in Soil Pool", standard_name="soil_carbon_content"),
    "cLitter": dict(terms=[("C_lit", 1.0), ("C_cwd", 1.0)], factor=POOL, units="kg m-2",
                    long_name="Carbon Mass in Litter Pool (incl. coarse woody debris)",
                    standard_name="litter_carbon_content"),
    # --- water fluxes (kg m-2 d-1 -> kg m-2 s-1) -----------------------------
    "et": dict(terms=[("ets", 1.0)], factor=WFLUX, units="kg m-2 s-1",
               long_name="Total Evapotranspiration", standard_name="water_evapotranspiration_flux"),
    "tran": dict(terms=[("transp", 1.0)], factor=WFLUX, units="kg m-2 s-1",
                 long_name="Transpiration", standard_name="transpiration_flux"),
    "mrro": dict(terms=[("runoff", 1.0)], factor=WFLUX, units="kg m-2 s-1",
                 long_name="Total Runoff", standard_name="runoff_flux"),
    "mrros": dict(terms=[("q_surf", 1.0)], factor=WFLUX, units="kg m-2 s-1",
                  long_name="Surface Runoff", standard_name="surface_runoff_flux"),
    # --- water / snow pools (kg m-2) -----------------------------------------
    "mrso": dict(terms=[("H2O_LY1", 1.0), ("H2O_LY2", 1.0), ("H2O_LY3", 1.0)],
                 factor=1.0, units="kg m-2", long_name="Total Soil Moisture Content",
                 standard_name="soil_moisture_content"),
    "snw": dict(terms=[("H2O_SWE", 1.0)], factor=1.0, units="kg m-2",
                long_name="Surface Snow Amount (snow water equivalent)",
                standard_name="surface_snow_amount"),
    # --- diagnostics ---------------------------------------------------------
    "lai": dict(terms=[("D_LAI", 1.0)], factor=1.0, units="1",
                long_name="Leaf Area Index", standard_name="leaf_area_index"),
    "tsl": dict(terms=[("D_TEMP_LY1", 1.0)], factor=1.0, units="K",
                long_name="Temperature of Soil (layer 1)", standard_name="soil_temperature"),
}


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def download(local: str = SOURCE_FILE) -> str:
    """Fetch the source file from ORNL DAAC if not already present.

    Requires Earthdata Login credentials in ``~/.netrc`` and that the ORNL DAAC
    application is authorized in your Earthdata profile.
    """
    if os.path.isfile(local):
        return local
    print(f"Downloading {REMOTE_URL}")
    cj = os.path.expanduser("~/.urs_cookies")
    rc = os.system(
        f"curl -L --location-trusted -n -c {cj} -b {cj} -o {local} '{REMOTE_URL}'"
    )
    if rc != 0 or not os.path.isfile(local):
        raise RuntimeError(
            "Download failed. Ensure ~/.netrc has urs.earthdata.nasa.gov "
            "credentials and that the ORNL DAAC app is approved in your "
            "Earthdata profile, or download the file manually from "
            f"https://doi.org/{DOI}"
        )
    return local


def find_coord(ds: xr.Dataset, candidates) -> str:
    for c in candidates:
        if c in ds.variables:
            return c
    raise KeyError(f"none of {candidates} found in dataset")


def resolve(ds: xr.Dataset, base: str) -> xr.DataArray:
    """Return the median-ensemble DataArray for ``base``.

    Handles variables stored without an ensemble suffix (e.g. drivers) and those
    stored as ``<base>_<STAT>`` without hard-coding the suffix string.
    """
    if base in ds.data_vars:
        da = ds[base]
    else:
        cands = [v for v in ds.data_vars if v.startswith(base + "_")]
        if not cands:
            raise KeyError(f"no variable matching base '{base}'")

        def rank(v):
            suf = v[len(base) + 1:].lower()
            if suf in MEDIAN_TOKENS:
                return (0, v)
            if any(tok in suf for tok in MEDIAN_TOKENS):
                return (1, v)
            if any(tok in suf for tok in MEAN_TOKENS):
                return (2, v)
            return (3, v)

        da = ds[sorted(cands, key=rank)[0]]
    return da.where(da != FILL)


def monthly_noleap(n: int, year0: int = 2001, month0: int = 1):
    """Mid-month noleap time axis plus month-start/-end bounds for ``n`` months."""
    times, lo, hi = [], [], []
    y, m = year0, month0
    for _ in range(n):
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        lo.append(cftime.DatetimeNoLeap(y, m, 1))
        hi.append(cftime.DatetimeNoLeap(ny, nm, 1))
        # mid-month (noleap month length)
        mid = cftime.DatetimeNoLeap(y, m, 1) + (cftime.DatetimeNoLeap(ny, nm, 1) - cftime.DatetimeNoLeap(y, m, 1)) / 2
        times.append(mid)
        y, m = ny, nm
    bnds = np.array([lo, hi]).T
    return np.array(times), bnds


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main(source: str = SOURCE_FILE):
    source = download(source)
    ds = xr.open_dataset(source, decode_times=False, mask_and_scale=False)

    latn = find_coord(ds, ["latitude", "lat", "y"])
    lonn = find_coord(ds, ["longitude", "lon", "x"])
    timen = find_coord(ds, ["time", "month", "t"])
    nt = ds.sizes[timen]

    time, tbnds = monthly_noleap(nt)
    download_stamp = time_str = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    references = (
        "@misc{cardamom_cwe_2026,\n"
        "  author = {Bilir, T. E. and Bloom, A. A. and Parazoo, N. C. and Liu, J. and Braghiere, R. K.},\n"
        "  title  = {CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021},\n"
        "  year   = {2026},\n"
        "  publisher = {ORNL DAAC},\n"
        f"  doi = {{{DOI}}}\n"
        "}"
    )

    for name, d in VARDEFS.items():
        try:
            arr = sum(coef * resolve(ds, base) for base, coef in d["terms"])
        except KeyError as e:
            print(f"  - skip {name}: {e}")
            continue

        arr = (arr * d["factor"]).astype("float32")
        arr = arr.rename({latn: "lat", lonn: "lon", timen: "time"})
        arr = arr.transpose("time", "lat", "lon")
        arr.attrs = {k: d[k] for k in ("long_name", "units") if k in d}
        if "standard_name" in d:
            arr.attrs["standard_name"] = d["standard_name"]

        out = arr.to_dataset(name=name)
        out = out.assign_coords(time=("time", time))
        out["time_bnds"] = xr.DataArray(tbnds, dims=("time", "nb"))

        # ascending latitude, longitude in [-180, 180)
        out = out.sortby("lat")
        if float(out.lon.max()) > 180.0:
            out = out.assign_coords(lon=(((out.lon + 180) % 360) - 180)).sortby("lon")

        out["lat"].attrs = {"axis": "Y", "long_name": "latitude", "units": "degrees_north"}
        out["lon"].attrs = {"axis": "X", "long_name": "longitude", "units": "degrees_east"}
        # note: 'bounds' is set via encoding below, not attrs (xarray manages it)
        out["time"].attrs = {"axis": "T", "long_name": "time"}
        out.attrs = {
            "title": "CARDAMOM Carbon-Water-Energy Reanalysis v1100.1",
            "institution": INSTITUTION,
            "source": f"https://doi.org/{DOI}",
            "history": (
                f"{download_stamp}: downloaded {SOURCE_FILE} from ORNL DAAC; "
                f"extracted median ensemble member; converted to CF-compliant "
                f"netCDF for ILAMB"
            ),
            "references": references,
            "Conventions": "CF-1.11",
        }

        path = Path(f"DATA/{name}/{OUT_SOURCE}")
        path.mkdir(parents=True, exist_ok=True)
        enc = {
            name: {"zlib": True, "_FillValue": FILL},
            "time": {"units": "days since 1850-01-01", "calendar": "noleap", "bounds": "time_bnds"},
            "time_bnds": {"units": "days since 1850-01-01", "calendar": "noleap"},
        }
        out.to_netcdf(path / f"{name}.nc", encoding=enc)
        print(f"  wrote {path/f'{name}.nc'}  [{d['units']}]")

    ds.close()


if __name__ == "__main__":
    main()
