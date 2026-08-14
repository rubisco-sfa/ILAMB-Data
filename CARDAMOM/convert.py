"""Convert the CARDAMOM Carbon-Water-Energy Reanalysis (ORNL DAAC ds 2492) into
CF-compliant, per-variable netCDF4 files for ILAMB benchmarking.

Source dataset
--------------
Bilir, T.E., A.A. Bloom, N.C. Parazoo, J. Liu, and R.K. Braghiere. 2026.
CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021.
ORNL DAAC, Oak Ridge, Tennessee, USA. https://doi.org/10.3334/ORNLDAAC/2492

A single file, ``CARDAMOM_satellite_constrained_terrestrial_biosphere_reanalysis.nc4``,
holds 319 variables on a 4 deg lat x 5 deg lon global grid, monthly Jan-2001 to
Dec-2021. Most quantities are stored as four ensemble statistics (median, mean,
25th and 75th percentiles); here we extract the *median* member and carry the
25th/75th as CF uncertainty bounds. Fill value is -9999.0.

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

Quality control: masking of non-physical grid cells
---------------------------------------------------
CARDAMOM estimates parameters *and initial conditions* independently at each
4x5 grid cell and makes no steady-state assumption (see the ORNL DAAC user
guide, "Parameter Inference and Implementation"). At a small number of cells the
initial pool sizes are poorly constrained, producing a decaying disequilibrium
transient rather than a physically meaningful state. The clearest case is
42N/80W, where the coarse woody debris pool is initialised at 48180 gC m-2 and
decays to 690 gC m-2 by 2021 -- a factor of 70, against 2.04 for the next-worst
cell; the associated heterotrophic respiration reaches 60 gC m-2 d-1. Every one
of the 34 cell-months in the whole record with rh_co2 > 20 gC m-2 d-1 falls in
2001-2003, which is the signature of an initial condition rather than of a
process. Masking this one cell moves the global-land mean of cSoil by 2.0% and of
NEP by 13.9%.

Separately, DALEC-CWE has no glacier or permanent-ice representation, so snow
accumulates without bound over ice caps (Svalbard, Ellesmere, Devon, Novaya
Zemlya, Baffin and the St Elias/Wrangell/Chugach/Alaska ranges), giving
multi-metre mean snow water equivalent. A distinct failure mode gives soil-water
columns in excess of 50 m at some high-latitude cells that are *not* glaciated.

We therefore mask whole grid cells that breach a fixed, physically justified
bound (``QC_BOUNDS`` below), in the manner of GIMMS_LAI4g (``< 7000``) and NCSCD
(``< 1e-3``). Three independent masks are built -- carbon, water and
temperature -- and each is applied only across its own domain, so that the
carbon budget still closes cell-by-cell (reco = ra + rh, npp = gpp - ra) without
the water bounds needlessly removing good carbon cells. Cells are masked for all time
rather than only in the breaching months, because the diagnosis is a property of
the pixel: at 42N/80W rh_co2 breaches in 32 of 252 months but averages 6.8
gC m-2 d-1 against a global median of 0.41, so the months that pass the bound are
not trustworthy either.

Note that the bounds are evaluated on the *median* member only. The published
25th/75th uncertainty bounds are the raw ensemble interquartile range and are not
filtered, so at retained cells they can exceed these ceilings (see README).

This is deliberately *not* a statistical outlier filter. The apparent hotspots in
gpp, reco, rh, npp, cVeg, ra and et are the right-hand tail of a strongly skewed
but physically real distribution -- their maxima sit at only 1.0-1.5x the 99th
percentile and fall in Borneo, New Guinea and the Congo. Percentile clipping
would delete the wet tropics.
"""

from __future__ import annotations

import datetime
import os
import warnings
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
# The source advertises has_aux_unc=TRUE with aux_uncertainty_id="_25th, _75th";
# these are the lower/upper ensemble bounds we carry as CF uncertainty.
LOWER_TOKENS = ["25th", "q25", "p25", "25", "25pc", "pct25"]
UPPER_TOKENS = ["75th", "q75", "p75", "75", "75pc", "pct75"]

SECONDS_PER_DAY = 86400.0
G_PER_KG = 1000.0
FLUX = 1.0 / (G_PER_KG * SECONDS_PER_DAY)  # g m-2 d-1   -> kg m-2 s-1
WFLUX = 1.0 / SECONDS_PER_DAY              # kg m-2 d-1  -> kg m-2 s-1
POOL = 1.0 / G_PER_KG                      # g m-2       -> kg m-2

# ----------------------------------------------------------------------------
# Quality control: fixed physical ceilings on *source* variables, in source
# units (g m-2 d-1 for carbon fluxes, g m-2 for carbon pools, kg m-2 for water
# stores, kg m-2 d-1 for water fluxes). A cell breaching any bound is masked for
# all time across that bound's domain. See the module docstring for the
# diagnosis; `reduce` selects whether the bound is tested against any single
# month or against the temporal mean.
#   domain: "carbon" or "water" -- selects which output variables are masked.
# ----------------------------------------------------------------------------
QC_BOUNDS = [
    # base,        bound,   reduce,  domain,    rationale
    ("C_som",      1.0e5,   "any",   "carbon",  "soil C > 100 kgC m-2; exceeded only by deep peat"),
    ("C_cwd",      2.0e4,   "any",   "carbon",  "coarse woody debris > 20 kgC m-2"),
    ("rh_co2",     20.0,    "any",   "carbon",  "Rh > 20 gC m-2 d-1 is not sustained by any ecosystem"),
    ("H2O_LY3",    1.0e4,   "any",   "water",   "soil layer 3 > 10 m water column"),
    ("H2O_SWE",    3.0e3,   "mean",  "water",   "mean SWE > 3 m implies a glacier, which DALEC-CWE lacks"),
    ("runoff",     30.0,    "any",   "water",   "runoff > 30 kg m-2 d-1"),
    # Soil temperature. The ceiling is set from the model's own ERA5 forcing:
    # across the whole grid the hottest monthly-mean air temperature is 38.5 C,
    # the hottest monthly T2M_MAX is 46.3 C and the hottest monthly *skin*
    # temperature is 41.8 C. A subsurface layer cannot exceed the surface
    # driving it as a monthly mean, so 50 C leaves ~8 C of headroom over the
    # global skin maximum and is deliberately conservative. It catches 6 cells,
    # every one of which exceeds the global skin maximum by 12-32 C -- including
    # 67.6 C in the Sahara where ERA5 skin temperature that month is 31.8 C.
    ("D_TEMP_LY1", 323.15,  "any",   "temp",    "soil T > 50 C exceeds the hottest skin temperature in the forcing (41.8 C)"),
]

# Source quantities that may legitimately be negative (net exchanges). Everything
# else is a rate or a stock and is clipped at zero on read. This removes the small
# number of negatives the source carries in positive-definite fields: gpp has 15
# (eleven at roundoff ~1e-17, four larger, the biggest -0.0222 gC m-2 d-1) and
# transp_25th has 7 (min -6.6e-4 kg m-2 d-1). Clipping on read rather than on
# write matters: it keeps the derived variables exactly consistent with their
# terms, so npp == gpp - ra still holds at the affected cells.
SIGNED_BASES = {"NBE", "NEP"}

# ----------------------------------------------------------------------------
# The CF `comment` attribute. Caveats a user needs before benchmarking against
# this dataset; see README.md for the full discussion.
# ----------------------------------------------------------------------------
COMMENT_BASE = (
    "Grid cells whose state is non-physical have been masked; see the history "
    "attribute for the exact bounds. These arise where CARDAMOM's per-pixel "
    "initial conditions are poorly constrained (producing a decaying "
    "disequilibrium transient over 2001-2003) or over ice caps, which DALEC-CWE "
    "does not represent. The bounds are evaluated on the median member only; the "
    "25th/75th bounds are the raw ensemble interquartile range and may exceed "
    "them at retained cells. IMPORTANT: CARDAMOM assimilates GRACE/GRACE-FO "
    "water storage, mean runoff (GRUN), satellite GPP and LAI, above- and "
    "below-ground biomass, harmonised soil organic carbon, MODIS snow-covered "
    "fraction, MOPITT-CO-derived fire emissions and CMS-Flux NBE, and is forced "
    "by ERA5 meteorology, atmospheric CO2 and prescribed burned area. "
    "Comparisons against models are therefore NOT independent of the "
    "corresponding ILAMB reference datasets; see README.md for the mapping and "
    "for which variables remain informative."
)
COMMENT = {
    "mrso": COMMENT_BASE + " Additionally, DALEC-CWE's three soil water layers "
    "are per-pixel calibrated stores whose thicknesses are not published, so "
    "although summing them is the correct construction for CMIP mrso, absolute "
    "magnitudes are not comparable to a model with a fixed soil column depth "
    "(the upper tail exceeds any CMIP column capacity). Use tws, which ILAMB's "
    "ConfTWSA de-means before scoring, for a like-for-like comparison.",
    "tws": COMMENT_BASE + " ILAMB's ConfTWSA subtracts the temporal mean from "
    "both reference and model, so the unpublished layer depths do not affect "
    "this comparison. Note that GRACE/GRACE-FO water storage anomalies were "
    "assimilated by CARDAMOM, so this is not an independent check against GRACE.",
    "tsl": COMMENT_BASE + " The soil-temperature ceiling is set from the model's "
    "own ERA5 forcing, whose hottest monthly-mean skin temperature anywhere is "
    "41.8 C; a subsurface layer cannot exceed its own surface forcing as a "
    "monthly mean. Additionally, this is the temperature of DALEC-CWE's first "
    "energy state, whose depth is not published. No depth coordinate is "
    "provided and depth-sensitive analyses (e.g. ILAMB's ConfPermafrost, which "
    "uses dmax=3.5 m) are not supported by this dataset.",
    "nee": COMMENT_BASE + " Additionally, nee is defined here as -NEP, taken "
    "directly from the source. This is not identical to reco - gpp as published "
    "in this collection (they differ by up to 1.7 gC m-2 d-1, median 0.006), and "
    "ILAMB derives the model-side nee as ra + rh - gpp, so a small definitional "
    "offset between reference and model is expected.",
}

# ----------------------------------------------------------------------------
# ILAMB output variable definitions.
#   terms : list of (source_base_name, coefficient) summed together
#   factor: multiplicative unit conversion applied to the (summed) result
#   domain: which QC mask applies -- "carbon", "water", or None for neither
# Sign convention: ILAMB nbp positive = carbon uptake by land; the source NBE is
# net biospheric exchange (positive = flux to the atmosphere), so nbp = -NBE.
#
# Uncertainty bounds are written only for single-term variables. Percentiles are
# not additive, so the 25th percentile of C_fol+C_lab+C_roo+C_woo is not the
# 25th percentile of cVeg; emitting one would misstate the ensemble spread. For a
# single term with a negative coefficient (nbp, nee) the bounds are swapped,
# since negating a distribution exchanges its lower and upper quartiles.
# ----------------------------------------------------------------------------
VARDEFS = {
    # --- carbon fluxes (g m-2 d-1 -> kg m-2 s-1) ------------------------------
    "gpp": dict(terms=[("gpp", 1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
                long_name="Gross Primary Productivity", standard_name="gross_primary_productivity_of_biomass_expressed_as_carbon"),
    "ra": dict(terms=[("resp_auto", 1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
               long_name="Autotrophic Respiration", standard_name="plant_respiration_carbon_flux"),
    "rh": dict(terms=[("rh_co2", 1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
               long_name="Heterotrophic Respiration", standard_name="heterotrophic_respiration_carbon_flux"),
    "reco": dict(terms=[("resp_auto", 1.0), ("rh_co2", 1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
                 long_name="Ecosystem Respiration"),
    "npp": dict(terms=[("gpp", 1.0), ("resp_auto", -1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
                long_name="Net Primary Productivity", standard_name="net_primary_productivity_of_biomass_expressed_as_carbon"),
    "nbp": dict(terms=[("NBE", -1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
                long_name="Net Biospheric Production (land carbon uptake)", standard_name="surface_net_downward_mass_flux_of_carbon_dioxide_expressed_as_carbon_due_to_all_land_processes"),
    "nee": dict(terms=[("NEP", -1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
                long_name="Net Ecosystem Exchange", standard_name="surface_upward_mass_flux_of_carbon_dioxide_expressed_as_carbon_due_to_emission_from_net_ecosystem_productivity"),
    "fFire": dict(terms=[("f_total", 1.0)], factor=FLUX, units="kg m-2 s-1", domain="carbon",
                  long_name="Carbon Mass Flux into Atmosphere Due to CO2 Emission from Fire",
                  standard_name="surface_upward_mass_flux_of_carbon_dioxide_expressed_as_carbon_due_to_emission_from_fires"),
    # --- carbon pools (g m-2 -> kg m-2) --------------------------------------
    "cVeg": dict(terms=[("C_fol", 1.0), ("C_lab", 1.0), ("C_roo", 1.0), ("C_woo", 1.0)],
                 factor=POOL, units="kg m-2", long_name="Carbon Mass in Vegetation", domain="carbon",
                 standard_name="vegetation_carbon_content"),
    "cSoil": dict(terms=[("C_som", 1.0)], factor=POOL, units="kg m-2", domain="carbon",
                  long_name="Carbon Mass in Soil Pool", standard_name="soil_mass_content_of_carbon"),
    "cLitter": dict(terms=[("C_lit", 1.0), ("C_cwd", 1.0)], factor=POOL, units="kg m-2", domain="carbon",
                    long_name="Carbon Mass in Litter Pool (incl. coarse woody debris)",
                    standard_name="litter_carbon_content"),
    # --- water fluxes (kg m-2 d-1 -> kg m-2 s-1) -----------------------------
    "et": dict(terms=[("ets", 1.0)], factor=WFLUX, units="kg m-2 s-1", domain="water",
               long_name="Total Evapotranspiration", standard_name="water_evapotranspiration_flux"),
    "tran": dict(terms=[("transp", 1.0)], factor=WFLUX, units="kg m-2 s-1", domain="water",
                 long_name="Transpiration", standard_name="transpiration_flux"),
    "mrro": dict(terms=[("runoff", 1.0)], factor=WFLUX, units="kg m-2 s-1", domain="water",
                 long_name="Total Runoff", standard_name="runoff_flux"),
    "mrros": dict(terms=[("q_surf", 1.0)], factor=WFLUX, units="kg m-2 s-1", domain="water",
                  long_name="Surface Runoff", standard_name="surface_runoff_flux"),
    # --- water / snow pools (kg m-2) -----------------------------------------
    # NOTE on mrso: DALEC-CWE's three soil water layers are per-pixel calibrated
    # stores whose thicknesses are not published, so while the sum is the correct
    # construction for CMIP mrso (full-column total soil moisture) its absolute
    # magnitude is not comparable to a model with a fixed soil column. Use tws
    # for a like-for-like comparison; see README.
    "mrso": dict(terms=[("H2O_LY1", 1.0), ("H2O_LY2", 1.0), ("H2O_LY3", 1.0)],
                 factor=1.0, units="kg m-2", long_name="Total Soil Moisture Content", domain="water",
                 standard_name="mass_content_of_water_in_soil"),
    # tws is the one water-storage product that is magnitude-independent in ILAMB:
    # ConfTWSA subtracts the temporal mean from both reference and model before
    # scoring, so the unpublished layer depths do not matter.
    "tws": dict(terms=[("H2O_LY1", 1.0), ("H2O_LY2", 1.0), ("H2O_LY3", 1.0), ("H2O_SWE", 1.0)],
                factor=1.0, units="kg m-2", long_name="Terrestrial Water Storage", domain="water"),
    "snw": dict(terms=[("H2O_SWE", 1.0)], factor=1.0, units="kg m-2", domain="water",
                long_name="Surface Snow Amount (snow water equivalent)",
                standard_name="surface_snow_amount"),
    # --- diagnostics ---------------------------------------------------------
    "lai": dict(terms=[("D_LAI", 1.0)], factor=1.0, units="1", domain=None,
                long_name="Leaf Area Index", standard_name="leaf_area_index"),
    # NOTE on tsl: D_TEMP_LY1 is the temperature of DALEC-CWE's first energy
    # state, whose depth is not published. No depth coordinate is written, and
    # depth-sensitive confrontations (ConfPermafrost, dmax=3.5) are unsupported.
    "tsl": dict(terms=[("D_TEMP_LY1", 1.0)], factor=1.0, units="K", domain="temp",
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


def resolve(ds: xr.Dataset, base: str, tokens=None, fallback=True) -> xr.DataArray:
    """Return an ensemble-member DataArray for ``base``.

    ``tokens`` selects which member: the median by default, or ``LOWER_TOKENS`` /
    ``UPPER_TOKENS`` for the 25th/75th percentiles. Handles variables stored
    without an ensemble suffix (e.g. drivers) and those stored as
    ``<base>_<STAT>`` without hard-coding the suffix string. With
    ``fallback=False`` an exact suffix match is required, so a missing quartile
    member raises rather than silently returning the median.
    """
    tokens = MEDIAN_TOKENS if tokens is None else tokens
    if base in ds.data_vars:
        da = ds[base]
    else:
        cands = [v for v in ds.data_vars if v.startswith(base + "_")]
        if not cands:
            raise KeyError(f"no variable matching base '{base}'")

        def rank(v):
            suf = v[len(base) + 1:].lower()
            if suf in tokens:
                return (0, v)
            if any(tok in suf for tok in tokens):
                return (1, v)
            if any(tok in suf for tok in MEAN_TOKENS):
                return (2, v)
            return (3, v)

        best = sorted(cands, key=rank)[0]
        if not fallback and rank(best)[0] > 1:
            raise KeyError(f"no member of '{base}' matching {tokens}")
        da = ds[best]
    da = da.where(da != FILL)
    if base not in SIGNED_BASES:
        da = da.where(da.isnull() | (da >= 0.0), 0.0)
    return da


def build_qc_masks(ds: xr.Dataset):
    """Return ``{domain: 2-D bool mask}`` of cells breaching a QC_BOUNDS ceiling.

    True marks a cell to be removed. Masks are per-domain so that the water
    bounds do not remove otherwise sound carbon cells, and vice versa; see the
    module docstring.
    """
    shape = (ds.sizes["lat"], ds.sizes["lon"])
    masks = {d: np.zeros(shape, bool) for d in {b[3] for b in QC_BOUNDS}}
    detail = {}  # (lat, lon) -> list of reasons, for the run log

    for base, bound, how, domain, why in QC_BOUNDS:
        da = resolve(ds, base)
        arr = da.values
        # ocean columns are all-NaN, so nanmean/nanmax warn on empty slices
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            stat = np.nanmean(arr, axis=0) if how == "mean" else np.nanmax(arr, axis=0)
        bad = np.isfinite(stat) & (stat > bound)
        masks[domain] |= bad
        for i, j in zip(*np.where(bad)):
            key = (float(ds["lat"].values[i]), float(ds["lon"].values[j]))
            detail.setdefault(key, []).append(f"{base} {how}>{bound:g}")

    print("  QC: masking non-physical cells (fixed physical bounds)")
    for (la, lo), reasons in sorted(detail.items()):
        print(f"      lat={la:6.1f} lon={lo:7.1f}  {', '.join(reasons)}")
    nland = int(np.isfinite(resolve(ds, "C_som").values[0]).sum())
    for dom, m in masks.items():
        print(f"      {dom:6s} mask: {int(m.sum()):3d} cells removed of {nland} land cells")
    return masks


def edges(centres: np.ndarray) -> np.ndarray:
    """Cell bounds (n, 2) from evenly spaced cell centres."""
    step = float(np.diff(centres).mean())
    return np.stack([centres - 0.5 * step, centres + 0.5 * step], axis=1)


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
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    references = (
        "@misc{cardamom_cwe_2026,\n"
        "  author = {Bilir, T. E. and Bloom, A. A. and Parazoo, N. C. and Liu, J. and Braghiere, R. K.},\n"
        "  title  = {CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021},\n"
        "  year   = {2026},\n"
        "  publisher = {ORNL DAAC},\n"
        f"  doi = {{{DOI}}}\n"
        "}"
    )

    masks = build_qc_masks(ds)
    qc_rule = "; ".join(
        f"{base} {how}>{bound:g} ({why})" for base, bound, how, _, why in QC_BOUNDS
    )

    for name, d in VARDEFS.items():
        try:
            arr = sum(coef * resolve(ds, base) for base, coef in d["terms"])
        except KeyError as e:
            print(f"  - skip {name}: {e}")
            continue

        # Uncertainty bounds: only meaningful for a single source term, since
        # percentiles are not additive. A negative coefficient reverses the
        # distribution, so the quartiles swap. A missing quartile member costs
        # only the bounds, never the variable itself.
        lo_da = hi_da = None
        if len(d["terms"]) == 1:
            src_base, coef = d["terms"][0]
            try:
                lo = resolve(ds, src_base, LOWER_TOKENS, fallback=False)
                hi = resolve(ds, src_base, UPPER_TOKENS, fallback=False)
                lo_da, hi_da = (coef * lo, coef * hi) if coef > 0 else (coef * hi, coef * lo)
            except KeyError as e:
                print(f"  - {name}: no uncertainty bounds ({e})")

        def finish(da):
            """Unit-convert, orient, and apply the QC mask."""
            da = (da * d["factor"]).astype("float32")
            da = da.rename({latn: "lat", lonn: "lon", timen: "time"}).transpose("time", "lat", "lon")
            if d["domain"] is not None:
                da = da.where(~xr.DataArray(masks[d["domain"]], dims=("lat", "lon")))
            return da

        arr = finish(arr)
        arr.attrs = {k: d[k] for k in ("long_name", "units") if k in d}
        if "standard_name" in d:
            arr.attrs["standard_name"] = d["standard_name"]

        out = arr.to_dataset(name=name)
        if lo_da is not None:
            out[f"{name}_bnds"] = xr.concat([finish(lo_da), finish(hi_da)], dim="nb").transpose(
                "time", "lat", "lon", "nb"
            )
            out[f"{name}_bnds"].attrs = {
                "long_name": f"{d['long_name']} ensemble interquartile range",
                "units": d["units"],
                "comment": "CARDAMOM posterior ensemble 25th and 75th percentiles",
            }
            out[name].attrs["bounds"] = f"{name}_bnds"

        out = out.assign_coords(time=("time", time))
        out["time_bnds"] = xr.DataArray(tbnds, dims=("time", "nb"))

        # ascending latitude, longitude in [-180, 180)
        out = out.sortby("lat")
        if float(out.lon.max()) > 180.0:
            out = out.assign_coords(lon=(((out.lon + 180) % 360) - 180)).sortby("lon")

        # cell bounds, derived after the sort/roll so they match the final axes
        out["lat_bnds"] = xr.DataArray(edges(out["lat"].values), dims=("lat", "nb"))
        out["lon_bnds"] = xr.DataArray(edges(out["lon"].values), dims=("lon", "nb"))

        out["lat"].attrs = {"axis": "Y", "long_name": "latitude",
                            "standard_name": "latitude", "units": "degrees_north",
                            "bounds": "lat_bnds"}
        out["lon"].attrs = {"axis": "X", "long_name": "longitude",
                            "standard_name": "longitude", "units": "degrees_east",
                            "bounds": "lon_bnds"}
        out["time"].attrs = {"axis": "T", "long_name": "time", "standard_name": "time",
                             "bounds": "time_bnds"}
        out["time_bnds"].attrs = {"long_name": "time_bounds"}
        out.attrs = {
            "title": "CARDAMOM Carbon-Water-Energy Reanalysis v1100.1",
            "institution": INSTITUTION,
            "source": f"https://doi.org/{DOI}",
            "history": (
                f"{stamp}: downloaded {SOURCE_FILE} from ORNL DAAC; extracted the median "
                f"ensemble member (25th/75th percentiles retained as CF bounds where the "
                f"variable derives from a single source term); masked grid cells breaching "
                f"fixed physical bounds [{qc_rule}], applied per domain "
                f"(carbon bounds to carbon variables, water bounds to water variables); "
                f"converted to CF-compliant netCDF for ILAMB"
            ),
            "references": references,
            "comment": COMMENT.get(name, COMMENT_BASE),
            "Conventions": "CF-1.11",
        }

        # Declare the axis bounds as coordinates, not data variables, so that on
        # re-open the only data variables are the measurement and its uncertainty
        # -- which is what scripts/validate_dataset.py expects.
        out = out.set_coords(["time_bnds", "lat_bnds", "lon_bnds"])

        path = Path(f"DATA/{name}/{OUT_SOURCE}")
        path.mkdir(parents=True, exist_ok=True)
        enc = {
            name: {"zlib": True, "_FillValue": FILL},
            "time": {"units": "days since 1850-01-01", "calendar": "noleap", "_FillValue": None},
            "time_bnds": {"units": "days since 1850-01-01", "calendar": "noleap", "_FillValue": None},
            # coordinates and their bounds must not carry a fill value
            "lat": {"_FillValue": None}, "lon": {"_FillValue": None},
            "lat_bnds": {"_FillValue": None}, "lon_bnds": {"_FillValue": None},
        }
        if lo_da is not None:
            enc[f"{name}_bnds"] = {"zlib": True, "_FillValue": FILL}
            # suppress the spurious `coordinates = "lat_bnds lon_bnds time_bnds"`
            # xarray would otherwise attach; a bounds variable is not a coordinate
            out[f"{name}_bnds"].encoding["coordinates"] = None
        out.to_netcdf(path / f"{name}.nc", encoding=enc)
        nmask = int(masks[d["domain"]].sum()) if d["domain"] else 0
        print(f"  wrote {path/f'{name}.nc'}  [{d['units']}]"
              f"{'  +unc' if lo_da is not None else '      '}"
              f"  masked {nmask} cells ({d['domain'] or 'none'})")

    ds.close()


if __name__ == "__main__":
    main()
