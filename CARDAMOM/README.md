# CARDAMOM Carbon-Water-Energy Reanalysis (v1100.1)

Conversion of the **CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021**
(ORNL DAAC, [doi:10.3334/ORNLDAAC/2492](https://doi.org/10.3334/ORNLDAAC/2492))
into CF-compliant, per-variable netCDF4 for ILAMB.

> Bilir, T.E., A.A. Bloom, N.C. Parazoo, J. Liu, and R.K. Braghiere. 2026.
> CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021. ORNL DAAC.

Method reference: Bilir et al. 2025, *Satellite-constrained reanalysis reveals CO₂
versus climate process compensation across the global land carbon sink*, AGU
Advances 6:e2025AV001689, [doi:10.1029/2025AV001689](https://doi.org/10.1029/2025AV001689).
Known limitations are discussed in its Section 3.5.

## Source
Single file `CARDAMOM_satellite_constrained_terrestrial_biosphere_reanalysis.nc4`:
4° lat × 5° lon global grid (lat 35, lon 72; 835 land cells), monthly Jan-2001 –
Dec-2021 (252 steps), fill value `-9999`. Most quantities are provided as four
ensemble statistics (`_median`, `_mean`, `_25th`, `_75th`); the conversion extracts
the **median** member (auto-detected, so it is robust to the exact suffix) and
carries the 25th/75th as CF uncertainty bounds.

The underlying model is DALEC-CWE: seven carbon pools, three soil water pools,
three linked energy states, and a snow water equivalent pool. Parameters *and
initial conditions* are estimated independently at each grid cell by DE-MCMC over
99 parameters, with no steady-state assumption. Land pixels are those with ≥25%
land area, excluding Antarctica and Greenland.

`convert.py` downloads the file from the Earthdata-protected ORNL DAAC endpoint
(requires Earthdata Login via `~/.netrc` or a bearer token in `~/.edl_token`),
then writes `DATA/<var>/CARDAMOM/<var>.nc`.

## ⚠️ Not independent of several existing ILAMB reference datasets

CARDAMOM is a model–data fusion product, and the observations it assimilates
overlap with datasets ILAMB already benchmarks against. Comparing a model to
CARDAMOM is therefore **not** an independent check against those references:

| CARDAMOM output | assimilated constraint | overlapping ILAMB reference |
|---|---|---|
| `tws`, `mrso` | GRACE/GRACE-FO water storage anomalies | GRACE (`twsa`) |
| `mrro`, `mrros` | GRUN runoff | Dai, GRDC (`mrro`) |
| `gpp` | satellite GPP (Joiner et al. 2018) | FLUXCOM, WECANN (`gpp`) |
| `lai` | satellite LAI | AVHRR, MODIS (`lai`) |
| `cSoil` | HWSD soil carbon (Hiederer & Köchy 2011) | HWSD, NCSCD (`cSoil`) |
| `cVeg` | above/below-ground biomass | ESACCI, GEOCARBON, XuSaatchi (`cVeg`) |
| `nbp`, `nee` | CMS-Flux NBE (Liu et al. 2021) | GCP, Hoffman (`nbp`) |
| `snw` | MODIS snow-covered fraction | CanSISE (`swe`) — SCF, not SWE, so only indirect |
| `fFire` | fire carbon emissions | GFED (`burntArea`, `fFire`) |

`ra`, `rh`, `reco`, `npp`, `cLitter`, `et`, `tran` and `tsl` are not directly
constrained and are the most informative targets for benchmarking.

## Quality control: masked grid cells

Twenty-one of 835 land cells are masked because their state is non-physical. This
is a **fixed physical-bound filter, not a statistical outlier filter** — see
"On the apparent tropical hotspots" below for why that distinction matters.

Two independent masks are built and each is applied across its own domain, so the
carbon budget still closes cell-by-cell (`reco = ra + rh`, `npp = gpp − ra`)
without the water bounds needlessly discarding sound carbon cells:

| bound (source units) | test | domain | cells | rationale |
|---|---|---|---|---|
| `C_som` > 100 kgC m⁻² | any month | carbon | 2 | exceeded only by deep peat |
| `C_cwd` > 20 kgC m⁻² | any month | carbon | 3 | implausible coarse woody debris |
| `rh_co2` > 20 gC m⁻² d⁻¹ | any month | carbon | 3 | not sustained by any ecosystem |
| `H2O_LY3` > 10 000 kg m⁻² | any month | water | 10 | a >10 m water column |
| `H2O_SWE` > 3 000 kg m⁻² | time mean | water | 7 | >3 m mean SWE implies a glacier |
| `runoff` > 30 kg m⁻² d⁻¹ | any month | water | 2 | both already caught above |

Net effect: **6 cells** removed from the carbon variables (835 → 829), **15** from
the water variables (835 → 820), none from `lai` or `tsl`. `convert.py` prints the
full cell list with reasons on every run.

Cells are masked for all time rather than only in the breaching months, because
the defect is a property of the pixel: at 42°N/80°W `rh_co2` breaches in 32 of 252
months but averages 6.8 gC m⁻² d⁻¹ against a global median of 0.41, so the passing
months are not trustworthy either.

### Why these cells fail

**Poorly constrained initial conditions (carbon).** CARDAMOM estimates initial
pool sizes per pixel with no steady-state assumption, so at a few cells the
initial state is badly determined and the run spends years relaxing away from it.
The clearest case is 42°N/80°W, where `C_cwd` starts at 48 180 gC m⁻² and decays
to 690 gC m⁻² by 2021 — a factor of 70, against 1.9 for the next-worst cell —
driving `rh_co2` to 60 gC m⁻² d⁻¹ and `NEP` to −51 gC m⁻² d⁻¹. This single cell
was the whole of the `cSoil` and `nee` signal reviewers noticed; it alone shifted
global-mean `cSoil` by 3.4% and `NEP` by 13%. The timing is diagnostic: **all 34
months in the entire record with `rh_co2` > 20 gC m⁻² d⁻¹ fall in 2001–2003, none
later.**

**No glacier representation (water).** DALEC-CWE has no glacier or permanent-ice
store, so snow and soil water accumulate without bound over ice caps. Twelve of
the fifteen water cells are glaciated: Svalbard, Ellesmere, Devon, Novaya Zemlya,
Baffin, the St Elias / Wrangell / Chugach / Alaska ranges, Glacier Bay, and the
Andes. Masking them drops `snw` p99 from 2558 to 312 kg m⁻² and `mrso` max from
52 607 to 16 050 kg m⁻².

### On the apparent tropical hotspots

`gpp`, `reco`, `rh`, `npp`, `cVeg`, `ra` and `et` contain no outliers. Their
time-mean maxima sit at only 1.0–1.5× the 99th percentile and fall in Borneo, New
Guinea, Amazonia and the Congo — the most productive land on Earth. These fields
are strongly right-skewed (`cLitter`: median 1.15, p99 12.3 kg m⁻²), so on a
linear colour scale the tropics saturate and the rest of the map flattens, which
reads as a hotspot artifact. **Percentile-based clipping would delete the wet
tropics** and bias every score derived from them, so it is deliberately not used
here. No dataset in ILAMB-Data does statistical outlier rejection; the idiom is a
fixed physical bound with a documented reason (cf. `GIMMS_LAI4g`, `NCSCD`,
`permafrost/Obu2018`).

## Uncertainty

The source advertises `has_aux_unc = TRUE` with `aux_uncertainty_id = "_25th, _75th"`.
Those quartiles are written as `<var>_bnds` and referenced by the `bounds`
attribute, which is how ILAMB reads observational uncertainty
(`ConfUncertainty`, following `DaviesBarnard`).

Bounds are provided **only for the 14 variables that derive from a single source
term**. Percentiles are not additive, so the 25th percentile of
`C_fol + C_lab + C_roo + C_woo` is not the 25th percentile of `cVeg`; emitting one
would misstate the ensemble spread. The six derived sums — `reco`, `npp`, `cVeg`,
`cLitter`, `mrso`, `tws` — therefore carry no bounds. For `nbp` and `nee`, which
are negated source terms, the quartiles are swapped, since negating a distribution
exchanges its lower and upper quartiles.

## Soil water and soil temperature: what is and is not comparable

DALEC-CWE's three soil water layers are **per-pixel calibrated stores whose
thicknesses are not published** — the source file carries no static parameter
fields, and neither the DAAC user guide nor Bilir et al. (2025) gives depths.
Per-pixel LY2/LY1 storage ratios span 0.03–142, so no single global depth exists.
Consequences:

- **`tws` is the like-for-like water product.** ILAMB's `ConfTWSA` subtracts the
  temporal mean from both reference and model before scoring, so unpublished
  depths do not matter. (But note the GRACE circularity above.)
- **`mrso` magnitudes are not model-comparable.** Summing the three layers is the
  correct construction for CMIP `mrso` (full-column total soil moisture) and the
  median, 1369 kg m⁻², is reasonable — but the upper tail still reaches ~11 700
  kg m⁻² after masking, against ~3400 for a CLM5-depth column. Retained with this
  caveat rather than dropped, so users can decide.
- **No `mrsos` is provided.** `H2O_LY1` is roughly 5× the CMIP top-10 cm
  definition, so emitting it would score badly against WangMao for a purely
  definitional reason.
- **No volumetric (m³ m⁻³) soil moisture** can be derived, for the same reason.
- **`tsl` carries no depth coordinate.** It is the temperature of the first energy
  state, whose depth is likewise unpublished. Depth-sensitive analyses — notably
  ILAMB's `ConfPermafrost`, which uses `dmax = 3.5` m — are **not supported**.

## Formatting choices
- CF-1.11, `noleap` calendar, `days since 1850-01-01` with `time_bnds`, plus
  `lat_bnds`/`lon_bnds`. Axis bounds are written as coordinate variables so that
  the only data variables are the measurement and its uncertainty.
- **Native 4×5° grid retained** — the source is coarser than 0.5°, so upsampling
  would fabricate resolution; ILAMB regrids at comparison time.
- Fluxes converted g m⁻² d⁻¹ → kg m⁻² s⁻¹; pools g m⁻² → kg m⁻².
- `_FillValue` is retained on every data variable: ILAMB 2.7 masks on
  `_FillValue`/`missing_value` and has no NaN code path, so a bare NaN would not
  be masked.
- Quantities that are physically non-negative are clipped at zero **on read**,
  which removes the roundoff-level negatives the source carries (`gpp` has 15
  values near −1×10⁻⁵ gC m⁻² d⁻¹) while keeping derived variables exactly
  consistent with their terms. `NBE` and `NEP` are net exchanges and are left
  signed.

## Variable mapping (CARDAMOM → ILAMB/MIP)

`unc` marks variables carrying `<var>_bnds` uncertainty; `QC` gives the mask domain.

| ILAMB | CARDAMOM source | unc | QC | notes |
|-------|-----------------|-----|----|-------|
| gpp   | `gpp` | ✓ | carbon | |
| ra    | `resp_auto` | ✓ | carbon | |
| rh    | `rh_co2` | ✓ | carbon | CO₂ heterotrophic respiration |
| reco  | `resp_auto + rh_co2` | | carbon | |
| npp   | `gpp − resp_auto` | | carbon | |
| nbp   | `−NBE` | ✓ | carbon | sign: land C uptake positive; bounds swapped |
| nee   | `−NEP` | ✓ | carbon | bounds swapped |
| fFire | `f_total` | ✓ | carbon | |
| cVeg  | `C_fol + C_lab + C_roo + C_woo` | | carbon | living biomass |
| cSoil | `C_som` | ✓ | carbon | |
| cLitter | `C_lit + C_cwd` | | carbon | litter + coarse woody debris |
| et    | `ets` | ✓ | water | total evapotranspiration |
| tran  | `transp` | ✓ | water | |
| mrro  | `runoff` | ✓ | water | |
| mrros | `q_surf` | ✓ | water | |
| mrso  | `H2O_LY1 + H2O_LY2 + H2O_LY3` | | water | magnitudes not model-comparable |
| tws   | `H2O_LY1 + H2O_LY2 + H2O_LY3 + H2O_SWE` | | water | use with `ConfTWSA` |
| snw   | `H2O_SWE` | ✓ | water | snow water equivalent |
| lai   | `D_LAI` | ✓ | — | |
| tsl   | `D_TEMP_LY1` | ✓ | — | layer-1 temperature, no depth coordinate |

## Reproducing

```bash
cd CARDAMOM && python convert.py     # ~6 s once the 544 MB source is local
```

All 20 outputs pass `scripts/validate_dataset.py` (requires `pydantic`).
