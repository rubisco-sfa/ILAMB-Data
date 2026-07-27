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

CARDAMOM is a model–data fusion product. The observations it assimilates overlap
with datasets ILAMB already benchmarks against, so comparing a model to CARDAMOM
is **not** an independent check against those references. ILAMB names below are
the section names in `src/ILAMB/data/ilamb.cfg`, verified against that file.

| CARDAMOM output | assimilated constraint | ILAMB confrontation (variable) | overlap |
|---|---|---|---|
| `tws`, `mrso` | GRACE/GRACE-FO TWS anomalies (Wiese et al. 2016) | GRACE (`twsa`, alt `tws`) | **direct** |
| `cVeg` | above/below-ground biomass, incl. **Xu, Saatchi et al. 2021** | XuSaatchi2021, ESACCI, GEOCARBON, NBCD2000, Saatchi2011, Thurner, USForest (`biomass`, alt `cVeg`) | **same dataset** as XuSaatchi2021 |
| `nbp`, `nee` | CMS-Flux NBE (Liu et al. 2021) | GCP, Hoffman (`nbp`) | **direct** |
| `gpp` | reflectance-based GPP (Joiner et al. 2018) | FLUXCOM, WECANN, FLUXNET2015 (`gpp`) | direct |
| `lai` | MODIS/Aqua LAI (Myneni et al. 2015) | AVHRR, AVH15C1, MODIS (`lai`) | direct |
| `cSoil` | harmonised SOC (Hiederer & Köchy 2011, i.e. HWSD) | HWSD, NCSCDV22, Koven (`cSoilAbove1m`, alt `cSoil`) | direct; note ILAMB scores a top-1 m quantity |
| `snw` | MODIS/Terra snow-covered **fraction** (Hall & Riggs 2016) | CanSISE (`swe`, alt `snw`) | indirect — SCF, not SWE |
| `mrro`, `mrros` | **mean** runoff only (GRUN, Ghiggi et al. 2019) | Dai, LORA, CLASS (`runoff`, alt `mrro`) | mean only — seasonality and IAV stay informative |
| `fFire` | fire C emissions from **MOPITT CO** (Jiang et al. 2017) | *no `fFire` confrontation exists in the stock config* | — |
| — | `Driver_BURNED_AREA` is a prescribed **forcing** | GFED4.1S (`burntArea`) | **circular by construction** |
| all | ERA5 meteorology and atmospheric CO₂ (forcings) | ERA5 and others under `[h1: Forcings]` | forcing-side |

**Only `ra`, `rh`, `cLitter` and `tsl` are defensibly independent.** Four
variables that look independent are not:

- **`reco`** is pinned by the assimilation: `reco = gpp − NEP` holds to a median
  absolute difference of 0.0059 gC m⁻² d⁻¹ against a typical `reco` of 1.25
  (0.47%), and both `gpp` and `NEP` are constrained.
- **`npp`** = `gpp − ra` inherits the `gpp` constraint.
- **`et`** is effectively the water-balance residual of ERA5 precipitation
  (forcing), GRUN-constrained runoff and GRACE-constrained storage:
  corr(time-mean ET, P−Q) = **0.9968**, and the long-term balance closes to
  2 mm yr⁻¹ out of ~555. Benchmarking it largely re-tests ERA5 − GRUN.
- **`tran`** inherits the same ET balance.

`ra` and `rh` are pinned only *jointly* (through `reco`), so individually they
remain useful targets.

## Quality control: masked grid cells

Twenty-seven of 835 land cells are masked because their state is non-physical.
This is a **fixed physical-bound filter, not a statistical outlier filter** — see
"On the apparent tropical hotspots" below for why that distinction matters.

Three independent masks are built and each is applied across its own domain, so
the carbon budget still closes cell-by-cell (`reco = ra + rh`, `npp = gpp − ra`)
without the water or temperature bounds needlessly discarding sound carbon cells:

| bound (source units) | test | domain | cells | rationale |
|---|---|---|---|---|
| `C_som` > 100 kgC m⁻² | any month | carbon | 2 | exceeded only by deep peat |
| `C_cwd` > 20 kgC m⁻² | any month | carbon | 3 | implausible coarse woody debris |
| `rh_co2` > 20 gC m⁻² d⁻¹ | any month | carbon | 3 | not sustained by any ecosystem |
| `H2O_LY3` > 10 000 kg m⁻² | any month | water | 9 | a >10 m water column |
| `H2O_SWE` > 3 000 kg m⁻² | time mean | water | 7 | >3 m mean SWE implies a glacier |
| `runoff` > 30 kg m⁻² d⁻¹ | any month | water | 2 | both already caught above |
| `D_TEMP_LY1` > 50 °C | any month | temp | 6 | exceeds the hottest skin temperature in the forcing |

Net effect: **6 cells** removed from the carbon variables (835 → 829), **15** from
the water variables (835 → 820) and **6** from `tsl` (835 → 829); `lai` is
unmasked. `convert.py` prints the full cell list with reasons on every run.

Cells are masked for all time rather than only in the breaching months, because
the defect is a property of the pixel: at 42°N/80°W `rh_co2` breaches in 32 of 252
months but averages 6.8 gC m⁻² d⁻¹ against a global median of 0.41, so the passing
months are not trustworthy either.

### Why these cells fail

**Poorly constrained initial conditions (carbon).** CARDAMOM estimates initial
pool sizes per pixel with no steady-state assumption, so at a few cells the
initial state is badly determined and the run spends years relaxing away from it.
The clearest case is 42°N/80°W, where `C_cwd` starts at 48 180 gC m⁻² and decays
to 690 gC m⁻² by 2021 — a factor of 70, against 2.04 for the next-worst cell —
driving `rh_co2` to 60 gC m⁻² d⁻¹ and `NEP` to −51 gC m⁻² d⁻¹. This single cell
was the whole of the `cSoil` and `nee` signal reviewers noticed; masking it alone
moves global-mean `cSoil` by 2.0% and `NEP` by 13.9% (the full six-cell carbon
mask moves `cSoil` by 3.3%). The timing is diagnostic: **all 34 cell-months in the
entire record with `rh_co2` > 20 gC m⁻² d⁻¹ fall in 2001–2003, none later.**

**Two distinct water failure modes.** The seven cells caught by `H2O_SWE` are
glaciated, and DALEC-CWE has no glacier or permanent-ice store, so snow
accumulates without bound: Baffin, Devon, Novaya Zemlya, Ellesmere (×2) and
Svalbard (×2). The nine caught by `H2O_LY3` are a *different* problem — runaway
deep soil water — and only four of them are glaciated (Glacier Bay, Alaska Range,
Chugach, St Elias, plus Svalbard which is caught by both). **11 of the 15 water
cells are glaciated, not all of them.** In particular (−30, −65) is the Sierras
Pampeanas of Argentina, not the glaciated Central Andes (which lie near 70°W, in
an unmasked cell); its mean SWE is 2.7 kg m⁻², so the glacier explanation does not
apply to it at all. Masking drops `snw` p99 from 2558 to 312 kg m⁻² and `mrso` max
from 52 607 to 16 050 kg m⁻².

**A glaciated cell survives.** (70°N, 75°W) has a mean SWE of 2 915 kg m⁻² and a
peak of 3 851 — a glacier by the criterion above — but sits 2.9% under the 3 000
bound and is retained. It is the largest remaining `snw` value.

### On the apparent tropical hotspots

`gpp`, `reco`, `rh`, `npp`, `cVeg`, `ra` and `et` contain no outliers. Their
time-mean maxima sit at only 1.0–1.5× the 99th percentile (`ra` is the largest at
1.52×) and fall in Borneo, New Guinea and the Congo — the most productive land on
Earth. These fields are strongly right-skewed (`cLitter`: median 1.13, p99
11.6 kg m⁻²), so on a
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

**The bounds are not quality-controlled.** `QC_BOUNDS` is evaluated on the median
member only, so at retained cells the published interquartile range can exceed
those ceilings — the `rh` upper bound reaches 73.7 gC m⁻² d⁻¹ (against a 20 bound)
in 5 cell-months, `cSoil` reaches 115.5 kg m⁻² (against 100), and `snw` reaches
4064 kg m⁻² (against 3000). This is the raw posterior spread and is left
unfiltered deliberately, but it means the bounds should not be read as
physically-screened envelopes. Whether cells should instead be masked when their
*quartiles* breach is an open question for the data producers.

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
  median, 1367 kg m⁻², is reasonable — but the upper tail still reaches ~11 200
  kg m⁻² after masking, against ~3400 for a CLM5-depth column. Retained with this
  caveat rather than dropped, so users can decide.
- **No `mrsos` is provided.** `H2O_LY1` is roughly 5× the CMIP top-10 cm
  definition, so emitting it would score badly against WangMao for a purely
  definitional reason.
- **No volumetric (m³ m⁻³) soil moisture** can be derived, for the same reason.
- **`tsl` carries no depth coordinate.** It is the temperature of the first energy
  state, whose depth is likewise unpublished. Depth-sensitive analyses — notably
  ILAMB's `ConfPermafrost`, which uses `dmax = 3.5` m — are **not supported**.
- **`tsl` is bounded at 50 °C, which removes 6 cells.** The ceiling comes from
  the model's own ERA5 forcing: across the whole grid the hottest monthly-mean
  air temperature is 38.5 °C, the hottest `T2M_MAX` is 46.3 °C and the hottest
  monthly **skin** temperature is 41.8 °C. A subsurface layer cannot exceed the
  surface driving it as a monthly mean, so anything above 50 °C is unphysical
  regardless of climate zone. Every masked cell breaks that by a wide margin —
  the Sahara cell (18°N, 5°W) reaches 67.6 °C in a month when ERA5 skin
  temperature there is 31.8 °C, and (66°N, 155°E) reaches 73.4 °C with a
  long-term mean of −3.7 °C. Note this is a *monthly mean*: the familiar 70 °C
  desert readings are instantaneous midday skin values and are not comparable.

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
  which removes the small number of negatives the source carries in
  positive-definite fields (`gpp` has 15: eleven at roundoff ~1×10⁻¹⁷, four
  larger, the biggest −0.0222 gC m⁻² d⁻¹) while keeping derived variables exactly
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
