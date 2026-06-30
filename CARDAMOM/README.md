# CARDAMOM Carbon-Water-Energy Reanalysis (v1100.1)

Conversion of the **CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021**
(ORNL DAAC, [doi:10.3334/ORNLDAAC/2492](https://doi.org/10.3334/ORNLDAAC/2492))
into CF-compliant, per-variable netCDF4 for ILAMB.

> Bilir, T.E., A.A. Bloom, N.C. Parazoo, J. Liu, and R.K. Braghiere. 2026.
> CARDAMOM Carbon-Water-Energy Reanalysis v1100.1, 2001-2021. ORNL DAAC.

## Source
Single file `CARDAMOM_satellite_constrained_terrestrial_biosphere_reanalysis.nc4`:
4° lat × 5° lon global grid (lat 35, lon 72), monthly Jan-2001 – Dec-2021 (252
steps), fill value `-9999`. Most quantities are provided as four ensemble
statistics (`_median`, `_mean`, `_25th`, `_75th`); the conversion extracts the
**median** member (auto-detected, so it is robust to the exact suffix).

`convert.py` downloads the file from the Earthdata-protected ORNL DAAC endpoint
(requires Earthdata Login via `~/.netrc` or a bearer token in `~/.edl_token`),
then writes `DATA/<var>/CARDAMOM/<var>.nc`.

## Formatting choices
- CF-1.11, `noleap` calendar, `days since 1850-01-01` with `time_bnds`.
- **Native 4×5° grid retained** — the source is coarser than 0.5°, so upsampling
  would fabricate resolution; ILAMB regrids at comparison time.
- Fluxes converted g m⁻² d⁻¹ → kg m⁻² s⁻¹; pools g m⁻² → kg m⁻².

## Variable mapping (CARDAMOM → ILAMB/MIP)
| ILAMB | CARDAMOM source | notes |
|-------|-----------------|-------|
| gpp   | `gpp` | |
| ra    | `resp_auto` | |
| rh    | `rh_co2` | CO₂ heterotrophic respiration |
| reco  | `resp_auto + rh_co2` | |
| npp   | `gpp − resp_auto` | |
| nbp   | `−NBE` | sign: land C uptake positive |
| nee   | `−NEP` | |
| fFire | `f_total` | |
| cVeg  | `C_fol + C_lab + C_roo + C_woo` | living biomass |
| cSoil | `C_som` | |
| cLitter | `C_lit + C_cwd` | litter + coarse woody debris |
| et    | `ets` | total evapotranspiration |
| tran  | `transp` | |
| mrro  | `runoff` | |
| mrros | `q_surf` | |
| mrso  | `H2O_LY1 + H2O_LY2 + H2O_LY3` | |
| snw   | `H2O_SWE` | snow water equivalent |
| lai   | `D_LAI` | |
| tsl   | `D_TEMP_LY1` | soil-layer-1 temperature |
