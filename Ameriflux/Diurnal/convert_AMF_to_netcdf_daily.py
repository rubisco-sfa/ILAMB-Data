#!/usr/bin/env python3
"""
Convert AmeriFlux daily CSV files (_DD_) to CF-compliant NetCDF files.

- Only keeps variables that work with daily data:
    GPP (GPP_DT_VUT_REF), PR, TAS
- Radiation, RECO, and NEE variables are skipped
- Output is stored in _output folder
- Site names are extracted from the original CSV filenames
"""

import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path

# ----------------------------
# Configuration
# ----------------------------
RAW_DIR = Path("_raw")
OUTPUT_DIR = Path("_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ----------------------------
# Variable mapping
# (long_name, short_name, CSV column, units)
# ----------------------------
VARMAP = [
    ("gross_primary_productivity", "gpp", "GPP_DT_VUT_REF", "gC m-2 d-1"),
    # ("net_ecosystem_exchange", "nee", "NEE_VUT_REF", "gC m-2 d-1"),  # Not used
    # ("ecosystem_respiration", "reco", "RECO_VUT_REF", "gC m-2 d-1"),  # Not used
    ("precipitation", "pr", "P_F", "mm d-1"),
    ("surface_air_temperature", "tas", "TA_F", "K"),
    # Radiation variables skipped for daily processing
    # ("surface_downward_shortwave_radiation", "rsds", "SW_IN_F", "W m-2"),
    # ("surface_upward_shortwave_radiation", "rsus", "SW_OUT", "W m-2"),
    # ("surface_downward_longwave_radiation", "rlds", "LW_IN_F", "W m-2"),
    # ("surface_upward_longwave_radiation", "rlus", "LW_OUT", "W m-2"),
]

# ----------------------------
# Read all daily CSVs
# ----------------------------
data_by_var = {v[1]: [] for v in VARMAP}
site_names = []
time_index = None

print("Reading daily CSV files...")

for csvfile in sorted(RAW_DIR.glob("*_DD*.csv")):
    # Extract site name from filename
    # If filename is ICs_2025_DD.csv → site = ICs
    # If filename is AMF_ICs_DD_2025.csv → site = ICs
    stem_parts = csvfile.stem.split("_")
    if stem_parts[0] == "AMF":   # if there's an "AMF_" prefix
        site = stem_parts[1]
    else:
        site = stem_parts[0]

    print(f"  → {site}")

    df = pd.read_csv(csvfile)

    # ----------------------------
    # Time parsing
    # ----------------------------
    if "TIMESTAMP" in df.columns:
        df["time"] = pd.to_datetime(df["TIMESTAMP"], format="%Y%m%d")
    elif "TIMESTAMP_START" in df.columns:
        df["time"] = pd.to_datetime(df["TIMESTAMP_START"], format="%Y%m%d")
    else:
        raise ValueError(f"No recognizable timestamp column in {csvfile.name}")

    df = df.set_index("time").sort_index()

    if time_index is None:
        time_index = df.index

    # ----------------------------
    # Extract variables
    # ----------------------------
    for long_name, short, amf_col, units in VARMAP:
        if amf_col not in df.columns:
            print(f"    ⚠️  Missing {amf_col} at {site}, filling with NaNs")
            values = np.full(len(time_index), np.nan)
        else:
            values = df.reindex(time_index)[amf_col].to_numpy()
        data_by_var[short].append(values)

    site_names.append(site)

# ----------------------------
# Create NetCDF files by variable
# ----------------------------
print("\nCreating NetCDF files in _output/ ...")
for long_name, short, amf_col, units in VARMAP:
    data_array = np.vstack(data_by_var[short]).T  # shape: (time, site)

    da = xr.DataArray(
        data_array,
        dims=("time", "site"),
        coords={
            "time": time_index,
            "site": site_names,
        },
        name=short,
        attrs={
            "long_name": long_name,
            "units": units,
            "source": "AmeriFlux daily _DD_ CSV",
            "product": "VUT_REF" if "VUT" in amf_col else "standard",
        },
    )

    ds = da.to_dataset()

    outfile = OUTPUT_DIR / f"{short}_daily.nc"
    ds.to_netcdf(outfile)
    print(f"  ✔ Saved {outfile}")

print("\nAll done!")

