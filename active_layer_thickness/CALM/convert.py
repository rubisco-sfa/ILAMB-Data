import os
import time

import cftime as cf
import numpy as np
import pandas as pd
import xarray as xr

# Get data
remote_source = "https://www2.gwu.edu/~calm/data/CALM_Data/CALM_Summary_table.xls"
local_source = os.path.basename(remote_source)
if not os.path.isfile(local_source):
    os.system(f"wget {remote_source}")

# Timestamps
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# Find a way to search the excel sheet for these
dfs = []
for first, last in zip(
    [
        31,
        76,
        103,
        108,
        147,
        156,
        179,
        196,
        225,
        234,
        241,
        247,
        251,
        258,
        265,
        270,
        286,
        336,
        350,
    ],
    [
        72,
        99,
        104,
        142,
        152,
        175,
        192,
        221,
        230,
        236,
        243,
        247,
        254,
        261,
        266,
        281,
        332,
        346,
        352,
    ],
):
    dfs.append(pd.read_excel(local_source, skiprows=first - 2, nrows=last - first + 1))
df = pd.concat(dfs)

# Fix some column names
df = df.reset_index(drop=True)
df = df.rename(
    columns={
        "Unnamed: 0": "Site Code",
        "Unnamed: 1": "Site Name",
        "Unnamed: 4": "Method",
        "###": 1992,
        "###.1": 1993,
        "###.2": 1994,
        "###.3": 1995,
        "###.4": 1997,
    }
)

# Mistake in the source data
query = df[df["Site Name"] == "Andryushkino"]
assert len(query) == 1
if query["LAT"].iloc[0] < 60:
    df.loc[df["Site Name"] == "Andryushkino", "LAT"] += 60.0

# Cleanup the string columns
for col in ["Site Code", "Site Name", "Method"]:
    df[col] = df[col].astype(str).str.strip()

# Cleanup the data columns
years = [c for c in df.columns if isinstance(c, int)]
for year in years:
    col = df[year].astype(str).str.strip()
    col = col.replace(["-", "inactive"], np.nan)
    col = col.str.replace("*", "", regex=False)
    col = col.str.replace("<", "", regex=False)
    col = col.str.replace(">", "", regex=False)
    col = col.replace("", np.nan)
    df[year] = col.astype(float)

# Setup target grid and only keep cells where there is some data
GRID_RES = 1.0
lat = np.linspace(-90, 90, int(round(180.0 / GRID_RES)) + 1)
lon = np.linspace(-180, 180, int(round(360.0 / GRID_RES)) + 1)
df.rename(columns={y: str(y) for y in years}).to_parquet("df.parquet")
df = (
    df.groupby(
        [
            pd.cut(df["LAT"], lat),
            pd.cut(df["LONG"], lon),
        ]
    )
    .median(numeric_only=True)
    .drop(columns=["LAT", "LONG"])
    .reset_index()
)
df = df[~df[years].isna().all(axis=1)].reset_index(drop=True)

# The pandas cuts leaves these dimensions as intervals, we want the midpoint.
for col in ["LAT", "LONG"]:
    df[col] = df[col].apply(lambda x: x.mid)

tb = np.array(
    [
        [cf.DatetimeNoLeap(y, 1, 1) for y in years],
        [cf.DatetimeNoLeap(y + 1, 1, 1) for y in years],
    ]
).T
t = np.array([tb[i, 0] + 0.5 * (tb[i, 1] - tb[i, 0]) for i in range(tb.shape[0])])

ds = xr.DataArray(
    df[years].to_numpy().T,
    coords={"time": t},
    dims=("time", "data"),
    attrs={"long_name": "Average thaw depth at end-of-season", "units": "cm"},
).to_dataset(name="alt")
ds["time_bnds"] = xr.DataArray(tb, dims=("time", "nb"))
ds["lat"] = xr.DataArray(df["LAT"].to_numpy(), dims=("data"))
ds["lon"] = xr.DataArray(df["LONG"].to_numpy(), dims=("data"))
ds.attrs = {
    "title": "CALM: Circumpolar Active Layer Monitoring Network",
    "versions": "2022",
    "institutions": "The George Washington University",
    "source": remote_source,
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/ILAMB-Data/blob/master/CALM/convert.py",
    "references": """
@ARTICLE{CALM,
  author = {CALM},
  title = {Circumpolar Active Layer Monitoring Network-CALM: Long-Term Observations of the Climate-Active Layer-Permafrost System.},
  journal = {online},
  url = {https://www2.gwu.edu/~calm/}
}""",
}
ds.to_netcdf(
    "CALM.nc",
    encoding={
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
