"""Convert Morgan's HIPTOM estimate to a ILAMB format.

https://doi.org/10.1029/2022GB007520

"""

import os
import time

import numpy as np
import pandas as pd
import xarray as xr

# generate timestamp information
REMOTE_SOURCE = "HIPTOM_Flux_estimate.csv"
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(REMOTE_SOURCE))
)
generate_stamp = time.strftime("%Y-%m-%d")

# read in source data and find monthly means
df = pd.read_csv(REMOTE_SOURCE).drop(columns="Unnamed: 0")

# The original data was assumed to be taken at noon on the given day of the year
# (a representative time corresponding to the mean daily carbon). The flux was
# then approximated using a finite difference approximation leading to 364 data
# points here, located at the day interfaces.
df["doy"] = pd.to_datetime(df["doy"], unit="D", origin="2001-1-2 00:00:00")
df = df.set_index("doy")

# convert to monthly mean for model comparison
dfm = df.resample("M").mean()

# we will need time bounds for ILAMB
dfm["time_begin"] = dfm.index - pd.offsets.MonthBegin(1)
dfm["time_end"] = dfm.index + pd.Timedelta(1, unit="D")
dfm.index = dfm["time_begin"] + 0.5 * (dfm["time_end"] - dfm["time_begin"])

# convert to xarray and populate attributes
ds = dfm.to_xarray()
ds = ds.rename({"index": "time", "flux": "gsnf", "error": "gsnf_error"})
ds["gsnf"].attrs = {
    "long_name": "Growing season net flux",
    "units": "Pg yr-1",
    "bounds": "gsnf_error",
}
ds["gsnf_error"] = xr.DataArray(
    np.asarray(
        [
            ds["gsnf"] - ds["gsnf_error"],
            ds["gsnf"] + ds["gsnf_error"],
        ]
    ).T,
    dims=("time", "nb"),
)
ds["time"].attrs["bounds"] = "time_bounds"
ds["time"].encoding["units"] = "days since 2001-01-01"
ds["time"].encoding["calendar"] = "noleap"
ds["time_bounds"] = xr.DataArray(
    np.asarray([ds["time_begin"], ds["time_end"]]).T, dims=("time", "nb")
)
ds["time_bounds"].encoding["units"] = "days since 2001-01-01"
ds["time_bounds"].encoding["calendar"] = "noleap"
ds = ds.drop(["time_begin", "time_end"])
ds.attrs = {
    "title": "Northern Hemisphere Growing Season Net Carbon Flux",
    "versions": "1",
    "institutions": "University of Michigan",
    "source": "Aircraft observations (HIPPO, AToM) of atmospheric carbon dioxide concentrations are used to infer the net flux of the northern extratropical growing season net flux.",
    "history": f"""
{download_stamp}: received emailed file, {REMOTE_SOURCE};
{generate_stamp}: converted to netCDF""",
    "references": """
@ARTICLE{Loechli2023,
  author = {Morgan Loechli, Britton B. Stephens, Roisin Commane, Frédéric Chevallier, Kathryn McKain, Ralph F. Keeling, Eric J. Morgan, Prabir K. Patra, Maryann R. Sargent, Colm Sweeney, Gretchen Keppel-Aleks},
  title = {Evaluating Northern Hemisphere Growing Season Net Carbon Flux in Climate Models Using Aircraft Observations},
  journal = {Global Biogeochemical Cycles},
  year = {2023},
  volume = {37},
  number = {2}
  doi = {https://doi.org/10.1029/2022GB007520}
}""",
}
ds.to_netcdf("HIPPO_ATom.nc")
