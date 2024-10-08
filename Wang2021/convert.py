import argparse
import os
import time

import cftime
import numpy as np
import xarray as xr

import gdown

if not os.path.isfile("olc_ors.nc"):
    gdown.download(id="1-v8QmowHzZrvUHWUdmSJw5rtwfxYJnfA",output="olc_ors.nc")
if not os.path.isfile("ec_ors.nc"):
    gdown.download(id="1fJnQ8P9WT6M1bSypbcZe2Cxqo_XwZsku",output="ec_ors.nc")

# setup some simple argument parsing to avoid a loop and indentation
parser = argparse.ArgumentParser()
parser.add_argument(
    "-f", dest="local_source", default="ec_ors.nc", help="source netcdf file"
)
args = parser.parse_args()
method = {
    "ec_ors.nc": "emergent constraint technique of (Mystakidis, 2016)",
    "olc_ors.nc": "optimal linear combination technique of (Hobeichi, 2018)",
}

# get some timestamp information
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime(args.local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# just 'open' the dataset and don't 'load' until you need to
ds = xr.open_dataset(args.local_source)

# add bounds to time, by default ILAMB uses a noleap calendar
tbnd = np.array(
    [
        [cftime.DatetimeNoLeap(t.dt.year, t.dt.month, 1) for t in ds["time"]],
        [
            cftime.DatetimeNoLeap(
                t.dt.year if t.dt.month < 12 else t.dt.year + 1,
                (t.dt.month + 1) if t.dt.month < 12 else 1,
                1,
            )
            for t in ds["time"]
        ],
    ]
).T

# Rewrite the time to be the middle of the time interval using the noleap calendar
ds["time"] = [cftime.DatetimeNoLeap(t.dt.year, t.dt.month, 15) for t in ds["time"]]
ds["time_bnds"] = (("time", "bnds"), tbnd)

# depth is missing a bounds attribute
ds["depth"].attrs["bounds"] = "depth_bnds"

# rename sm and *Do not* remove the standard deviation for now
ds = ds.rename({"sm": "mrsol","std": "mrsol_std"})

# convert % to kg m-2
ds["mrsol"] = ds["mrsol"] * ds["depth_bnds"].diff(dim="bnds").squeeze() * 998.0
ds["mrsol"].attrs = {"standard_name": "soil_moisture_content", "units": "kg m-2", "standard_error": "mrsol_std"}
ds["mrsol_std"] = ds["mrsol_std"] * ds["depth_bnds"].diff(dim="bnds").squeeze() * 998.0

#
ds["lat"].attrs["units"] = "degrees_north"
ds["lon"].attrs["units"] = "degrees_east"

# add all the global attributes
ds.attrs = {
    "title": f"Observation-based global multilayer soil moisture products for 1970 to 2016 using {method[args.local_source]}",
    "version": "1",
    "institutions": "Oak Ridge National Laboratory",
    "source": f"{args.local_source} from https://drive.google.com/drive/folders/1bm57jo6yUHGJ0P-sfPwA4NM5VCzSLoUr",
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/ILAMB-Data/blob/master/Wang2021/convert.py. Converted to mass area density and compressed to reduce file size.",
    "references": """
@ARTICLE{Wang2021,
  author = {Wang, Y. and Mao, J. and Jin, M. and Hoffman, F. M. and Shi, X. and Wullschleger, S. D. and Dai, Y.},
  title = {Development of observation-based global multilayer soil moisture products for 1970 to 2016},
  journal = {Earth System Science Data},
  year = {2021},
  volume = {13},
  issue = {9},
  page = {4385--4405},
  doi = {https://doi.org/10.5194/essd-13-4385-2021}
}
@ARTICLE{Wang2021a,
  author = "Yaoping Wang and Jiafu Mao",
  title = "Global Multi-layer Soil Moisture Products",
  year = "2021",
  doi = "10.6084/m9.figshare.13661312.v1"
}
""",
}

# remember to turn on compression, and the time needs encoding information to be
# self-consistent
ds.to_netcdf(
    f"mrsol_{args.local_source.split('_')[0]}.nc",
    encoding={
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
        "mrsol": {"zlib": True},  # <-- turns on compression for this variable
        "mrsol_std": {"zlib": True},
    },
)
