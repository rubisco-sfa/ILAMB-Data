import datetime
import os
import time

import cftime
import numpy as np
import rasterio as rio
import rioxarray  # noqa: F401
import xarray as xr
from tqdm import tqdm

# requires ~/.netrc
dir = "daacdata.apps.nsidc.org/pub/DATASETS/nsidc0447_CMC_snow_depth_v01/Snow_Depth/Snow_Depth_Daily_Values/GeoTIFF"
local_sources = [
    f"cmc_sdepth_dly_{year}_v01.{3 if year==2019 else 2}.tif"
    for year in range(1998, 2021)
]
for filename in local_sources:
    if os.path.isfile(os.path.join(dir, filename)):
        continue
    link = f"https://{dir}/{filename}"
    cmd = " ".join(
        [
            "wget",
            "--load-cookies ~/.urs_cookies",
            "--save-cookies ~/.urs_cookies",
            "--keep-session-cookies",
            "--no-check-certificate",
            "--auth-no-challenge=on",
            '-r --reject "index.html*"',
            "-np -e robots=off",
            link,
        ]
    )
    os.system(cmd)

# Timestamps
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime(os.path.join(dir, local_sources[0])))
)
generate_stamp = time.strftime("%Y-%m-%d")

# I tried using the rasterio engine inside of xarray.open_mfdataset() but was having
# hangs that I couldn't explain.
das = []
for f in tqdm(range(len(local_sources))):
    filename = local_sources[f]
    img = rio.open(os.path.join(dir, filename))
    lat = [img.xy(i, 0)[1] for i in range(img.height)]
    lon = [img.xy(0, i)[0] for i in range(img.width)]
    year = filename.split("_")[3]
    dpy = 365 if int(year) % 4 else 366
    time = [
        datetime.datetime.strptime(year + str(b + dpy - img.count), "%Y%j")
        for b in range(1, img.count + 1)
    ]
    data = img.read()
    da = xr.DataArray(data=data, coords={"time": time, "y": lat, "x": lon})
    da = da.groupby("time.month").mean(dim="time")
    da["month"] = [cftime.DatetimeNoLeap(int(year), m, 1) for m in da["month"]]
    da = da.rename(month="time")
    das.append(da)
da = xr.concat(das, dim="time")
ds = da.to_dataset(name="snd")
ds = ds.rio.write_crs(img.crs).rio.reproject("EPSG:4326")
ds = ds.rename(x="lon", y="lat").drop("spatial_ref")
ds["snd"] = xr.where(
    (ds["snd"] < 1e30) & ((ds["snd"] > 0).any(dim="time")), ds["snd"], np.nan
)
ds["snd"].attrs = {"standard_name": "surface_snow_thickness", "units": "cm"}
ds["time_bnds"] = xr.DataArray(
    [
        [
            cftime.DatetimeNoLeap(t.dt.year, t.dt.month, 1),
            cftime.DatetimeNoLeap(
                t.dt.year + (t.dt.month == 12),
                1 if t.dt.month == 12 else (t.dt.month + 1),
                1,
            ),
        ]
        for t in ds["time"]
    ],
    dims=["time", "nb"],
)

# Add attributes
ds.attrs = {
    "title": "Daily Snow Depth Analysis Data",
    "version": "1",
    "institutions": "Canadian Meteorological Centre (CMC)",
    "source": f"[{','.join(local_sources)}]",
    "history": f"Downloaded on {download_stamp} and generated a coarsened netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/ILAMB-Data/blob/master/snd/CMC/convert.py.",
    "references": """
@ARTICLE{CMC2010,
    author = {Brown, R. D. and B. Brasnett.},
    title= {Canadian Meteorological Centre (CMC) Daily Snow Depth Analysis Data, Version 1},
    journal = {NASA National Snow and Ice Data Center Distributed Active Archive Center},
    year = {2010},
    doi = {10.5067/W9FOYWH0EQZ3}
}""",
}
ds.to_netcdf(
    "snd.nc",
    encoding={
        "snd": {"zlib": True},
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
