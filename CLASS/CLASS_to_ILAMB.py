import os
import time
from urllib.request import urlretrieve

import cftime
import numpy as np
import xarray as xr

# define sources
remote_sources = [
    "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2003.nc",
    "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2004.nc",
    "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2005.nc",
    "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2006.nc",
    "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2007.nc",
    "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2008.nc",
    "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2009.nc",
]
gist_source = (
    "https://github.com/rubisco-sfa/ILAMB-Data/blob/master/CLASS/CLASS_to_ILAMB.py"
)
local_sources = [os.path.basename(s) for s in remote_sources]

# ensure we have downloaded the data
for remote_source, local_source in zip(remote_sources, local_sources):
    if not os.path.isfile(local_source):
        urlretrieve(remote_source, local_source)
    stamp = time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(local_source)))

ds = xr.open_mfdataset(local_sources)

# rename some variables to follow conventions
ds = ds.rename({"hfds": "hfdsl", "hfds_sd": "hfdsl_sd", "rs": "rns", "rs_sd": "rns_sd"})

# noleap calendar is the ILAMB default, add bounds
ds["time"] = [
    cftime.DatetimeNoLeap(t.dt.year, t.dt.month, t.dt.day) for t in ds["time"]
]
tb = np.asarray(
    [
        [cftime.DatetimeNoLeap(t.dt.year, t.dt.month, 1) for t in ds["time"]],
        [
            cftime.DatetimeNoLeap(
                t.dt.year + (t.dt.month == 12),
                1 if t.dt.month == 12 else t.dt.month + 1,
                1,
            )
            for t in ds["time"]
        ],
    ]
).T

for v in ["mrro", "pr", "hfls", "hfss", "rns"]:

    uncert = f"{v}_sd"
    out = xr.Dataset(
        {
            v: ds[v],
            uncert: ds[uncert],
            "time_bnds": xr.DataArray(tb, dims=("time", "nv")),
        }
    )
    out[v].attrs["ancillary_variables"] = uncert
    out[uncert].attrs = {
        "standard_name": f"{v} standard_error",
        "units": out[v].attrs["units"],
    }

    attrs = {}
    attrs["title"] = "Conserving Land-Atmosphere Synthesis Suite (CLASS) v1.1"
    attrs["version"] = "1.1"
    attrs["institution"] = "University of New South Wales"
    attrs["source"] = (
        "Ground Heat Flux (GLDAS, MERRALND, MERRAFLX, NCEP_DOII, NCEP_NCAR), Sensible Heat Flux(GLDAS, MERRALND, MERRAFLX, NCEP_DOII, NCEP_NCAR, MPIBGC, Princeton), Latent Heat Flux(DOLCE1.0), Net Radiation (GLDAS, MERRALND, NCEP_DOII, NCEP_NCAR, ERAI, EBAF4.0), Precipitation(REGEN1.1), Runoff(LORA1.0), Change in Water storage(GRACE(GFZ, JPL, CSR))"
    )
    attrs["history"] = (
        """
%s: downloaded source from %s
%s: converted to ILAMB netCDF4 with %s"""
        % (stamp, "[" + ",".join(remote_sources) + "]", stamp, gist_source)
    )
    attrs[
        "references"
    ] = """
@InCollection{Hobeichi2019,
  author = 	 {Sanaa Hobeichi},
  title = 	 {Conserving Land-Atmosphere Synthesis Suite (CLASS) v1.1},
  booktitle = 	 {NCI National Research Data Collection},
  doi =          {doi:10.25914/5c872258dc183},
  year = 	 {2019}
}
@article{Hobeichi2020,
    author = {Hobeichi, Sanaa and Abramowitz, Gab and Evans, Jason},
    title = {Conserving Land-Atmosphere Synthesis Suite (CLASS)},
    journal = {Journal of Climate},
    volume = {33},
    number = {5},
    pages = {1821-1844},
    year = {2020},
    month = {01},
    doi = {doi:10.1175/JCLI-D-19-0036.1},
}
"""
    out.attrs = attrs
    out.to_netcdf(
        "%s.nc" % v,
        encoding={
            v: {"zlib": True},
            uncert: {"zlib": True},
            "time": {"units": "days since 2003-01-01", "bounds": "time_bnds"},
            "time_bnds": {"units": "days since 2003-01-01"},
        },
    )
