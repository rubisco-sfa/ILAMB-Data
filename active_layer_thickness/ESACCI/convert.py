import os
import time

import cftime
import numpy as np
import requests
import xarray as xr
from ilamb3 import dataset as dset
from tqdm import tqdm


def download_file(remote_source):
    """."""
    local_source = os.path.basename(remote_source)
    local_source = local_source.split("?")[0]
    if not os.path.isfile(local_source):
        resp = requests.get(remote_source, stream=True)
        total_size = int(resp.headers.get("content-length"))
        with open(local_source, "wb") as fdl:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=local_source,
                ascii=True,
            ) as pbar:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk:
                        fdl.write(chunk)
                        pbar.update(len(chunk))
    return local_source


# Download source data
root = "https://dap.ceda.ac.uk/neodc/esacci/permafrost/data/active_layer_thickness/L4/area4/pp/v03.0"
remote_sources = [
    f"{root}/ESACCI-PERMAFROST-L4-ALT-ERA5_MODISLST_BIASCORRECTED-AREA4_PP-{year}-fv03.0.nc?download=1"
    for year in range(1997, 2003)
]
remote_sources += [
    f"{root}/ESACCI-PERMAFROST-L4-ALT-MODISLST_CRYOGRID-AREA4_PP-{year}-fv03.0.nc?download=1"
    for year in range(2003, 2020)
]
local_sources = []
for source in remote_sources:
    local_sources.append(download_file(source))
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime(local_sources[0]))
)
generate_stamp = time.strftime("%Y-%m-%d")


# Reproject and coarsen for use in ESMs
ds = xr.open_mfdataset(local_sources)
ds = (
    ds.rio.write_crs("EPSG:3995")
    .rio.reproject("EPSG:4326")
    .rename({"x": "lon", "y": "lat"})
)
ds = dset.coarsen_dataset(ds, res=0.5)

# Cleanup the resulting file
ds = ds.drop(["cell_measures", "polar_stereographic"])
ds = ds.rename({"ALT": "alt"})
ds["alt"] = ds["alt"].transpose("time", "lat", "lon")
ds["alt"].attrs = {"standard_name": "permafrost_active_layer_thickness", "units": "m"}

# Re-encode time to use the ILAMB standard noleap calendar and include bounds
ds["time"] = [
    cftime.DatetimeNoLeap(t.dt.year, t.dt.month, t.dt.day) for t in ds["time"]
]
tb = np.asarray(
    [
        [cftime.DatetimeNoLeap(t.dt.year, 1, 1) for t in ds["time"]],
        [cftime.DatetimeNoLeap(t.dt.year + 1, 1, 1) for t in ds["time"]],
    ]
).T
ds["time_bnds"] = xr.DataArray(tb, dims=("time", "nv"))

# Add attributes
ds.attrs = {
    "title": "Permafrost active layer thickness for the Northern Hemisphere",
    "version": "3",
    "institutions": "ESA Permafrost Climate Change Initiative, NERC EDS Centre for Environmental Data Analysis",
    "source": f"[{','.join(remote_sources)}]",
    "history": f"Downloaded on {download_stamp} and generated a coarsened netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/ILAMB-Data/blob/master/active_layer_thickness/ESACCI/convert.py.",
    "references": """
@ARTICLE{,
    author = {Obu, J.; Westermann, S.; Barboux, C.; Bartsch, A.; Delaloye, R.; Grosse, G.; Heim, B.; Hugelius, G.; Irrgang, A.; Kääb, A.M.; Kroisleitner, C.; Matthes, H.; Nitze, I.; Pellet, C.; Seifert, F.M.; Strozzi, T.; Wegmüller, U.; Wieczorek, M.; Wiesmann, A.},
    title= {ESA Permafrost Climate Change Initiative (Permafrost_cci): Permafrost active layer thickness for the Northern Hemisphere, v3.0},
    journal = {NERC EDS Centre for Environmental Data Analysis},
    year = {2021},
    doi = {10.5285/67a3f8c8dc914ef99f7f08eb0d997e23}
}""",
}

ds.to_netcdf(
    "alt.nc",
    encoding={
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
