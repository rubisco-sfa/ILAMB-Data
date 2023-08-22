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

ds = xr.open_mfdataset(local_sources[:2])
ds = (
    ds.rio.write_crs("EPSG:3995")
    .rio.reproject("EPSG:4326")
    .rename({"x": "lon", "y": "lat"})
)
ds = dset.coarsen_dataset(ds, res=0.5)
