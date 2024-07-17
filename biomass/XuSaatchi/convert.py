"""Downloads the XuSaatchi Biomass dataset and converts to netCDF, bringing up
to ILAMB/CF standards."""

import os
import time

import cftime
import ilamblib as il
import numpy as np
import requests
import xarray as xr

SHOW_PLOTS = False

# pylint: disable=line-too-long,invalid-name
remote_source = "https://zenodo.org/record/4161694/files/test10a_cd_ab_pred_corr_2000_2019_v2.tif?download=1"
local_source = os.path.basename(remote_source).split("?")[0]

# optionally download the raw data
if not os.path.isfile(local_source):
    print(f"Downloading {remote_source}...")
    with requests.get(remote_source) as req:
        with open(local_source, "wb") as out:
            out.write(req.content)
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# reproject and coarsen the data, target resolution is half degree
ds = xr.open_dataset(local_source, engine="rasterio")
ds = ds.rio.reproject("EPSG:4326")
ds = ds.rename({"x": "lon", "y": "lat"})
# pylint: disable=no-member
dsc = il.coarsen_dataset(ds)
dsc["da"] = dsc["da"].transpose("time", "lat", "lon")

# make sure that the mass is conserved
s = (ds["da"] * ds["cell_measures"]).sum()
sc = (dsc["da"] * dsc["cell_measures"]).sum()
assert ((s - sc) / sc) < 1e-3

# re-code time with bounds that will let us coarsen model results
tb = np.asarray(
    [
        [cftime.DatetimeNoLeap(t.dt.year, 1, 1) for t in ds["time"]],
        [cftime.DatetimeNoLeap(t.dt.year + 1, 1, 1) for t in ds["time"]],
    ]
).T
dsc["time_bnds"] = xr.DataArray(tb, dims=("time", "nv"))
dsc["time"] = dsc["time_bnds"].mean(dim="nv")
dsc["time"].encoding["units"] = "days since 1850-01-01"
dsc["time"].attrs["bounds"] = "time_bnds"

if SHOW_PLOTS:
    import matplotlib.pyplot as plt

    vmax = ds["da"].quantile(0.98)
    plt.subplot(1, 2, 1)
    ds["da"].isel({"time": 0}).plot(vmax=vmax)
    plt.subplot(1, 2, 2)
    dsc["da"].isel({"time": 0}).plot(vmax=vmax)
    plt.show()

# encode CF metadata
dsc = dsc.drop(["spatial_ref", "cell_measures"]).rename({"da": "biomass"})
dsc["biomass"].attrs = {
    "long_name": "annual carbon density map of global live woody vegetation",
    "units": "Mg ha-1",
}
dsc.attrs = {
    "title": "Changes in Global Terrestrial Live Biomass over the 21st Century",
    "version": "2",
    "institution": "Jet Propulsion Laboratory, California Institute of Technology",
    "source": "Estimates of carbon stock changes of live woody biomass from 2000 to 2019 using measurements from ground, air, and space",
    "history": f"""
{download_stamp}: downloaded source from {remote_source};
{generate_stamp}: coarsened to 0.5 degree resolution, brought meta-data up to CF standards using https://github.com/rubisco-sfa/ILAMB-Data/blob/master/Saatchi/convert.py;""",
    "references": """
@Xu2021{
  author = {Xu, Liang, Saatchi, Sassan S., Yang, Yan, Yu, Yifan, Pongratz, Julia, Bloom, A. Anthony, Bowman, Kevin, Worden, John, Liu, Junjie, Yin, Yi, Domke, Grant, McRoberts, Ronald E., Woodall, Christopher, Nabuurs, Gert-Jan, de-Miguel, Sergio, Keller, Michael, Harris Nancy, Maxwell, Sean, & Schimel, David},
  title= {Dataset for "Changes in Global Terrestrial Live Biomass over the 21st Century" (2.0)},
  journal = {Zenodo},
  year = {2021},
  doi = {https://doi.org/10.5281/zenodo.4161694}
}
""",
}
dsc.to_netcdf("Saatchi.nc")
