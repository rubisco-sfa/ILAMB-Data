"""Convert tif files to ILAMB-ready netCDF, coarsen to 0.5 degree"""
import glob
import os
import time

import cftime
import numpy as np
import xarray as xr
from ilamb3 import dset

# Coarsening these tif files requires a large amount of memory (~250Gb) and
# takes considerable time. Thus we create intermediate coarsened netCDF files
# that we then can merge together in a ILAMB-appropriate file.
for local_source in glob.glob("*.tif"):

    download_stamp = time.strftime(
        "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
    )
    filename = local_source.replace(".tif", ".nc")
    if os.path.isfile(filename):
        continue
    print(f"Coarsening {local_source}...")

    # reproject and coarsen the data, target resolution is half degree
    ds = xr.open_dataset(local_source, engine="rasterio")
    ds = ds.rename({"x": "lon", "y": "lat"}).sel({"band": 1})
    dsc = dset.coarsen_dataset(ds)
    dsc.to_netcdf(filename)

# Read in the intermediate data and sanitize preparing for merge
data = {
    "agb": xr.open_dataset("aboveground_biomass_carbon_2010.nc"),
    "agbu": xr.open_dataset("aboveground_biomass_carbon_2010_uncertainty.nc"),
    "bgb": xr.open_dataset("belowground_biomass_carbon_2010.nc"),
    "bgbu": xr.open_dataset("belowground_biomass_carbon_2010_uncertainty.nc"),
}
for key, ds in data.items():
    data[key] = (
        ds["band_data"].drop_vars(["band", "spatial_ref"]).rename(key) * 0.1
    )  # in the docs, there is a scaling factor specified /facepalm
# pylint: disable=unbalanced-tuple-unpacking
data["agb"], data["agbu"], data["bgb"], data["bgbu"] = xr.align(
    data["agb"], data["agbu"], data["bgb"], data["bgbu"], join="override"
)
out = xr.Dataset(
    {
        "biomass": data["agb"] + data["bgb"],
        "uncert": np.sqrt(data["agbu"] ** 2 + data["bgbu"] ** 2),
    }
)

# Add a time dimension so ILAMB can tell what year to match from models
# re-code time with bounds that will let us coarsen model results
tb = np.asarray(
    [
        [cftime.DatetimeNoLeap(2010, 1, 1)],
        [cftime.DatetimeNoLeap(2011, 1, 1)],
    ]
).T
tb = xr.DataArray(tb, dims=("time", "nv"))
t = tb.mean(dim="nv")
out["biomass"] = out["biomass"].expand_dims(dim={"time": t})
out["uncert"] = out["uncert"].expand_dims(dim={"time": t})
out["time"].encoding["units"] = "days since 2010-1-1"
out["time_bnds"] = tb
out["time_bnds"].encoding["units"] = "days since 2010-1-1"
out["time"].attrs["bounds"] = "time_bnds"
out["biomass"].attrs = {
    "name": "Total carbon biomass",
    "units": "Mg ha-1",
    "standard_error": "uncert",
}
out["uncert"].attrs = {
    "name": "Total carbon biomass uncertainty",
    "units": "Mg ha-1",
}
# pylint: disable=line-too-long
out.attrs = {
    "title": "Global Aboveground and Belowground Biomass Carbon Density Maps for the Year 2010",
    "version": "1",
    "institutions": "University of Wisconsin-Madison",
    "source": "25 datasets spanning woody, grassland, tundra, and cropland cover types, and reporting above and below ground biomass (https://www.nature.com/articles/s41597-020-0444-4/tables/2)",
    "history": f"""
{download_stamp}: Data requested and downloaded from https://doi.org/10.3334/ORNLDAAC/1763;
{time.strftime('%Y-%m-%d')}: Coarsened (mass conserving) to 0.5 degree and converted to ILAMB-ready netcdf files.""",
    "references": """
@ARTICLE{Spawn2020a,
  author = {Spawn, S.A., and H.K. Gibbs},
  title = {Global Aboveground and Belowground Biomass Carbon Density Maps for the Year 2010},
  journal = {ORNL DAAC},
  year = {2020},
  doi = {https://doi.org/10.3334/ORNLDAAC/1763}
}
@ARTICLE{Spawn2020b
  author = {Seth A. Spawn, Clare C. Sullivan, Tyler J. Lark & Holly K. Gibbs},
  title = {Harmonized global maps of above and belowground biomass carbon density in the year 2010},
  journal = {Scientific Data},
  volume = {7},
  number = {112},
  year = {2020},
  doi = {https://doi.org/10.1038/s41597-020-0444-4}
}
""",
}

out.to_netcdf("SpawnGibbs2020.nc")
