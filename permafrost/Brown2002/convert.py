import os
import time
from functools import partial

import cftime
import fiona
import geopandas as gpd
import numpy as np
import rasterio as rio
import xarray as xr
from fiona.transform import transform_geom
from rasterio import features
from shapely.geometry import mapping, shape


def base_transformer(geom, src_crs, dst_crs):
    return shape(
        transform_geom(
            src_crs=src_crs,
            dst_crs=dst_crs,
            geom=mapping(geom),
            antimeridian_cutting=True,
        )
    )


RASTER_RES = 0.25
CMD = "wget --ftp-user=anonymous -r -nd ftp://sidads.colorado.edu/pub/DATASETS/fgdc/ggd318_map_circumarctic/permaice*"
if not os.path.isfile("permaice.shp"):
    os.system(CMD)
if os.path.isfile("permaice_4326cut.shp"):
    df = gpd.read_file("permaice_4326cut.shp")
else:
    df = gpd.read_file("permaice.shp")
    df["NUM_CODE"] = df["NUM_CODE"].astype(int)
    # this reprojection method cuts across the antimeridian but is very slow, so we dump
    # out the intermediate result in case you want to play around with it
    forward_transformer = partial(base_transformer, src_crs=df.crs, dst_crs="EPSG:4326")
    with fiona.Env(OGR_ENABLE_PARTIAL_REPROJECTION="YES"):
        df = df.set_geometry(df.geometry.apply(forward_transformer))
    df.to_file("permaice_4326cut.shp")
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime("permaice.shp"))
)
generate_stamp = time.strftime("%Y-%m-%d")

# for convenience, add an extent code which reflects glaciated, we use this to mask out
# these regions in the ILAMB analysis.
df.loc[df["NUM_CODE"] == 21, "EXTENT"] = "G"

# now remove sporadic/isolated permafrost and all polygons with no EXTENT code and
# change to numerical values
df.loc[df["EXTENT"] == "S"] = np.nan
df.loc[df["EXTENT"] == "I"] = np.nan
df = df[df["EXTENT"].notna()]
df["TYPE"] = df["EXTENT"].apply(lambda x: ["G", "C", "D"].index(x))

# rasterize
nlat = int(round((df.total_bounds[3] - df.total_bounds[1]) / RASTER_RES))
nlon = int(round((df.total_bounds[2] - df.total_bounds[0]) / RASTER_RES))
transform = rio.transform.from_bounds(*(df.total_bounds), nlon, nlat)
data = features.rasterize(
    shapes=[(geom, value) for geom, value in zip(df["geometry"], df["TYPE"])],
    fill=np.nan,
    out_shape=(nlat, nlon),
    transform=transform,
)
data = data[::-1]  # raster is generated upsidedown
data = data.reshape((1,) + data.shape)  # pad for a single entry time dimension

# encode the dataset
ds = xr.DataArray(
    data,
    coords={
        "time": [cftime.DatetimeNoLeap(1976, 6, 1)],
        "lat": np.linspace(df.total_bounds[1], df.total_bounds[3], nlat),
        "lon": np.linspace(df.total_bounds[0], df.total_bounds[2], nlon),
    },
    dims=("time", "lat", "lon"),
    attrs={
        "long_name": "permafrost extent",
        "labels": ["Glaciated", "Continuous", "Discontinuous"],
    },
).to_dataset(name="permafrost_extent")
ds["time_bnds"] = xr.DataArray(
    np.asarray(
        [[cftime.DatetimeNoLeap(1960, 1, 1), cftime.DatetimeNoLeap(1993, 1, 1)]]
    ),
    dims=("time", "nb"),
)
ds.attrs = {
    "title": "Circum-Arctic Map of Permafrost and Ground-Ice Conditions",
    "version": "2",
    "institutions": "National Snow and Ice Data Center Distributed Active Archive Center",
    "source": f"Shapefiles obtained by: {CMD}",
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/ILAMB-Data/blob/main/NSIDC/convert.py",
    "references": """
@ARTICLE{,
    author = {Brown, J., O. Ferrians, J. A. Heginbottom, and E. Melnikov},
    title= {Circum-Arctic Map of Permafrost and Ground-Ice Conditions, Version 2},
    journal = {National Snow and Ice Data Center Distributed Active Archive Center},
    year = {2002},
    doi = {10.7265/skbgkf16}
}""",
}
ds.to_netcdf(
    "Brown2002.nc",
    encoding={
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
