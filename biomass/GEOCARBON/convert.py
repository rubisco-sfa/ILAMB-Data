import os
import time

import cftime
import numpy as np
import xarray as xr


def CoarsenDataset(filename, res=0.5, ntile=None):
    """Coarsens the source dataset by using xarray and dask to avoid large memory.

    Parameters
    ----------
    filename : str
        the netcdf file to coarsen
    outfile : str
        the name of the file for the coarsened output
    res : float, optional
        the approximate resolution of the coarsen output in degrees
    ntile : int, optional the number of coarse cells to process at a
        time, routine will process a ntile x ntile subgrid. Increase
        to run faster, decrease to reduce the peak memory.

    """
    with xr.open_dataset(filename) as ds:
        ds = ds.rename({"latitude": "lat", "longitude": "lon"})
        res0 = float(ds.lon.diff(dim="lon").mean())
        n = int(round(res / res0))
        if ntile is not None:
            ds = ds.chunk({"lat": ntile * n, "lon": ntile * n})
        c = ds.coarsen({"lat": n, "lon": n}, boundary="pad").mean()
    return c


if __name__ == "__main__":

    # If local file does not exist, download
    remote_source = "https://www.bgc-jena.mpg.de/geodb/projects/DataDnld.php"
    local_source = "Forest_Aboveground_Biomassv3.nc"
    download_stamp = time.strftime(
        "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
    )
    generate_stamp = time.strftime("%Y-%m-%d")

    # Coarsen
    ds = CoarsenDataset(local_source, res=0.5)

    # For some unknown reason, we need to re-create the biomass
    # dataarray such that the encoding works out properly. I was
    # having the problem that unit conversions didn't fail, but
    # neither did they change the magnitudes of the data.
    ds["biomass"] = xr.DataArray(
        np.ma.masked_invalid(
            ds["Forest_Aboveground_Biomass_v3"]
            .to_numpy()
            .reshape((1,) + ds["Forest_Aboveground_Biomass_v3"].shape)
        ),
        dims=("time", "lat", "lon"),
    )
    ds["biomass"].attrs = {"long_name": "above_ground_biomass", "units": "Mg ha-1"}
    ds = ds.drop_vars("Forest_Aboveground_Biomass_v3")

    # Having trouble with the time so let's just re-encode it
    ds["time"] = [cftime.DatetimeNoLeap(2015, 6, 1)]
    tb = np.asarray(
        [
            [cftime.DatetimeNoLeap(2000, 1, 1), cftime.DatetimeNoLeap(2010, 1, 1)],
        ]
    )
    ds["time_bnds"] = xr.DataArray(tb, dims=("time", "nv"))
    ds["time"].encoding["units"] = "days since 2000-01-01"
    ds["time"].attrs["bounds"] = "time_bnds"

    ds = ds.sortby(list(ds.sizes.keys()))
    ds.attrs = {}
    ds.attrs["title"] = (
        "GEOCARBON: Towards an Operational Global Carbon Observing System"
    )
    ds.attrs["version"] = "3"
    ds.attrs["institution"] = "GEOCARBON (European FP7 project)"
    ds.attrs["source"] = (
        "Files are available https://www.bgc-jena.mpg.de/geodb/projects/Home.php"
    )
    ds.attrs[
        "history"
    ] = f"""
{download_stamp}: downloaded source from {remote_source};
{generate_stamp}: coarsened to 0.5 degree resolution using https://github.com/rubisco-sfa/ILAMB-Data/blob/master/biomass/GEOCARBON/convert.py;"""

    ds.attrs[
        "references"
    ] = """
@ARTICLE{GEOCARBON,
  author = {GEOCARBON},
  title = {Operational Global Carbon Observing System},
  journal = {ENV.2011.4.1.1-1},
  year = {283080},
}"""
    ds.to_netcdf("biomass.nc", encoding={"biomass": {"zlib": True}})
