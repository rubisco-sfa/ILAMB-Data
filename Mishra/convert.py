import os
import time
import xarray as xr
from urllib.request import urlretrieve
import matplotlib.pyplot as plt
import numpy as np

def CoarsenDataset(ds,outfile,res=0.5):
    """Coarsens the source dataset by using xarray and dask to avoid large memory.
    
    Parameters
    ----------
    filename : str
        the netcdf file to coarsen
    outfile : str
        the name of the file for the coarsened output
    res : float, optional
        the approximate resolution of the coarsen output in degrees

    """
    n  = int(round(res/np.abs(ds['lat'].diff('lat').mean().values)))
    c  = ds.coarsen({'lat':n,'lon':n},boundary='pad').mean()
    c.to_netcdf(outfile)

if __name__ == "__main__":
    
    # If local files do not exist, download
    remote_sources  = ["https://datadryad.org/stash/downloads/file_stream/1526564",
                       "https://datadryad.org/stash/downloads/file_stream/1526565"]
    local_sources = ["circum1.tif","tibet1.tif"]
    for remote_source,local_source in zip(remote_sources,local_sources):
        local_source  = os.path.basename(remote_source)
        if not os.path.isfile(local_source): urlretrieve(remote_source, local_source)
    download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_sources[0])))
    generate_stamp = time.strftime('%Y-%m-%d')

    """
    This dataset provides data in 10 bands which have the following definitions:

    * band  1: soc0_1m    <---- compares to cSoilAbove1m
    * band  2: soc0_3m    <---- compares to cSoil
    * band  3: soc1_2m 
    * band  4: soc2_3m 
    * band  5: uncert0_1m 
    * band  6: uncert0_3m 
    * band  7: uncert1_2m 
    * band  8: uncert2_3m 
    * band  9: soc0_03m
    * band 10: uncert0_03m
    
    """
    for local_source in local_sources:
        ds = xr.open_dataset(local_source,engine='rasterio')
        ds = ds.sel({'band':[1,2,5,6]})
        ds = ds.rio.reproject('EPSG:4326')
        ds = ds.rename({'x':'lon','y':'lat'})
        CoarsenDataset(ds,local_source.replace(".tif",".nc"))
