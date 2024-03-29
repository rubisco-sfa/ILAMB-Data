import os
import time
import xarray as xr
from urllib.request import urlretrieve
import numpy as np
import cftime

def CoarsenDataset(filename,outfile,res=0.5,ntile=10):
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
        n  = int(((ds.lat>=0)*(ds.lat<res)).sum())
        ds = ds.chunk({'lat':ntile*n,'lon':ntile*n})
        c  = ds.coarsen({'lat':n,'lon':n},boundary='pad').mean()
        c.to_netcdf(outfile)

if __name__ == "__main__":

    # If local file does not exist, download
    remote_source  = "https://dap.ceda.ac.uk/neodc/esacci/biomass/data/agb/maps/v3.0/netcdf/ESACCI-BIOMASS-L4-AGB-MERGED-100m-2010-fv3.0.nc"
    local_source  = os.path.basename(remote_source)
    if not os.path.isfile(local_source):
        print("Warning: downloading a 18 Gb file...")
        urlretrieve(remote_source, local_source)
    download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))
    generate_stamp = time.strftime('%Y-%m-%d')

    # First we create the coarsened file with a generic routine
    if not os.path.isfile("out.nc"):
        """
        @theseus, 250 Gb memory
        peak memory: 6158.36 MiB, increment: 6049.49 MiB
        CPU times: user 1h 10min 54s, sys: 40min 13s, total: 1h 51min 8s
        Wall time: 1h 24min 6s
        """
        CoarsenDataset(local_source,"out.nc",res=0.5,ntile=10)

    # Now we cleanup the file for ILAMB uses
    ds = xr.load_dataset("out.nc")
    
    # For some unknown reason, we need to re-create the biomass
    # dataarray such that the encoding works out properly. I was
    # having the problem that unit conversions didn't fail, but
    # neither did they change the magnitudes of the data.
    a = ds['agb'].attrs
    ds['biomass'] = xr.DataArray(np.ma.masked_invalid(ds['agb'].to_numpy()),dims=('time','lat','lon'))
    ds['biomass'].attrs = a
    ds = ds.drop(['crs','agb','agb_se'])

    # Having trouble with the time so let's just re-encode it
    ds['time'] = [cftime.DatetimeNoLeap(t.dt.year,t.dt.month,t.dt.day) for t in ds['time']]
    tb = np.asarray([[cftime.DatetimeNoLeap(t.dt.year  ,t.dt.month,1) for t in ds['time']],
                     [cftime.DatetimeNoLeap(t.dt.year+1,1         ,1) for t in ds['time']]]).T
    ds['time_bnds'] = xr.DataArray(tb,dims = ('time','nv'))
    ds['time'].encoding['units']= "days since 2010-01-01"
    ds['time'].attrs['bounds'] = 'time_bnds'

    ds = ds.reindex(lat=ds.lat[::-1])
    ds.attrs = {}
    ds.attrs['title'] = 'ESA CCI above-ground biomass product level'
    ds.attrs['version'] = '3'
    ds.attrs['institution'] = 'GAMMA Remote Sensing'
    ds.attrs['source'] = 'ALOS-2 PALSAR-2 FB and WB mosaics, Sentinel-1 GRD'
    ds.attrs['history'] = """
%s: downloaded source from %s;
%s: coarsened to 0.5 degree resolution using https://github.com/rubisco-sfa/ILAMB-Data/blob/master/ESACCI/convert.py;""" % (download_stamp,remote_source,generate_stamp)
    ds.attrs['references'] = """
@ARTICLE{Santoro2021,
  author = {M. Santoro and O. Cartus},
  title = {ESA Biomass Climate Change Initiative (Biomass_cci): Global datasets of forest above-ground biomass for the years 2010, 2017 and 2018, v3},
  journal = {NERC EDS Centre for Environmental Data Analysis},
  year = {2021},
  doi = {http://dx.doi.org/10.5285/5f331c418e9f4935b8eb1b836f8a91b8}
}"""
    ds.to_netcdf("biomass.nc")

        
