import numpy as np
from netCDF4 import Dataset
import os
import time
from urllib.request import urlretrieve
from ILAMB.constants import bnd_months,mid_months
import xarray as xr
import pandas as pd

# define sources
remote_source = "ftp://ftp.cdc.noaa.gov/Datasets/gpcc/full_v2018/precip.mon.total.v2018.nc"
local_source  = os.path.basename(remote_source)

# ensure we have downloaded the data
if not os.path.isfile(local_source): urlretrieve(remote_source, local_source)   
stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))

cf_name = "pr"
var_name = "precip"
dset = xr.load_dataset(local_source)
dset = dset.sel(time=slice('1980-01-01',dset.time[-1]))
data = np.ma.masked_invalid(dset[var_name])
mask = np.ones(data.shape[0],dtype=bool)[:,np.newaxis,np.newaxis]*(np.abs(data)<1e-15).all(axis=0)
data = np.ma.masked_array(data,mask=data.mask+mask)
download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))
generate_stamp = time.strftime('%Y-%m-%d')
dt = pd.to_datetime(dset.time.data)
t  = np.array([(t.year-1850)*365+mid_months[t.month-1] for t in dt])
tb = np.array([[(t.year-1850)*365+bnd_months[t.month-1],
                (t.year-1850)*365+bnd_months[t.month]] for t in dt])
lat = dset.lat
lon = dset.lon
dpm = np.diff(tb,axis=1)[:,0]
data = data / dpm[:,np.newaxis,np.newaxis]
with Dataset("%s.nc" % cf_name, mode="w") as oset:
    
    # dimensions
    oset.createDimension("time", size = t.size)
    oset.createDimension("lat", size = lat.size)
    oset.createDimension("lon", size = lon.size)
    oset.createDimension("nb", size = 2)
    
    # time
    T = oset.createVariable("time", t.dtype, ("time"))
    T[...] = t
    T.units = "days since 1850-01-01 00:00:00"
    T.calendar = "noleap"
    T.bounds = "time_bounds"
    
    # time bounds
    TB = oset.createVariable("time_bounds", t.dtype, ("time", "nb"))
    TB[...] = tb
    
    # latitude
    X = oset.createVariable("lat", lat.dtype, ("lat"))
    X[...] = lat
    X.standard_name = "latitude"
    X.long_name = "site latitude"
    X.units = "degrees_north"
    
    # longitude
    Y = oset.createVariable("lon", lon.dtype, ("lon"))
    Y[...] = lon
    Y.standard_name = "longitude"
    Y.long_name = "site longitude"
    Y.units = "degrees_east"
    
    # data
    D = oset.createVariable(cf_name, data.dtype, ("time", "lat", "lon"), zlib=True)
    D[...] = data
    D.units = dset[var_name].units + " d-1"
    with np.errstate(invalid='ignore'):
        D.actual_range = np.asarray([data.min(),data.max()])
        
    oset.title = "GPCC Full Data Reanalysis Version 2018 0.5x0.5 Monthly Total"
    oset.version = "v2018"
    oset.institutions = "Global Precipitation Climatology Centre (GPCC)"
    oset.source = "GPCC Precipitation 0.5 degree V2018 Full Reanalysis"
    oset.history = """
%s: downloaded source from %s;
%s: converted to netCDF, original data is divided by days in a month for a mean monthly rate.""" % (download_stamp, remote_source, generate_stamp)
    oset.references  = """
@article{Schneider2017, 
  author    = {Schneider, Udo and Finger, Peter and Meyer-Christoffer, Anja and Rustemeier, Elke and Ziese, Markus and Becker, Andreas}, 
  title     = {Evaluating the Hydrological Cycle over Land Using the Newly-Corrected Precipitation Climatology from the Global Precipitation Climatology Centre (GPCC)}, 
  volume    = {8}, 
  url       = {http://dx.doi.org/10.3390/atmos8030052}, 
  doi       = {doi:10.3390/atmos8030052}, 
  number    = {12}, 
  journal   = {Atmosphere}, 
  publisher = {MDPI AG}, 
  year      = {2017}, 
  month     = {Mar}, 
  pages     = {52}
}
"""
    oset.comments = """
time_period: %d-%02d through %d-%02d; temporal_resolution: monthly; spatial_resolution: 0.5 degree; units: %s""" % (dt[0].year,dt[0].month,dt[-1].year,dt[-1].month,D.units)
    oset.convention = "CF-1.8"
