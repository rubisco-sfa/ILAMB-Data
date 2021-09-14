import os
import time
from netCDF4 import Dataset
import xarray as xr
import cftime as cf
import numpy as np
from ILAMB.constants import bnd_months

local_source = "gpp.nc"
download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))
generate_stamp = time.strftime('%Y-%m-%d')

dset = xr.load_dataset("gpp.nc")
tb = np.array([[(t.year-1850)*365.+bnd_months[t.month-1],
                (t.year-1850)*365.+bnd_months[t.month]] for t in dset.time.data])
t = tb.mean(axis=1)
lat = dset.lat
lon = dset.lon
data = dset["gpp"]

# Fixes to the xarray / interpolation / masking fiasco
mask = np.isnan(data)
data = np.nan_to_num(data,-999)
mask[:,:66,:] = True
data[:,:66,:] = -999
mask[...,0] = True
data[...,0] = -999
data = np.ma.masked_array(data,mask=mask)

with Dataset("gpp_0.5x0.5.nc", mode="w") as oset:
    
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
    D = oset.createVariable("gpp", data.dtype, ("time", "lat", "lon"), fill_value=-999, zlib=True)
    D[...] = data
    D.units = "g m-2 d-1"
    with np.errstate(invalid='ignore'):
        D.actual_range = np.asarray([data.min(),data.max()])        
    oset.title = "Global 4 km resolution monthly gridded Gross Primary Productivity (GPP) data set dervied from FLUXNET2015"
    oset.version = "1.0"
    oset.institutions = "Oak Ridge National Laboratory"
    oset.source = ""
    oset.history = """
%s: downloaded ;
%s: converted to netCDF""" % (download_stamp, generate_stamp)
    oset.references  = """
@ARTICLE{Kumar2019,
  author = {Kumar, J and Hoffman, F and Hargrove, W and Collier, N},
  title = {Global 4 km resolution monthly gridded Gross Primary Productivity (GPP) data set dervied from FLUXNET2015},
  journal = {NGEE Tropics Data Collection},
  year = {2019},
  doi = {10.15486/NGT/1279968}
}
@ARTICLE{Kumar2016,
  author = {Kumar, J and Hoffman, F and Hargrove, W and Collier, N},
  title = {Understanding the representativeness of FLUXNET for upscaling carbon flux from eddy covariance measurements},
  journal = {Earth Syst. Sci. Data Discuss},
  year = {2016},
  doi = {essd-2016-36}
}"""
    oset.comments = ""
    oset.convention = "CF-1.8"
