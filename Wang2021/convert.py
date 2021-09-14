import xarray as xr
import glob,time
import numpy as np
import pandas as pd
from ILAMB.constants import mid_months,bnd_months
import os
from netCDF4 import Dataset

rs = { 'ec_ors.nc':'https://drive.google.com/file/d/1fJnQ8P9WT6M1bSypbcZe2Cxqo_XwZsku/view?usp=sharing',
      'olc_ors.nc':'https://drive.google.com/file/d/1-v8QmowHzZrvUHWUdmSJw5rtwfxYJnfA/view?usp=sharing'}
ds = { 'ec_ors.nc':'Emergent Constraint techqniue of Mystakidis, 2016 (https://doi.org/10.1111/gcb.13217)',
      'olc_ors.nc':'Optimal Linear Combination technique of Hobeichi, 2018 (https://doi.org/10.5194/hess-22-1317-2018)'}
for fname in rs:
    cf_name = "mrsos"
    var_name = "sm"
    V = [fname]
    remote_source = rs[fname]
    dset = xr.open_dataset(V[0])
    dset = dset.sel({'depth':0.1},method='nearest')
    data = np.ma.masked_invalid(dset[var_name]) * 998. * 0.1 # vol-% to kg m-2
    download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(V[0])))
    generate_stamp = time.strftime('%Y-%m-%d')
    dt = pd.to_datetime(dset.time.data)
    t  = np.array([(t.year-1850)*365+mid_months[t.month-1] for t in dt])
    tb = np.array([[(t.year-1850)*365+bnd_months[t.month-1],
                    (t.year-1850)*365+bnd_months[t.month]] for t in dt])
    lat = dset.lat
    lon = dset.lon
    with Dataset("%s_%s.nc" % (cf_name,fname.split("_")[0]), mode="w") as oset:

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
        D.units = "kg m-2"
        with np.errstate(invalid='ignore'):
            D.actual_range = np.asarray([data.min(),data.max()])

        oset.title = "Observation-based global multilayer soil moisture products for 1970 to 2016"
        oset.version = "1"
        oset.institutions = "Oak Ridge National Laboratory"
        oset.source = "Offline land surface models, reanalysis, and satellite soil moisture datasets combined using the %s" % ds[fname]
        oset.history = """
%s: downloaded %s;
%s: selected top 10cm, converted unit from vol-%% to kg m-2""" % (download_stamp, remote_source, generate_stamp)
        oset.references  = """
@ARTICLE{Wang2021,
  author = {Wang, Y. and Mao, J. and Jin, M. and Hoffman, F. M. and Shi, X. and Wullschleger, S. D. and Dai, Y.},
  title = {Development of observation-based global multilayer soil moisture products for 1970 to 2016},
  journal = {Earth System Science Data},
  year = {2021},
  volume = {13},
  issue = {9},
  page = {4385--4405},
  doi = {10.5194/essd-13-4385-2021}
}"""
        oset.comments = """
time_period: %d-%02d through %d-%02d; temporal_resolution: monthly; spatial_resolution: 0.5 degree; units: %s""" % (dt[0].year,dt[0].month,dt[-1].year,dt[-1].month,D.units)
        oset.convention = "CF-1.8"
