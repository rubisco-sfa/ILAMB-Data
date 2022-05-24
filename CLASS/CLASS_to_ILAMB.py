import numpy as np
from netCDF4 import Dataset
import pylab as plt
import os
import time
from urllib.request import urlretrieve
import xarray as xr
import cftime
import numpy as np

# define sources
remote_sources = ["http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2003.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2004.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2005.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2006.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2007.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2008.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2009.nc"]
gist_source = "https://github.com/rubisco-sfa/ILAMB-Data/blob/master/CLASS/CLASS_to_ILAMB.py"
local_sources = [os.path.basename(s) for s in remote_sources]

# ensure we have downloaded the data
for remote_source,local_source in zip(remote_sources,local_sources):
    if not os.path.isfile(local_source): urlretrieve(remote_source, local_source)   
    stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))

ds = xr.open_mfdataset(local_sources)

# rename some variables to follow conventions
ds = ds.rename({'hfds':'hfdsl','hfds_sd':'hfdsl_sd',
                'rs'  :'rns'  ,  'rs_sd':  'rns_sd'})

# noleap calendar is the ILAMB default, add bounds
ds['time'] = [cftime.DatetimeNoLeap(t.dt.year,t.dt.month,t.dt.day) for t in ds['time']]
tb = np.asarray([[cftime.DatetimeNoLeap(t.dt.year,t.dt.month,1) for t in ds['time']],
                 [cftime.DatetimeNoLeap(t.dt.year + (t.dt.month==12),
                                        1 if t.dt.month==12 else t.dt.month+1,
                                        1) for t in ds['time']]]).T
ds['time_bounds'] = xr.DataArray(tb,dims = ('time','nv'))
ds['time'].encoding['units']= "days since 2003-01-01"
ds['time'].attrs['bounds'] = 'time_bounds'

for v in ['mrro','pr','hfls','hfss','hfdsl','rns']:
    #ds[v].attrs['bounds'] = '%s_sd' % v
    ds[v].to_netcdf("%s.nc" % v)
    ds['time_bounds'].to_netcdf("%s.nc" % v,mode='a')

    # uncertainty, for now I have this 2-sided, need to allow 1-sided
    # note: the uncertainty for mrro and pr are causing large plotting ranges, fix!
    #vb = '%s_sd' % v
    #da = ds[vb]
    #db = xr.concat([ds[v]-da,ds[v]+da],dim='nv').transpose("time","lat","lon","nv")
    #db.name = vb
    #db.to_netcdf("%s.nc" % v,mode='a')
    
