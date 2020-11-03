import numpy as np
from netCDF4 import Dataset
import pylab as plt
import os
import time
from urllib.request import urlretrieve
from ILAMB.constants import bnd_months

# define sources
remote_sources = ["http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2003.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2004.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2005.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2006.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2007.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2008.nc",
                  "http://dapds00.nci.org.au/thredds/fileServer/ks32/ARCCSS_Data/CLASS/v1-1/CLASS_v1-1_2009.nc"]
gist_source = "https://gist.github.com/nocollier/d73585731756fa472731065389af45dc"
local_sources = [os.path.basename(s) for s in remote_sources]

# ensure we have downloaded the data
for remote_source,local_source in zip(remote_sources,local_sources):
    if not os.path.isfile(local_source): urlretrieve(remote_source, local_source)   
    stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))

# create time and its bounds in days since 1850-1-1
years = np.asarray([int(y.strip(".nc").split("_")[-1]) for y in local_sources])
tb = np.hstack([(((years-1850)*365)[:,np.newaxis]+bnd_months[:-1]).flatten(),(years[-1]+1-1850)*365])
tb = np.asarray([tb[:-1],tb[1:]]).T
t  = tb.mean(axis=1)

# get spatial grid from one file
lat, lon = None, None
with Dataset(local_sources[0]) as dset:
    lat = dset.variables['lat'][...]
    lon = dset.variables['lon'][...]

for var_name in ['mrro','pr','dw','hfls','hfss','hfds','rs']:

    # piece together data
    data      = np.zeros((t.size,lat.size,lon.size))
    data_bnds = np.zeros((t.size,lat.size,lon.size,2))
    units, long_name, standard_name = None, var_name, var_name
    for local_source in local_sources:
        print(local_source,var_name)
        with Dataset(local_source) as dset:
            y  = int(local_source.strip(".nc").split("_")[-1])
            d  = dset.variables[var_name        ][...]
            db = dset.variables[var_name + "_sd"][...]
            data     [12*(y-years[0]):12*(y-years[0]+1),...  ] = d
            data_bnds[12*(y-years[0]):12*(y-years[0]+1),...,0] = d-db
            data_bnds[12*(y-years[0]):12*(y-years[0]+1),...,1] = d+db
            units = dset.variables[var_name].units
            if "standard_name" in dset.variables[var_name].ncattrs():
                standard_name = dset.variables[var_name].standard_name
            if "long_name" in dset.variables[var_name].ncattrs():
                long_name = dset.variables[var_name].long_name                

    # write output
    data = np.ma.masked_invalid(data)
    data_bnds = np.ma.masked_invalid(data_bnds)
    with Dataset("%s.nc" % var_name, mode="w") as dset:

        # dimensions
        dset.createDimension("time", size = t.size)
        dset.createDimension("lat", size = lat.size)
        dset.createDimension("lon", size = lon.size)
        dset.createDimension("nb", size = 2)
        
        # time
        T = dset.createVariable("time", t.dtype, ("time"))
        T[...] = t
        T.units = "days since 1850-01-01 00:00:00"
        T.calendar = "noleap"
        T.bounds = "time_bounds"
        
        # time bounds
        TB = dset.createVariable("time_bounds", t.dtype, ("time", "nb"))
        TB[...] = tb
        
        # latitude
        X = dset.createVariable("lat", lat.dtype, ("lat"))
        X[...] = lat
        X.standard_name = "latitude"
        X.long_name = "site latitude"
        X.units = "degrees_north"
        
        # longitude
        Y = dset.createVariable("lon", lon.dtype, ("lon"))
        Y[...] = lon
        Y.standard_name = "longitude"
        Y.long_name = "site longitude"
        Y.units = "degrees_east"
                
        # data
        D = dset.createVariable(var_name, data.dtype, ("time", "lat", "lon"), zlib=True)
        D[...] = data
        D.units = units
        D.standard_name = standard_name
        D.long_name = long_name
        D.bounds = "%s_bnds" % (var_name)

        # data
        DB = dset.createVariable("%s_bnds" % (var_name), data.dtype, ("time", "lat", "lon", "nb"), zlib=True)
        DB[...] = data_bnds
        DB.units = units
        DB.standard_name = "standard error for %s" % standard_name
        DB.long_name = "standard error for %s" % long_name

        with np.errstate(invalid='ignore'):
            D.actual_range = np.asarray([data.min(),data.max()])
            DB.actual_range = np.asarray([data_bnds.min(),data_bnds.max()])

        dset.title = "Conserving Land-Atmosphere Synthesis Suite (CLASS) v1.1"
        dset.version = "1.1"
        dset.institution = "University of New South Wales"
        dset.source = "Ground Heat Flux (GLDAS, MERRALND, MERRAFLX, NCEP_DOII, NCEP_NCAR), Sensible Heat Flux(GLDAS, MERRALND, MERRAFLX, NCEP_DOII, NCEP_NCAR, MPIBGC, Princeton), Latent Heat Flux(DOLCE1.0), Net Radiation (GLDAS, MERRALND, NCEP_DOII, NCEP_NCAR, ERAI, EBAF4.0), Precipitation(REGEN1.1), Runoff(LORA1.0), Change in Water storage(GRACE(GFZ, JPL, CSR))"
        dset.history = """
%s: downloaded source from %s
%s: converted to ILAMB netCDF4 with %s""" % (stamp, "[" + ",".join(remote_sources) + "]", stamp, gist_source)
        dset.references = """
@InCollection{Hobeichi2019,
  author = 	 {Sanaa Hobeichi},
  title = 	 {Conserving Land-Atmosphere Synthesis Suite (CLASS) v1.1},
  booktitle = 	 {NCI National Research Data Collection},
  doi =          {doi:10.25914/5c872258dc183},
  year = 	 {2019}
}
@article{Hobeichi2020,
    author = {Hobeichi, Sanaa and Abramowitz, Gab and Evans, Jason},
    title = {Conserving Land–Atmosphere Synthesis Suite (CLASS)},
    journal = {Journal of Climate},
    volume = {33},
    number = {5},
    pages = {1821-1844},
    year = {2020},
    month = {01},
    doi = {doi:10.1175/JCLI-D-19-0036.1},
}
"""
