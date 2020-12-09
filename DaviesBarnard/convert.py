import time
import numpy as np
import os
from netCDF4 import Dataset

# Get time information
download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime("fBNF.nc")))
generate_stamp = time.strftime('%Y-%m-%d')

# Extract information from the original file
dset = Dataset("fBNF.nc")
y0,yf = dset.variables["Time"][[0,-1]]+1979
tb = (np.asarray([[y0,yf+1]])-1850.)*365
t = tb.mean(axis=1)
lat = dset.variables["latitude"][...]
lon = dset.variables["longitude"][...]
data = dset.variables["fBNF"][0,...]
data.shape = (1,)+data.shape

# We will use the 1q and 3q values to define the 'bounds' of the data,
# ILAMB will use this as a measure of uncertainty
data_bnds = np.ma.masked_array(np.zeros(data.shape + (2,)),mask=False)
data_bnds[0,...,0] = dset.variables["fBNF_1q"][0,...]
data_bnds[0,...,1] = dset.variables["fBNF_3q"][0,...]

# NOTE: the 1q and 3q values are currently set to the same values as fBNF
print("fBNF    == fBNF_1q (%s)" % np.allclose(data            ,data_bnds[...,0]))
print("fBNF_1q == fBNF_3q (%s)" % np.allclose(data_bnds[...,1],data_bnds[...,0]))

with Dataset("fBNF_0.5x0.5.nc", mode="w") as oset:

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
    X.units = "degrees_north"
    
    # longitude
    Y = oset.createVariable("lon", lon.dtype, ("lon"))
    Y[...] = lon
    Y.standard_name = "longitude"
    Y.units = "degrees_east"
    
    # data
    D = oset.createVariable("fBNF", data.dtype, ("time", "lat", "lon"), zlib=True)
    D[...] = data
    D.long_name = "Biological Nitrogen Fixation"
    D.units = dset.variables['fBNF'].units.replace("kgN","kg")
    D.bounds = "fBNF_bnds"
    with np.errstate(invalid='ignore'):
        D.actual_range = np.asarray([data.min(),data.max()])

    # uncertainty
    DB = oset.createVariable("fBNF_bnds", data.dtype, ("time", "lat", "lon", "nb"), zlib=True)
    DB[...] = data_bnds
    with np.errstate(invalid='ignore'):
        DB.actual_range = np.asarray([data_bnds.min(),data_bnds.max()])
        
    oset.title = "Biological Nitrogen Fixation"
    oset.version = "1"
    oset.institutions = "University of Exeter"
    oset.source = "A comprehensive global meta analysis of field measurements"
    oset.history = """
%s: obtained dataset
%s: converted to netCDF""" % (download_stamp, generate_stamp)
    oset.references  = """
@article{Davies-Barnard2020,
author = {Davies-Barnard, T. and Friedlingstein, P.},
title = {The Global Distribution of Biological Nitrogen Fixation in Terrestrial Natural Ecosystems},
journal = {Global Biogeochemical Cycles},
volume = {34},
number = {3},
doi = {https://doi.org/10.1029/2019GB006387},
url = {https://agupubs.onlinelibrary.wiley.com/doi/abs/10.1029/2019GB006387},
year = {2020}
}"""
    oset.comments = """
time_period: single estimate from %d through %d; temporal_resolution: monthly; spatial_resolution: 0.5 degree; units: %s""" % (y0,yf,D.units)
    oset.convention = "CF-1.8"

