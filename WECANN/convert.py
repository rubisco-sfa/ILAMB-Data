import os
import time
from netCDF4 import Dataset
from ILAMB.constants import mid_months,bnd_months
import numpy as np

# download source
remote_source = "https://avdc.gsfc.nasa.gov/pub/data/project/WECANN/WECANN_v1.0.nc"
local_source = os.path.basename(remote_source)
if not os.path.isfile(local_source): urlretrieve(remote_source, local_source)   
download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))
generate_stamp = time.strftime('%Y-%m-%d')

# parse the source
dset = Dataset(local_source)
date = ["".join(row) for row in dset.variables["Time"][...].data.astype('str').T]
year = [int(row[:4]) for row in date]
month = [int(row[-2:]) for row in date]
t = []; tb = []
for i in range(len(year)):
    t.append((year[i]-1850)*365+mid_months[month[i]-1])
    tb.append([(year[i]-1850)*365+bnd_months[month[i]-1],
               (year[i]-1850)*365+bnd_months[month[i]]])
t = np.asarray(t)
tb = np.asarray(tb)
lat = dset.variables['Latitude'][0,:].data
lon = dset.variables['Longitude'][:,0].data
res = np.hstack([np.abs(np.diff(lat)),np.abs(np.diff(lon))]).mean()

for cf_name,var_name in zip(["gpp","hfss","hfls"],
                            ["GPP","H"   ,"LE"  ]):
    data = np.swapaxes(dset.variables[var_name][...],1,2)
    data = np.ma.masked_invalid(data)
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
        D.units = dset[var_name].Units.replace("gC","g")
        with np.errstate(invalid='ignore'):
            D.actual_range = np.asarray([data.min(),data.max()])

        oset.title = "Water, Energy, and Carbon with Artificial Neural Networks (WECANN)"
        oset.version = "1"
        oset.institutions = "Columbia University"
        oset.source = "Solar Induced Fluorescence (SIF), Air Temperature, Precipitation, Net Radiation, Soil Moisture, and Snow Water Equivalent"
        oset.history = """
%s: downloaded %s;
%s: converted to ILAMB-ready netCDF""" % (download_stamp, remote_source, generate_stamp)
        oset.references  = """
@ARTICLE{Alemohammad2017,
  author = {Alemohammad, S. H. and Fang, B. and Konings, A. G. and Aires, F. and Green, J. K. and Kolassa, J. and Miralles, D. and Prigent, C. and Gentine, P.},
  title= {Water, Energy, and Carbon with Artificial Neural Networks (WECANN): a statistically based estimate of global surface turbulent fluxes and gross primary productivity using solar-induced fluorescence},
  journal = {Biogeosciences},
  volume = {14},
  year = {2017},
  number = {18},
  page = {4101--4124},
  doi = {https://doi.org/10.5194/bg-14-4101-2017}
}"""
        oset.comments = """
time_period: %d-%02d through %d-%02d; temporal_resolution: monthly; spatial_resolution: %.1f degree; units: %s""" % (year[0],month[0],year[-1],month[-2],res,D.units)
        oset.convention = "CF-1.8"
