import numpy as np
from netCDF4 import Dataset
import datetime
import math

# set up Data directory 
DataDir = "./"

# set up initial and final years of data
start_yr = 2017
end_yr   = 2017

remote_source = "https://climate.esa.int/en/odp/#/project"
local_source  = 'ESACCI-BIOMASS-L4-AGB-MERGED-100m-2017-fv1.0.nc'
stamp1        = '2020-09-18'
stamp2        = '2019-03-14'

sourceID = "ESACCI"
instit1  = "United space in Europe"
instit2  = "The European Space Agency's (ESA's) Climate Change Initiative (CCI) programme, the Biomass CCI team"

datestr = str(datetime.datetime.now())
TmpStr  = datestr.split(' ')
stamp3  = TmpStr[0]

long_name = 'global forest above ground biomass'
period = "2016-12-31-2017-12-30"
origtr = "climatology"
origsr = "100 m"
origut = "Mg/ha biomass"
finltr = "2016-12-31-2017-12-30"
finlsr = "0.5 degree"
finlut = "kg/m2 carbon"

# Create temporal dimension
nyears    = end_yr - start_yr + 1
nmonth    = 12
ndays  = np.asarray([31,28,31,30,31,30,31,31,30,31,30,31],dtype=float)
smonth = np.asarray(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'])
month_bnd = np.asarray([0,365],dtype=float)
tbnd0  = np.asarray([((np.arange(nyears)*365)[:,np.newaxis]+month_bnd[:-1]).flatten(),
                    ((np.arange(nyears)*365)[:,np.newaxis]+month_bnd[+1:]).flatten()]).T
tbnd0 += (start_yr-1850)*365
tbnd0.shape

tbnd   = np.ma.masked_array(np.zeros((1,2)))

tbnd[0,0] = tbnd0[0,0]
tbnd[0,1] = tbnd0[nyears-1,1]

t     = tbnd.mean(axis=1)
t.shape

# Create new spatial dimension
res    = 0.5
latbnd = np.asarray([np.arange(- 90    , 90     ,res),
                     np.arange(- 90+res, 90+0.01,res)]).T
lonbnd = np.asarray([np.arange(-180    ,180     ,res),
                     np.arange(-180+res,180+0.01,res)]).T
lat    = latbnd.mean(axis=1)
lon    = lonbnd.mean(axis=1)

# Create some fake data
data   = np.ma.masked_array(np.zeros((t.size,lat.size,lon.size)))
area   = np.ma.masked_array(np.zeros((lat.size,lon.size)))

nlat = lat.size
nlon = lon.size

R    = 6371007.181

for ny in range(nlat):
    if ny == 0:
       dlat = abs((lat[1]-lat[0])*0.01745)
    else:
       dlat = abs((lat[ny]-lat[ny-1])*0.01745)
    dy = R*dlat
    for nx in range(nlon):
        if nx==0:
           dlon = abs((lon[1]-lon[0])*0.01745)
        else:
           dlon = abs((lon[nx]-lon[nx-1])*0.01745)
        dx = R*math.cos(0.01745*lat[ny])*dlon
        area[ny,nx] = dx*dy

# read single netCDF file
filename = DataDir + '/' + local_source
ag=Dataset(filename,'r',format='NETCDF4')

agb1 = ag.variables['agb']
lat1 = ag.variables['lat']
lon1 = ag.variables['lon']

agb  = agb1[0,::-1,:]
lat1 = lat1[::-1]

del agb1

nlat1 = lat1.size
nlon1 = lon1.size

nlat2 = int(nlat1/2)
nlon2 = int(nlon1/2)

biomass = np.ma.masked_array(np.zeros((nlat,nlon)))

biomass[...] = 0.

for ny in range(nlat1):
    if ny == 0:
       dlat = abs((lat1[1]-lat1[0])*0.01745)
    else:
       dlat = abs((lat1[ny]-lat1[ny-1])*0.01745)
    dy = R*dlat
    for nx in range(nlon1):
        if nx==0:
           dlon = abs((lon1[1]-lon1[0])*0.01745)
        else:
           dlon = abs((lon1[nx]-lon1[nx-1])*0.01745)
        dx = R*math.cos(0.01745*lat1[ny])*dlon
        areas = dx*dy*1.0e-4

        #+++++ to generate data in 0.5x0.5 ++++
        iy   = int(lat1[ny]*2+90.*2)
        ix   = int(lon1[nx]*2+180.*2)

        # obtain total biomass mass at each grid cell
        biomass[iy,ix] = biomass[iy,ix] + agb[ny,nx]*areas

# convert total mass (Mg) to density (Mg/m2)
biomass[...] = biomass[...]/area[...]

# convert biomass to biomass carbon 
biomass[:,:] = biomass[:,:]/2.

# convert the unit from Mg/m2 to Kg/m2
biomass[:,:] = biomass[:,:]*1000.

data[0,:,:] = biomass[:,:]

with Dataset(DataDir + "biomass.nc", mode="w") as dset:

    # Create netCDF dimensions
    dset.createDimension("time",size=  t.size)
    dset.createDimension("lat" ,size=lat.size)
    dset.createDimension("lon" ,size=lon.size)
    dset.createDimension("nb"  ,size=2       )

    # Create netCDF variables
    T  = dset.createVariable("time"       ,t.dtype   ,("time"     ))
    TB = dset.createVariable("time_bounds",t.dtype   ,("time","nb"))
    X  = dset.createVariable("lat"        ,lat.dtype ,("lat"      ))
    XB = dset.createVariable("lat_bounds" ,lat.dtype ,("lat","nb" ))
    Y  = dset.createVariable("lon"        ,lon.dtype ,("lon"      ))
    YB = dset.createVariable("lon_bounds" ,lon.dtype ,("lon","nb" ))
    D  = dset.createVariable("biomass"    ,data.dtype,("time","lat","lon"), fill_value = -999., zlib=True)

    # Load data and encode attributes
    # time
    T [...]         = t
    T.units         = "days since 1850-01-01"
    T.calendar      = "noleap"
    T.bounds        = "time_bounds"
    TB[...]         = tbnd
    T.standard_name = "time"
    T.long_name     = "time"

    # lat
    X [...]    = lat
    X.units    = "degrees_north"
    XB[...]    = latbnd
    X.standard_name = "latitude"
    X.long_name     = "latitude"

    # lon
    Y [...]    = lon
    Y.units    = "degrees_east"
    YB[...]    = lonbnd
    Y.standard_name = "longitude"
    Y.long_name     = "longitude"

    # data
    D[...] = data
    D.units = "kg m-2"
    D.standard_name = "global forest biomass carbon"
    D.long_name     = long_name
    D.actual_range = np.asarray([data.min(),data.max()])
    
    dset.title       = "ESA CCI above-ground biomass product level 4, year 2017"
    dset.version     = "1"
    dset.institutions = "%s; %s" % (instit1, instit2)
    dset.source      = "This dataset contains a global map of above-ground biomass of the epoch 2017 obtained from L-and C-band spaceborne SAR backscatter, placed onto a regular grid."
    dset.history     = """
%s: downloaded source from %s;
%s: convert to ILAMB required spatial resolution;
%s: converted biomass to biomass carbon and saved to ILAMB required netCDF""" % (stamp1, remote_source, stamp2, stamp3)
    dset.references  = """
@ARTICLE{Santoro2019,
  author = {Santoro, M., Cartus, O.},
  title = {ESA Biomass Climate Change Initiative (Biomass_cci): Global datasets of forest above-ground biomass for the year 2017, v1. Centre for Environmental Data Analysis},
  journal = {},
  year = {2019},
  number = {},
  page = {},
  doi = {http://dx.doi.org/10.5285/bedc59f37c9545c981a839eb552e4084}
}"""
    dset.comments = """
time_period: %s; original_temporal_resolution: %s; original_spatial_resolution: %s; original_units: %s; final_temporal_resolution: %s; final_spatial_resolution: %s; final_units: %s""" % (period, origtr, origsr, origut, finltr, finlsr, finlut)
    dset.convention = "CF-1.7"
