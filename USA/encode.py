import geopandas as gp
import numpy as np
from shapely.geometry import Point
from netCDF4 import Dataset

world = gp.read_file(gp.datasets.get_path('naturalearth_lowres'))
usa = world[world['name']=='United States of America'].iloc[0]['geometry']

# Create the lat/lon dimensions
res     = 0.5
latbnd  = np.asarray([np.arange(- 90    , 90     ,res),
                      np.arange(- 90+res, 90+0.01,res)]).T
lonbnd  = np.asarray([np.arange(-180    ,180     ,res),
                      np.arange(-180+res,180+0.01,res)]).T
lat     = latbnd.mean(axis=1)
lon     = lonbnd.mean(axis=1)
LAT,LON = np.meshgrid(lat,lon,indexing='ij')
ID = np.zeros(LAT.shape,dtype=int)
for i,j in np.ndindex(ID.shape):
    ID[i,j] = 0 if Point(LON[i,j],LAT[i,j]).within(usa) else -9999

labels = np.asarray(['USA'])
lbl    = np.asarray(['global'],dtype='U')

with Dataset("GlobalUSA.nc",mode="w") as dset:
    
    # Create netCDF dimensions
    dset.createDimension("lat" ,size=lat.size)
    dset.createDimension("lon" ,size=lon.size)
    dset.createDimension("nb"  ,size=2       )
    dset.createDimension("n"   ,size=lbl.size)

    # Create netCDF variables
    X  = dset.createVariable("lat"        ,lat.dtype,("lat"      ))
    XB = dset.createVariable("lat_bounds" ,lat.dtype,("lat","nb" ))
    Y  = dset.createVariable("lon"        ,lon.dtype,("lon"      ))
    YB = dset.createVariable("lon_bounds" ,lon.dtype,("lon","nb" ))
    I  = dset.createVariable("ids"        ,ID .dtype,("lat","lon"),fill_value=-9999)
    L  = dset.createVariable("labels"     ,lbl.dtype,("n"        ))
    N  = dset.createVariable("names"      ,labels.dtype,("n"     ))
    
    # Load data and encode attributes
    X [...] = lat
    X.units = "degrees_north"
    XB[...] = latbnd
    
    Y [...] = lon
    Y.units = "degrees_east"
    YB[...] = lonbnd
    
    I[...]  = ID
    I.labels= "labels"
    I.names = "names"
    
    L[...]  = lbl
    N[...]  = labels

