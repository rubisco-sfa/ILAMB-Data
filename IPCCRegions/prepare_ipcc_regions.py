"""
Script to create netcdf with new AR6 regions into netcdf for use with ILAMB. 

Inne Vanderkelen - June 2020
"""

from netCDF4 import Dataset
import numpy as np
import geopandas as gpd 
from shapely.geometry import Point
from urllib.request import urlretrieve
import os,time

# ---------------------------
# Create the netcdf file

# Create the lat/lon dimensions
res    = 0.5
latbnd = np.asarray([np.arange(- 90    , 90     ,res),
                     np.arange(- 90+res, 90+0.01,res)]).T
lonbnd = np.asarray([np.arange(-180    ,180     ,res),
                     np.arange(-180+res,180+0.01,res)]).T
lat    = latbnd.mean(axis=1)
lon    = lonbnd.mean(axis=1)

# Create the number array, initialize to a missing value
miss   = -999
ids    = np.ones((lat.size,lon.size),dtype=int)*miss

# Ensure we have downloaded the data
remote_source = "https://github.com/SantanderMetGroup/ATLAS/raw/master/reference-regions/CMIP6_referenceRegions_shapefile.zip"
local_source  = os.path.basename(remote_source)
if not os.path.isfile(local_source):
    urlretrieve(remote_source, local_source)
os.system('unzip %s' % local_source)
stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))

# ---------------------------
# read in CMIP6 regions (to be replaced by final version once paper in accepted)
# corresponding paper: https://essd.copernicus.org/preprints/essd-2019-258/ 
regions = gpd.read_file('CMIP6_referenceRegions_shapefile/CMIP6_referenceRegions.shp')

# cut off antarctica and oceans
regions = regions[0:41]
names = regions.V3.values
geometries = regions.geometry.values

labels = ["Arctic - Greenland/Iceland",
          "North America - Northeast Canada",
          "North America - Central America",
          "North America - Eastern America",
          "North America - Northwest America",
          "North America - North America",
          "Central America - North",
          "Central America - South",
          "Central America - Caribbean",
          "South America - Northwest",
          "South America - Monsoon",
          "South America - South",
          "South America - Southwest",
          "South America - Southeast",
          "South America - North",
          "South America - Northeast",
          "Europe - North",
          "Europe - Central",
          "Europe - Eastern",
          "Europe/Africa - Mediterranean",
          "Africa - West",
          "Africa - Sahara",
          "Africa - Northeast",
          "Africa - Central East",
          "Africa - Southwest",
          "Africa - Southeast",
          "Africa - Central",
          "Asia - Russian Arctic",
          "Asia - Russian Far East",
          "Asia - Eastern Siberia",
          "Asia - Western Siberia",
          "Asia - Western Central",
          "Asia - Tibetan Plateau",
          "Asia - Eastern",
          "Asia - Arabian Peninsula",
          "Asia - Southern",
          "Asia - Southeastern",
          "Oceania - Northern",
          "Oceania - Central",
          "Oceania - Southern",
          "Oceania - New Zealand"]
labels = np.asarray(labels)

def inpolygon(polygon, xp, yp):
    return np.array([Point(x, y).intersects(polygon) for x, y in zip(xp, yp)],
                    dtype=np.bool)


# convert polygon geometries to grid per region
for ind,polygon in enumerate(geometries):
    print('processing region '+ names[ind] + ', '+str(ind)+' of '+str(len(geometries)))
    x, y = np.meshgrid(lon, lat)
    mask = inpolygon(polygon, x.ravel(), y.ravel())
    mask_2d = mask.reshape(len(lat),len(lon))

    # Paint the region with its number
    ids[np.where(mask_2d)] = ind


# Convert the ids to a masked array
ids = np.ma.masked_values(ids,miss)

# Create the array of labels -  in the order they are used. 
lbl = names.astype('U')

# Create netCDF dimensions
dset = Dataset("IPCCRegions.nc",mode="w")
dset.createDimension("lat" ,size=lat.size)
dset.createDimension("lon" ,size=lon.size)
dset.createDimension("nb"  ,size=2       )
dset.createDimension("n"   ,size=lbl.size)

# Create netCDF variables
X  = dset.createVariable("lat"        ,lat.dtype,("lat"      ))
XB = dset.createVariable("lat_bounds" ,lat.dtype,("lat","nb" ))
Y  = dset.createVariable("lon"        ,lon.dtype,("lon"      ))
YB = dset.createVariable("lon_bounds" ,lon.dtype,("lon","nb" ))
I  = dset.createVariable("ids"        ,ids.dtype,("lat","lon"))
L  = dset.createVariable("labels"     ,lbl.dtype,("n"        ))
N  = dset.createVariable("names"      ,labels.dtype,("n"     ))

# Load data and encode attributes
X [...] = lat
X.units = "degrees_north"
XB[...] = latbnd

Y [...] = lon
Y.units = "degrees_east"
YB[...] = lonbnd

I[...]  = ids
I.labels= "labels"
I.names = "names"

L[...]  = lbl
N[...]  = labels

dset.title = "IPCC climate reference regions for subcontinental analysis of climate model data"
dset.history = """
%s: downloaded source from %s
%s: converted to ILAMB netCDF4 with %s""" % (stamp,remote_source,stamp,"https://github.com/rubisco-sfa/ILAMB-Data/IPCCRegions")
dset.references = """
@Article{essd-2019-258,
  author = {Iturbide, M. and Guti\'errez, J. M. and Alves, L. M. and Bedia, J. and Cimadevilla, E. and Cofi\~no, A. S. and Cerezo-Mota, R. and Di Luca, A. and Faria, S. H. and Gorodetskaya, I. and Hauser, M. and Herrera, S. and Hewitt, H. T. and Hennessy, K. J. and Jones, R. G. and Krakovska, S. and Manzanas, R. and Mar\'{\i}nez-Castro, D. and Narisma, G. T. and Nurhati, I. S. and Pinto, I. and Seneviratne, S. I. and van den Hurk, B. and Vera, C. S.},
  title = {An update of IPCC climate reference regions for subcontinental analysis of climate model data: Definition and aggregated datasets},
  journal = {Earth System Science Data Discussions},
  volume = {2020},
  year = {2020},
  pages = {1--16},
  url = {https://essd.copernicus.org/preprints/essd-2019-258/},
  doi = {10.5194/essd-2019-258}
}

@Misc{workspace,
  title = {The Climate Change ATLAS: Datasets, code and virtual workspace},
  doi = {10.5281/zenodo.3688072}
}
"""

dset.close()
