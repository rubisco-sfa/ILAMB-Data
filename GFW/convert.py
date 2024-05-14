import os
import time
import xarray as xr
import rioxarray as rxr
from rasterio.enums import Resampling
from rasterio import Affine
from urllib.request import urlretrieve
from osgeo import gdal, osr
import numpy as np
import cftime as cf
import datetime

# vars
sdate = datetime.datetime(2020, 1, 1)
edate = datetime.datetime(2050, 1, 1)

# data sources
remote_data = 'https://www.arcgis.com/sharing/rest/content/items/f950ea7878e143258a495daddea90cc0/data'
local_data = 'sequestration_rate_mean_aboveground_full_extent_Mg_C_ha_yr.tif'
remote_u_data = 'https://www.arcgis.com/sharing/rest/content/items/d28470313b8e443aa90d5cbcd0f74163/data'
local_u_data = 'sequestration_error_ratio_layer_in_full_extent.tif'

# loop through source and uncertainty:
# for data in [remote_data, remote_u_data]:

# Open the input raster
download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_data)))
if not os.path.isfile(local_data):
    urlretrieve(remote_data, local_data)
data = rxr.open_rasterio(local_data)

# ensure data is not projected and is wgs84
epsg_code = int(data.rio.crs.to_epsg())
if epsg_code != 4326:
    data = data.rio.reproject(crs='EPSG:4326')

# coarsen to 0.5 degrees by averaging (right approach?)
target_res = 0.5
data = data.rio.write_nodata(np.nan, inplace=True)
resampled_data = data.coarsen(band=1, 
                              x=(int((data.rio.height * abs(data.rio.resolution()[0])) / target_res)),
                              y=(int((data.rio.width * abs(data.rio.resolution()[0])) / target_res))
                             ).mean()

# rename x/y dims to lat/lon
ds = resampled_data.rename({'x': 'lon', 'y': 'lat'})

# set time bounds as np array
tb_arr = np.asarray([
    [[cf.DatetimeNoLeap(sdate.year, sdate.month, sdate.day)]],
    [[cf.DatetimeNoLeap(edate.year, edate.month, edate.day)]]
]).T

# Create 2D bounds array (tb_da) and set time as mean between bounds
tb_da = xr.DataArray(tb_arr, dims=('time', 'nv'))
ds = ds.expand_dims(time=tb_da.mean(dim='nv'))
# add bounds to time
ds.time.attrs['bounds'] = tb_da


# below is copied from anoter script:

# # create header
# ds.attrs = {
#     "title": "Global Aboveground and Belowground Biomass Carbon Density Maps for the Year 2010",
#     "version": "1",
#     "institutions": "University of Wisconsin-Madison",
#     "source": "25 datasets spanning woody, grassland, tundra, and cropland cover types, and reporting above and below ground biomass (https://www.nature.com/articles/s41597-020-0444-4/tables/2)",
#     "history": f"""
# {download_stamp}: Data requested and downloaded from https://doi.org/10.3334/ORNLDAAC/1763;
# {time.strftime('%Y-%m-%d')}: Coarsened (mass conserving) to 0.5 degree and converted to ILAMB-ready netcdf files.""",
#     "references": """
# @ARTICLE{Spawn2020a,
#   author = {Spawn, S.A., and H.K. Gibbs},
#   title = {Global Aboveground and Belowground Biomass Carbon Density Maps for the Year 2010},
#   journal = {ORNL DAAC},
#   year = {2020},
#   doi = {https://doi.org/10.3334/ORNLDAAC/1763}
# }
# """,
# }