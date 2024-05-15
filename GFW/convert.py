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
github_path = 'https://github.com/rubisco-sfa/ILAMB-Data/blob/msteckler/GFW/convert.py'

# data sources
remote_data = 'https://www.arcgis.com/sharing/rest/content/items/f950ea7878e143258a495daddea90cc0/data'
local_data = 'sequestration_rate_mean_aboveground_full_extent_Mg_C_ha_yr.tif'
remote_u_data = 'https://www.arcgis.com/sharing/rest/content/items/d28470313b8e443aa90d5cbcd0f74163/data'
local_u_data = 'sequestration_error_ratio_layer_in_full_extent.tif'

# Open the input raster
download_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_data)))
if not os.path.isfile(local_data):
    urlretrieve(remote_data, local_data)
data = rxr.open_rasterio(local_data, band_as_variable=True)

# ensure data is not projected and is wgs84
epsg_code = int(data.rio.crs.to_epsg())
if epsg_code != 4326:
    data = data.rio.reproject(crs='EPSG:4326')

# resample to 0.5 degrees
target_res = 0.5
resampled_data = data.coarsen(x=(int(target_res / abs(data.rio.resolution()[0]))),
                              y=(int(target_res / abs(data.rio.resolution()[1])))).mean()

# rename variable and x/y dims to lat/lon
ds = resampled_data.rename({'x': 'lon', 'y': 'lat', 'band_1':'c_uptake_rate'})

# numpy array of time bounds
tb_arr = np.asarray([
    [cf.DatetimeNoLeap(sdate.year, sdate.month, sdate.day)],
    [cf.DatetimeNoLeap(edate.year, edate.month, edate.day)]
]).T

# xarray data array of time bounds
tb_da = xr.DataArray(tb_arr, dims=('time', 'nv'))

# add time dimension and time bounds attribute
ds = ds.expand_dims(time=tb_da.mean(dim='nv'))
ds['time_bounds'] = tb_da

# define attributes
t_attrs = {
    'long_name':'time',
    'units':'days since 2020-01-01',
    'calendar':'noleap',
    'bounds':'time_bounds',
}
y_attrs = {
    'long_name':'latitude',
    'units':'degrees_north',
}
x_attrs = {
    'long_name':'longitude',
    'units':'degrees_east',
}
v_attrs = {
    'long_name': 'mean aboveground biomass carbon accumulation rate',
    'units': 'Mg ha-1 yr-1',
    'ancillary_variables': 'error_ratio',
}

# add variable attributes
ds['time'].attrs = t_attrs
ds['lat'].attrs = y_attrs
ds['lon'].attrs = x_attrs
ds['c_uptake_rate'].attrs = v_attrs

# edit global attributes
generate_stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_data)))
ds.attrs = {
    'title':'Carbon accumulation potential from natural forest regrowth in forest and savanna biomes',
    'institution':'Global Forest Watch',
    'source':'Ensemble of 100 random forest models using ground measurements and 66 co-located environmental covariate layers',
    'history':f"""{download_stamp}: downloaded source from {remote_data}
{generate_stamp}: resampled to 0.5 degree resolution and created CF-compliant meta-data in {github_path}""",
    'references': """
@article{Cook-Patton2020,
author  = {Susan C. Cook-Patton, Sara M. Leavitt, David Gibbs, Nancy L. Harris, Kristine Lister, Kristina J. Anderson-Teixeira, Russell D. Briggs, Robin L. Chazdon, Thomas W. Crowther, Peter W. Ellis, Heather P. Griscom, Valentine Herrmann, Karen D. Holl, Richard A. Houghton, Cecilia Larrosa, Guy Lomax, Richard Lucas, Palle Madsen, Yadvinder Malhi, Alain Paquette, John D. Parker, Keryn Paul, Devin Routh, Stephen Roxburgh, Sassan Saatchi, Johan van den Hoogen, Wayne S. Walker, Charlotte E. Wheeler, Stephen A. Wood, Liang Xu & Bronson W. Griscom},
title   = {Mapping carbon accumulation potential from global natural forest regrowth},
journal = {Nature},
year    = {2020},
volume  = {585},
pages   = {545--550},
doi     = {https://doi.org/10.1038/s41586-020-2686-x}}""",
    'comment':''}

# export and encode any vars that need to be encoded
ds.to_netcdf('cookpatton2020_0.5deg.nc', format="NETCDF4", engine="netcdf4")