import numpy as np
import xarray as xr
import rioxarray as rxr
import cftime as cf
from urllib.request import urlretrieve
import datetime
import os
import time

#####################################################
# set the parameters for this particular dataset
#####################################################

# variables
sdate = datetime.datetime(1960, 1, 1, 0, 0, 0)
edate = datetime.datetime(2022, 12, 31, 0, 0, 0)
proj = 'EPSG:4326'
github_path = 'https://github.com/rubisco-sfa/ILAMB-Data/blob/master/Wang2023/convert.py'
var = 'cSoil'
long_name = 'carbon mass in soil pool'
uncertainty = 'variation_coefficient'
units = 'kg m-2'
target_res = 0.5

# data sources
remote_data = 'https://zenodo.org/records/8057232/files/30cm_SOC_mean.tif?download=1'
local_data = '30cm_SOC_mean.tif'
remote_u_data = 'https://zenodo.org/records/8057232/files/30cm_SOC_CV.tif?download=1'
local_u_data = '30cm_SOC_CV.tif'

#####################################################
# functions for processing the data (in order)
#####################################################

# 1. download the data if not present in current dir
def download_raster(local_data, remote_data):
    if not os.path.isfile(local_data):
        urlretrieve(remote_data, local_data)
    download_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data)))

    return download_stamp

# 2. create xarray dataset
def create_xarray(local_data, local_u_data, uncertainty):

    def open_raster(local_data):

        # read a .tif file into an xarray
        data = rxr.open_rasterio(local_data, band_as_variable=True)
        epsg_code = int(data.rio.crs.to_epsg())
        if epsg_code != 4326:
            data = data.rio.reproject(crs=proj)
        return data
    
    # open data and error files
    data = open_raster(local_data)
    errd = open_raster(local_u_data)
    data = data.assign(uncertainty=errd['band_1'])
    return data

# 3. function to resample to 0.5 degrees
def coarsen(target_res, data):
    resampled_data = data.coarsen(x=(int(target_res / abs(data.rio.resolution()[0]))),
                                  y=(int(target_res / abs(data.rio.resolution()[1])))).mean()
    return resampled_data

# 4. create a properly formatted netcdf
def create_netcdf(data, 
                  short_name, long_name, uncertainty, 
                  sdate, edate, 
                  units,
                  local_data, remote_data, 
                  github_path, download_stamp):

    # rename bands and mask invalid data
    ds = data.rename({'x': 'lon', 'y': 'lat', 'band_1':short_name, 'uncertainty':uncertainty})
    ds = ds.where(ds > 0)

    # create time dimension
    tb_arr = np.asarray([
        [cf.DatetimeNoLeap(sdate.year, sdate.month, sdate.day)],
        [cf.DatetimeNoLeap(edate.year, edate.month, edate.day)]
    ]).T
    tb_da = xr.DataArray(tb_arr, dims=('time', 'nv'))
    ds = ds.expand_dims(time=tb_da.mean(dim='nv'))
    ds['time_bounds'] = tb_da

    # dictionaries for formatting each dimension and variable
    t_attrs = {'axis': 'T', 'long_name': 'time'}
    y_attrs = {'axis': 'Y', 'long_name': 'latitude', 'units': 'degrees_north'}
    x_attrs = {'axis': 'X', 'long_name': 'longitude', 'units': 'degrees_east'}
    v_attrs = {'long_name': long_name, 'units': units, 'ancillary_variables': uncertainty}
    e_attrs = {'long_name': f'{short_name} {uncertainty}', 'units': 1}

    # apply formatting
    ds['time'].attrs = t_attrs
    ds['time_bounds'].attrs['long_name'] = 'time_bounds'
    ds['lat'].attrs = y_attrs
    ds['lon'].attrs = x_attrs
    ds[short_name].attrs = v_attrs
    ds[uncertainty].attrs = e_attrs

    # to_netcdf will fail without this encoding:
    ds['time'].encoding['units'] = f'days since {sdate.strftime("%Y-%m-%d %H:%M:%S")}'
    ds['time'].encoding['calendar'] = 'noleap'
    ds['time'].encoding['bounds'] = 'time_bounds'
    ds['time_bounds'].encoding['units'] = f'days since {sdate.strftime("%Y-%m-%d %H:%M:%S")}'

    # edit global attributes
    generate_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data)))
    ds.attrs = {
        'title':'Upscaled soil organic carbon measurements in the US',
        'institution':'Oak Ridge National Laboratory',
        'source':'Upscaled site-based SOC measurements to the continental scale using multivariate geographic clustering (MGC) approach coupled with machine learning models',
        'history':f"""
{download_stamp}: downloaded source from {remote_data}
{generate_stamp}: resampled to 0.5 degree resolution
{generate_stamp}: ensured units were CMIP standard kg m-2
{generate_stamp}: created CF-compliant metadata
{generate_stamp}: details on this process can be found at {github_path}""",
        'references': """
@article{Wang2024,
author  = {Wang, Z. and Kumar, J. and Weintraub-Leff, S.R. and Todd-Brown, K. and Mishra, U. and Sihi, D.},
title   = {Upscaling Soil Organic Carbon Measurements at the Continental Scale Using Multivariate Clustering Analysis and Machine Learning},
journal = {J. Geophys. Res. Biogeosci.},
year    = {2024},
volume  = {129},
pages   = {},
doi     = {10.1029/2023JG007702}}""",
        'comment':'',
        'Conventions':'CF-1.11'}

    # tidy up
    ds['lat'] = ds['lat'].astype('float32')
    ds['lon'] = ds['lon'].astype('float32')
    ds = ds.drop_vars('spatial_ref')
    # sort latitude coords: negative to positive
    ds = ds.reindex(lat=list(reversed(ds.lat)))

    # export
    ds.to_netcdf('wang2024.nc', format='NETCDF4', engine='netcdf4')

def main():

    download_stamp = download_raster(local_data, remote_data)
    _ = download_raster(local_u_data, remote_u_data)

    ds = create_xarray(local_data, local_u_data, uncertainty)

    ds = coarsen(target_res, ds)

    ds = create_netcdf(ds, var, long_name, uncertainty, sdate, edate, 
                       units, local_data, remote_data, github_path, download_stamp)

if __name__ == '__main__':
    main()