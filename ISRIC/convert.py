from pathlib import Path
from osgeo import gdal
import subprocess
import os
import time
import datetime
import xarray as xr
import rioxarray as rxr
import numpy as np
import cftime as cf
import cfunits
from urllib.request import urlretrieve
from rasterio.enums import Resampling

#####################################################
# set the parameters for this particular dataset
#####################################################

# variable info
var = 'cSoil'
anc_var = 'coefficient_of_variation'
var_long_name = 'carbon mass in soil pool'
source_units = 't ha-1'
target_units = 'kg m-2'
sdate = datetime.datetime(1905, 3, 31, 0, 0, 0) # Found here: https://data.isric.org/geonetwork/srv/api/records/713396f4-1687-11ea-a7c0-a0481ca9e724
edate = datetime.datetime(2016, 7, 4, 0, 0, 0)

# data source info
remote_data = '/vsicurl/https://files.isric.org/soilgrids/latest/data' #vsicurl is GDAL-specific
local_data = 'ocs_0-30cm_mean'
local_u_data = 'ocs_0-30cm_uncertainty'
github_path = 'https://github.com/rubisco-sfa/ILAMB-Data/blob/master/ISRIC/convert.py'
output_path = 'soilgrids2_cSoil.nc'
cellsize = 1000 # download res (m) in homolosine epsg; I chose a higher res (but less than 0.5 in EPSG:4326) for processing speed
nodata = -32768

# regridding info
target_epsg = '4326'
res = 0.5
interp = 'average'

#####################################################
# functions in the order that they are used in main()
#####################################################

gdal.UseExceptions()

# 1. download data
def download_data(local_file, remote_file, cellsize, target_epsg):
    
    local_file_wext = f'{local_file}.tif'
    if not os.path.isfile(local_file_wext):

        # download data using GDAL in bash
        print('Creating VRT ...')
        create_vrt = (
            f'gdal_translate --config GDAL_HTTP_UNSAFESSL YES '
            f'-of VRT '
            f'-tr {cellsize} {cellsize} '
            f'{remote_file}/ocs/{local_file}.vrt '
            f'{local_file}.vrt'
            )
        print(create_vrt)
        process = subprocess.Popen(create_vrt.split(), stdout=None)
        process.wait()

        # reproject from Homolosine to target_epsg 4326
        print('Reprojecting VRT ...')
        reproject = (
            f'gdalwarp --config GDAL_HTTP_UNSAFESSL YES '
            f'-overwrite '
            f'-t_srs EPSG:{target_epsg} '
            f'-of VRT '
            f'-te -180 -90 180 90 '
            f'{local_file}.vrt '
            f'{local_file}_{target_epsg}.vrt'
            )
        print(reproject)
        process = subprocess.Popen(reproject.split(), stdout=None)
        process.wait()

        # create tiff
        print('Creating TIFF ...')
        create_tif = (
            f'gdal_translate --config GDAL_HTTP_UNSAFESSL YES '
            f'-co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 -co BIGTIFF=YES '
            f'{local_file}_{target_epsg}.vrt '
            f'{local_file}.tif'
            )
        print(create_tif)
        process = subprocess.Popen(create_tif.split(), stdout=None)
        process.wait()
    
    download_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_file_wext)))
    return download_stamp

# 2. create xarray dataset
def create_xarray(local_data, local_u_data, anc_var, target_epsg, nodata):

    def open_raster(local_data, target_epsg):

        # read a .tif file into an xarray
        data = rxr.open_rasterio(local_data, band_as_variable=True)
        epsg_code = int(data.rio.crs.to_epsg())
        print(epsg_code)
        if epsg_code != target_epsg:
            data = data.rio.reproject(dst_crs=f'EPSG:{target_epsg}')
        return data
    
    # open data and error files
    data = open_raster(local_data, target_epsg)
    errd = open_raster(local_u_data, target_epsg)
    errd_aligned = errd.rio.reproject_match(data)
    data[anc_var] = errd_aligned['band_1']

    # set nodata
    data = data.where(data != nodata, np.nan)

    return data

# 3. resample to 0.5 degrees
def coarsen(data, target_res):
    
    resampled_data = data.coarsen(x=(int(target_res / abs(data.rio.resolution()[0]))),
                                  y=(int(target_res / abs(data.rio.resolution()[1]))),
                                  boundary='trim').mean()
    return resampled_data

# 4(a). sub-function to convert units
def convert_units(data_array, target_units):
    print('Converting units ...')
    
    # create unit objects
    original_units = data_array.attrs.get('units', None) 
    original_units_obj = cfunits.Units(original_units)
    target_units_obj = cfunits.Units(target_units)
    
    # convert original to target unit
    new_array = cfunits.Units.conform(data_array, original_units_obj, target_units_obj)
    
    # create a new xr data array with converted values
    new_data_array = xr.DataArray(
        new_array,
        dims=data_array.dims,
        coords=data_array.coords,
        attrs=data_array.attrs)
    
    # update units attribute
    new_data_array.attrs['units'] = target_units
    return new_data_array

# 4. create and format netcdf
def create_netcdf(data, var, var_long_name, anc_var,
                  source_units, target_units,
                  sdate, edate, download_stamp,
                  output_path):
    print('Creating netcdf ...')

    # rename bands and mask invalid data
    ds = data.rename({'x': 'lon', 'y': 'lat', 'band_1':var})

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
    v_attrs = {'long_name': var_long_name, 'units': source_units, 'ancillary_variables': anc_var}
    e_attrs = {'long_name': f'{var} {anc_var}', 'units': 1}

    # apply formatting
    ds['time'].attrs = t_attrs
    ds['time_bounds'].attrs['long_name'] = 'time_bounds'
    ds['lat'].attrs = y_attrs
    ds['lon'].attrs = x_attrs
    ds[var].attrs = v_attrs
    ds[anc_var].attrs = e_attrs

    # to_netcdf will fail without this encoding:
    ds['time'].encoding['units'] = f'days since {sdate.strftime("%Y-%m-%d %H:%M:%S")}'
    ds['time'].encoding['calendar'] = 'noleap'
    ds['time'].encoding['bounds'] = 'time_bounds'
    ds['time_bounds'].encoding['units'] = f'days since {sdate.strftime("%Y-%m-%d %H:%M:%S")}'

    # apply unit conversion function
    ds[var] = convert_units(ds[var], target_units)

    # edit global attributes
    local_data_wext = f'{local_data}.tif'
    generate_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data_wext)))
    ds.attrs = {
        'title':'Soil organic carbon stock for 0-30 cm depth interval',
        'institution':'ISRIC Worl Soil Information',
        'source':'Predictions were derived using a digital soil mapping approach based on Quantile Random Forest, drawing on a global compilation of soil profile data and environmental layers published 2020-05-04',
        'history':f"""
    {download_stamp}: downloaded source from {remote_data}
    {generate_stamp}: resampled to 0.5 degree resolution
    {generate_stamp}: converted units from ton ha-1 to CMIP standard kg m-2
    {generate_stamp}: created CF-compliant metadata
    {generate_stamp}: details on this process can be found at {github_path}""",
        'references': """
    @article{Poggio2020,
    author  = {Poggio, Laura and de Sousa, Louis and Batjes, Niels and Heuvelink, Gerard and Kempen, Bas and Ribeiro, Eloi and Rossiter, David},
    title   = {SoilGrids 2.0: producing soil information for the globe with quantified spatial uncertainty},
    journal = {SOIL},
    year    = {2021},
    volume  = {7},
    pages   = {217--240},
    doi     = {10.5194/soil-7-217-2021}}""",
        'comment':'',
        'Conventions':'CF-1.11'}

    # tidy up
    ds['lat'] = ds['lat'].astype('float32')
    ds['lon'] = ds['lon'].astype('float32')
    ds = ds.drop_vars('spatial_ref')
    # sort latitude coords: negative to positive
    ds = ds.reindex(lat=list(reversed(ds.lat)))

    # export
    print(f'Exporting netcdf to {output_path}')
    ds.to_netcdf(output_path, format='NETCDF4', engine='netcdf4')

# use all steps above to convert the data into a netcdf
def main():

    # download data
    download_stamp = download_data(local_data, remote_data, cellsize, target_epsg)
    _ = download_data(local_u_data, remote_data, cellsize, target_epsg)

    # create xarray and re-grid
    data = create_xarray(f'{local_data}.tif', f'{local_u_data}.tif', anc_var, target_epsg, nodata)
    data = coarsen(data, res)

    # export and format netcdf
    create_netcdf(data, var, var_long_name, anc_var,
                  source_units, target_units,
                  sdate, edate, download_stamp,
                  output_path)

if __name__ == "__main__":
    main()