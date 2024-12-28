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
long_name = 'carbon mass in soil pool'
source_units = 't ha-1'
target_units = 'kg m-2'
anc_var = 'coefficient_of_variation'
sdate = datetime.datetime(1905, 3, 31, 0, 0, 0) # Found here: https://data.isric.org/geonetwork/srv/api/records/713396f4-1687-11ea-a7c0-a0481ca9e724
edate = datetime.datetime(2016, 7, 4, 0, 0, 0)

# data source info
remote_data = '/vsicurl/https://files.isric.org/soilgrids/latest/data' #vsicurl is GDAL-specific
local_data = 'ocs_0-30cm_mean'
local_u_data = 'ocs_0-30cm_uncertainty'
github_path = 'https://github.com/rubisco-sfa/ILAMB-Data/blob/master/ISRIC/convert.py'
output_path = 'soilgrids2_cSoil.nc'
cellsize = 1000 # download res (m) in homolosine proj; I chose a higher res (but less than 0.5 in EPSG:4326) for processing speed
nodata = -32768

# regridding info
proj = 'EPSG:4326'
res = 0.5
interp = 'average'

#####################################################
# functions in the order that they are used in main()
#####################################################

gdal.UseExceptions()

# 1. download data
def download_data(local_data, remote_data, cellsize):
    
    local_data_wext = f'{local_data}.tif'
    if not os.path.isfile(local_data_wext):

        # download data using GDAL in bash
        print('Creating VRT ...')
        create_vrt = f"""
        gdal_translate --config GDAL_HTTP_UNSAFESSL YES \
            -of VRT -tr {cellsize} {cellsize} \
            {remote_data}/ocs/{local_data}.vrt \
            {local_data}.vrt
        """
        process = subprocess.Popen(create_vrt.split(), stdout=None)
        process.wait()

        # reproject from Homolosine to EPSG 4326
        print('Reprojecting VRT ...')
        reproject = f"""
        gdalwarp --config GDAL_HTTP_UNSAFESSL YES \
            -overwrite -t_srs EPSG:4326 -of VRT \
            {local_data}.vrt \
            {local_data}_4326.vrt
        """
        process = subprocess.Popen(reproject.split(), stdout=None)
        process.wait()

        # create tiff
        print('Creating TIFF ...')
        create_tif = f"""
        gdal_translate --config GDAL_HTTP_UNSAFESSL YES \
            -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 -co BIGTIFF=YES \
            {local_data}_4326.vrt \
            {local_data}.tif
        """
        process = subprocess.Popen(create_tif.split(), stdout=None)
        process.wait()
    
    download_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data_wext)))
    return download_stamp

# 3. open data
def open_data(data_path, res, interp, proj, nodata):
    print('Opening and resampling raster ...')

    # Map string interp to Resampling enum
    resampling_methods = {
        "nearest": Resampling.nearest,
        "bilinear": Resampling.bilinear,
        "cubic": Resampling.cubic,
        "cubic_spline": Resampling.cubic_spline,
        "lanczos": Resampling.lanczos,
        "average": Resampling.average,
        "mode": Resampling.mode,
        "max": Resampling.max,
        "min": Resampling.min,
        "med": Resampling.med,
        "q1": Resampling.q1,
        "q3": Resampling.q3,
        "sum": Resampling.sum,
        "rms": Resampling.rms,
    }

    # Validate interp
    if interp not in resampling_methods:
        raise ValueError(
            f"Invalid resampling method '{interp}'. Choose from: {list(resampling_methods.keys())}"
        )
    resampling = resampling_methods[interp]

    # Open the raster
    data = rxr.open_rasterio(data_path, band_as_variable=True)

    # Always reproject with resolution adjustment
    data = data.rio.reproject(
        dst_crs=proj,
        shape=(int(180/res), int(360/res)),
        resampling=resampling
    )

    # Get current bounds
    target_bounds = (-180, -90, 180, 90)
    current_bounds = data.rio.bounds()

    # Conditional clipping or padding
    if (current_bounds[0] < target_bounds[0] or  # Left bound smaller
        current_bounds[1] < target_bounds[1] or  # Bottom bound smaller
        current_bounds[2] > target_bounds[2] or  # Right bound larger
        current_bounds[3] > target_bounds[3]):   # Top bound larger
        # Clip to match the target bounds
        print("Clipping data to target bounds...")
        data = data.rio.clip_box(*target_bounds)
    elif (current_bounds[0] > target_bounds[0] or  # Left bound larger
          current_bounds[1] > target_bounds[1] or  # Bottom bound larger
          current_bounds[2] < target_bounds[2] or  # Right bound smaller
          current_bounds[3] < target_bounds[3]):   # Top bound smaller
        # Pad to match the target bounds
        print("Padding data to target bounds...")
        data = data.rio.pad_box(*target_bounds)

    # Adjust the affine transform to match the exact bounds
    transform = data.rio.transform()
    new_transform = transform * transform.translation(
        xoff=(target_bounds[0] - transform.c) / transform.a,
        yoff=(target_bounds[1] - transform.f) / -transform.e
    )
    data = data.rio.write_transform(new_transform)

    # Set nodata value to NaN
    data = data.where(data != nodata, np.nan)

    # Debugging: Print the resulting dimensions and resolution
    print(f"Output CRS: {data.rio.crs}")
    print("Output bounds:", data.rio.bounds())
    print(f"Output resolution: {data.rio.resolution()}")
    print(f"Output dimensions: {data.sizes}")

    return data

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
def create_netcdf(data, errd, output_path, var, long_name, source_units, target_units, anc_var, sdate, edate, download_stamp):
    print('Creating netcdf ...')

    # rename dims
    ds = data.rename({'x': 'lon', 'y': 'lat', 'band_1':var})
    er = errd.rename({'x': 'lon', 'y': 'lat', 'band_1':anc_var})

    # numpy array of time bounds
    tb_arr = np.asarray([
        [cf.DatetimeNoLeap(sdate.year, sdate.month, sdate.day)],
        [cf.DatetimeNoLeap(edate.year, edate.month, edate.day)]
    ]).T

    # np array to xr data array (time bounds)
    tb_da = xr.DataArray(tb_arr, dims=('time', 'nv'))

    # add time dimension and time bounds attribute
    ds = ds.expand_dims(time=tb_da.mean(dim='nv'))
    ds['time_bounds'] = tb_da
    er = er.expand_dims(time=tb_da.mean(dim='nv'))
    er['time_bounds'] = tb_da

    # add standard error variable
    ds[anc_var] = er[anc_var]

    # https://cfconventions.org/Data/cf-documents/requirements-recommendations/conformance-1.8.html
    # edit time attributes
    t_attrs = {
        'axis':'T',
        'long_name':'time'}

    # edit lat/lon attributes
    y_attrs = {
        'axis':'Y',
        'long_name':'latitude',
        'units':'degrees_north'}
    x_attrs = {
        'axis':'X',
        'long_name':'longitude',
        'units':'degrees_east'}

    # edit variable attributes
    v_attrs = {
        'long_name': long_name,
        'units': source_units,
        'ancillary_variables': anc_var}
    e_attrs = {
        'long_name': f'{var} {anc_var}',
        'units': 1}

    # set attributes
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

    # open data & resample
    download_stamp = download_data(local_data, remote_data, cellsize)
    data = open_data(f'{local_data}.tif', res, interp, proj, nodata)

    # open error data & resample
    _ = download_data(local_u_data, remote_data, cellsize)
    errd = open_data(f'{local_u_data}_resampled.tif', res, interp, proj, nodata)

    # export and format netcdf
    create_netcdf(data, errd, output_path, var, long_name, 
                  source_units, target_units, anc_var,
                  sdate, edate, download_stamp)

if __name__ == "__main__":
    main()