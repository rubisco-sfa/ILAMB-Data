import os
import time
import datetime
import xarray as xr
import rioxarray as rxr
import numpy as np
import cftime as cf
import cfunits
import earthaccess
from earthaccess import DataCollections

#####################################################
# set the parameters for this particular dataset
#####################################################

# variables
cwd = os.getcwd() # cws = current working directory; working environment
sdate = datetime.datetime(1961, 1, 1, 0, 0, 0)
edate = datetime.datetime(2011, 12, 31, 0, 0, 0)
proj = 'EPSG:4326'
short_name = 'Rs'
long_name = 'mean annual autotrophic and heterotrophic soil respiration'
uncertainty = 'standard_deviation'
units = 'g m-2 yr-1'
target_units = 'kg m-2 s-1'
target_res = 0.5

# data sources
local_data = 'soil_resp_mean_quantile_regress_forest.tif'
local_u_data = 'soil_resp_std_dev_quantile_regress_forest.tif'
remote_data = 'https://daac.ornl.gov/CMS/guides/CMS_Global_Soil_Respiration.html'
github_path = 'https://github.com/rubisco-sfa/ILAMB-Data/blob/master/SRDB/convert.py'

#####################################################
# functions in the order that they are used in main()
#####################################################

# 1. if not present in current dir, download the necessary data
def download_rs_data(local_data, cwd):
    
    print("""
    An Earthdata username and password is required to download this data. 
    Create a free account here: https://urs.earthdata.nasa.gov/users/new')\n"""
         )
    auth = earthaccess.login() # user must input username/password

    # search for collections of this name; returns dict of collections
    dname = 'Global Gridded 1-km Annual Soil Respiration and Uncertainty Derived from SRDB V3'
    query = DataCollections().keyword(dname)
    
    # print a dictionary of collections with their names/abstracts
    collections = query.fields(['ShortName','Abstract']).get()
    collection = collections[0] # choose the collection
    cid = collection.concept_id() # get collection cid
    
    # using the cid, get a list of granules (.tif files)
    query = earthaccess.granule_query().concept_id(cid)
    granules = query.get()
    
    # download the data (or don't if already present)
    file1 = earthaccess.download(granules[3], cwd) # Rs .tif/.sha256
    file2 = earthaccess.download(granules[4], cwd) # uncertainty .tif/.sha256
    download_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data)))

    return download_stamp

# 2. create xarray dataset
def create_xarray(local_data, local_u_data, uncertainty, cwd):

    def open_raster(local_data, cwd):

        # read a .tif file into an xarray
        data = rxr.open_rasterio(local_data, band_as_variable=True)
        epsg_code = int(data.rio.crs.to_epsg())
        if epsg_code != 4326:
            data = data.rio.reproject(crs=proj)
        return data
    
    # open data and error files
    data = open_raster(local_data, cwd)
    errd = open_raster(local_u_data, cwd)
    data = data.assign(uncertainty=errd['band_1'])
    return data

# 3. resample to 0.5 degrees
def coarsen(data, target_res):
    
    resampled_data = data.coarsen(x=(int(target_res / abs(data.rio.resolution()[0]))),
                                  y=(int(target_res / abs(data.rio.resolution()[1])))).mean()
    return resampled_data

# 4(a). subfunction to convert original units to target units
def convert_units(data_array, target_units):
    
    # Get the original units from the data array attributes
    original_units = data_array.attrs.get('units', None)
    original_units_obj = cfunits.Units(original_units)
    target_units_obj = cfunits.Units(target_units)
        
    # Convert original to target unit using cfunits
    new_array = cfunits.Units.conform(data_array, original_units_obj, target_units_obj)
    
    # Create a new xr data array with converted values
    new_data_array = xr.DataArray(
        new_array,
        dims=data_array.dims,
        coords=data_array.coords,
        attrs=data_array.attrs
    )
    
    # Update the units attribute to the target units
    new_data_array.attrs['units'] = target_units
    return new_data_array

# 4. create a properly formatted netcdf
def create_netcdf(data, 
                  short_name, long_name, uncertainty, 
                  sdate, edate, 
                  units, target_units, 
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

    # apply unit conversion function
    ds[short_name] = convert_units(ds[short_name], target_units)

    # edit global attributes
    generate_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data)))
    ds.attrs = {
        'title':'Global gridded 1-km annual soil respiration and uncertainty derived from SRDB V3',
        'institution':'NASA Carbon Monitoring System',
        'source':'Fit a QRF algorithm to over 2,500-point observations and corresponding 1-km spatially distributed climate (annual temperature, annual and seasonal precipitation) and vegetation covariates',
        'history':f"""
{download_stamp}: downloaded source from {remote_data}
{generate_stamp}: resampled to 0.5 degree resolution
{generate_stamp}: ensured units were CMIP standard kg m-2 s-1
{generate_stamp}: created CF-compliant metadata
{generate_stamp}: details on this process can be found at {github_path}""",
        'references': """
@article{Warner2019,
author  = {Warner, D.L. and Bond-Lamberty, B. and Jian, J. and Stell, E. and Vargas, R.},
title   = {Spatial Predictions and Associated Uncertainty of Annual Soil Respiration at the Global Scale},
journal = {Global Biogeochem. Cy.},
year    = {2019},
volume  = {33},
pages   = {1733--1745},
doi     = {10.1029/2019GB006264}}""",
        'comment':'',
        'Conventions':'CF-1.11'}

    # tidy up
    ds['lat'] = ds['lat'].astype('float32')
    ds['lon'] = ds['lon'].astype('float32')
    ds = ds.drop_vars('spatial_ref')
    # sort latitude coords: negative to positive
    ds = ds.reindex(lat=list(reversed(ds.lat)))

    # export
    ds.to_netcdf('warner2019_Rs.nc', format='NETCDF4', engine='netcdf4')

# use the above functions to convert the data into a 0.5deg netcdf
def main():

    if not os.path.isfile(local_data):
        print('Data is not present in current working directory. Downloading...')
        download_stamp = download_rs_data(local_data, cwd)
    else:
        print('Data is present in current working directory.')
        download_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data)))
    
    data = create_xarray(local_data, local_u_data, uncertainty, cwd)
    data = coarsen(data, target_res)

    create_netcdf(data, 
                  short_name, long_name, uncertainty, 
                  sdate, edate, 
                  units, target_units, 
                  local_data, remote_data, 
                  github_path, download_stamp)


if __name__ == "__main__":
    main()