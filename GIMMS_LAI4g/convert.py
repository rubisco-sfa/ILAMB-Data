import pandas as pd
import numpy as np
import xarray as xr
import rioxarray as rxr
import cftime as cf
from urllib.request import urlretrieve
import datetime
import os
import time
import zipfile
import glob
from collections import defaultdict
import warnings

#####################################################
# set the parameters for this particular dataset
#####################################################

sdate = datetime.datetime(1982, 1, 1, 0, 0, 0)
edate = datetime.datetime(2020, 12, 31, 0, 0, 0)
github_path = 'https://github.com/rubisco-sfa/ILAMB-Data/blob/master/GIMMS_LAIg4/convert.py'
var = 'lai'
long_name = 'leaf_area_index'
output_path = 'cao2023.nc'

# filter specific warning
warnings.filterwarnings("ignore", category=Warning, message=".*TIFFReadDirectory:Sum of Photometric type-related color channels and ExtraSamples doesn't match SamplesPerPixel.*")

#####################################################
# functions in the order that they are used in main()
#####################################################

# 1. download the data and unzip
def download_zip(local_data, remote_data):
    # download data and get timestamp
    if not os.path.isfile(local_data):
        print(f'Downloading {local_data} ...')
        urlretrieve(remote_data, local_data)
    download_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_data)))

    # check if zip file
    if local_data.endswith('.zip'):
        zipdir = os.path.splitext(os.path.basename(local_data))[0]
        with zipfile.ZipFile(local_data, 'r') as zip_ref:
            print(f'Unzipping {local_data} ...')
            zip_ref.extractall()  # Extracts to the current directory
        return zipdir, download_stamp
        
    else:
        print('File does not end with .zip')
        return None, download_stamp

# 2. group half-month tifs by year and month
def group_tifs_by_month(zipdir):

    grouped_tifs = defaultdict(lambda: defaultdict(list))
    
    # group tifs
    for filename in sorted(glob.glob(f'{zipdir}/*.tif')):
        date = (filename.split('_')[-1]).split('.')[0]
        year, month = date[:4], date[4:6]

        grouped_tifs[year][month].append(filename)
            
    grouped_tifs = {year: dict(months) for year, months in grouped_tifs.items()}
    
    return grouped_tifs

# 3. open the rasters, reproject to WGS84, and resample to 0.5 deg
def open_grouped_tifs(raster_dict):

    # sub-function to resample to 0.5 degrees
    def coarsen(target_res, data):
        resampled_data = data.coarsen(x=(int(target_res / abs(data.rio.resolution()[0]))),
                                      y=(int(target_res / abs(data.rio.resolution()[1])))).mean()
        return resampled_data

    # begin open_grouped_tifs
    opened_dict = {}
    
    for year, months in raster_dict.items():
        opened_dict[year] = {}
        
        for month, submonth_paths in months.items():
            opened_dict[year][month] = []

            # open tifs
            for submonth_path in submonth_paths:
                submonth_data = rxr.open_rasterio(submonth_path, band_as_variable=True)
                submonth_data = submonth_data.where(submonth_data != 65535, np.nan)
                if 'band_2' in submonth_data.data_vars:
                    submonth_data = submonth_data.drop_vars('band_2')
                
                # check EPSG code and reproject if necessary
                if submonth_data.rio.crs is not None and submonth_data.rio.crs.to_epsg() != 4326:
                    submonth_data = submonth_data.rio.reproject(crs='EPSG:4326')

                # coarsen and set bounds
                submonth_data = coarsen(0.5, submonth_data)
                opened_dict[year][month].append(submonth_data)
    
    return opened_dict

# 4. get the mean of half-months in each month
def calculate_monthly_mean_with_time(raster_dict):
    monthly_mean_dict = {}
    
    for year, months in raster_dict.items():
        monthly_mean_dict[year] = {}
        
        for month, datasets in months.items():
            if len(datasets) > 0:
                # concatenate the datasets along a new dimension and calculate the mean
                monthly_mean_data = xr.Dataset()
                monthly_mean_data['band_1'] = xr.concat([ds['band_1'] for ds in datasets], dim="datasets").mean(dim="datasets")
                
                # create a time dimension from the year and month
                time_str = f"{year}-{month}-01"
                time = pd.to_datetime([time_str])
                monthly_mean_data = monthly_mean_data.expand_dims(time=time)
                
                # store the monthly mean dataset in the dictionary
                monthly_mean_dict[year][month] = monthly_mean_data
            else:
                # handle the case where no datasets are available
                monthly_mean_dict[year][month] = None
    
    return monthly_mean_dict

# 5. concatenate month mean tifs along time dimension
def concatenate_along_time(raster_dict):
    
    concatenated_data = []

    for year, months in raster_dict.items():
        for month, dataset in months.items():
            if dataset is not None:
                if concatenated_data:
                    # check if the dataset aligns with the first one in x and y dimensions
                    if not (
                        dataset['x'].equals(concatenated_data[0]['x']) and 
                        dataset['y'].equals(concatenated_data[0]['y'])
                    ):
                        print(f"Rasters for {year}-{month} do not align in x and y dimensions.")
                        return None
                concatenated_data.append(dataset)
    
    if concatenated_data:
        # concatenate all datasets along the time dimension
        result = xr.concat(concatenated_data, dim="time")
        return result
    else:
        return None

# 6. create the netcdf
def create_netcdf(ds, var, sdate, dstamps, local_path, remote_path, github_path, output_path):

    # rename dims
    ds = ds.rename({'x': 'lon', 'y': 'lat', 'band_1': var})
    
    # numpy array of time bounds
    tb_arr = np.asarray([[cf.DatetimeNoLeap(t.dt.year, t.dt.month, t.dt.day) for t in ds['time']], 
                         [cf.DatetimeNoLeap(t.dt.year, t.dt.month, t.dt.day) for t in ds['time']]]).T
    tb_da = xr.DataArray(tb_arr, dims=('time', 'nv'))
    ds['time_bounds'] = tb_da

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
        'units': 1}
    
    # set attributes
    ds['time'].attrs = t_attrs
    ds['time_bounds'].attrs['long_name'] = 'time_bounds'
    ds['lat'].attrs = y_attrs
    ds['lon'].attrs = x_attrs
    ds[var].attrs = v_attrs
    
    # to_netcdf will fail without this encoding:
    ds['time'].encoding['units'] = f'days since {sdate.strftime("%Y-%m-%d %H:%M:%S")}'
    ds['time'].encoding['calendar'] = 'noleap'
    ds['time'].encoding['bounds'] = 'time_bounds'
    ds['time_bounds'].encoding['units'] = f'days since {sdate.strftime("%Y-%m-%d %H:%M:%S")}'

    # edit global attributes
    generate_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(local_path)))
    ds.attrs = {
        'title':'Spatiotemporally consistent global dataset of the GIMMS Leaf Area Index (GIMMS LAI4g) from 1982 to 2020',
        'institution':'',
        'source':'The GIMMS LAI4g was generated based on biome-specific BPNN models that employed the latest PKU GIMMS NDVI product and 3.6 million high-quality global Landsat LAI samples. It was then consolidated with the Reprocess MODIS LAI to extend the temporal coverage to 2020 via a pixel-wise Random Forests fusion method.',
        'history':f"""
    {dstamps[0]}: downloaded source from {remote_path}
    {generate_stamp}: calcualted monthly means
    {generate_stamp}: resampled to 0.5 degree resolution
    {generate_stamp}: concatenated all years and months into one netcdf along the time dimension
    {generate_stamp}: created CF-compliant metadata
    {generate_stamp}: details on this process can be found at {github_path}""",
        'references': """
    @article{Cao2023,
    author  = {Cao, S. and Li, M. and Zhu, Z. and Wang, Z. and Zha, J. and Zhao, W. and Duanmu, Z. and Chen, J. and Zheng, Y. and Chen, Y. and Myneni, R.B. and Piao, S.},
    title   = {Spatiotemporally consistent global dataset of the GIMMS Leaf Area Index (GIMMS LAI4g) from 1982 to 2020},
    journal = {Earth Syst. Sci.},
    year    = {2023},
    volume  = {},
    pages   = {},
    doi     = {10.5194/essd-2023-68}}""",
        'comment':'',
        'Conventions':'CF-1.11'}

    # tidy up
    ds['lat'] = ds['lat'].astype('float32')
    ds['lon'] = ds['lon'].astype('float32')
    ds = ds.drop_vars('spatial_ref')
    # sort latitude coords: negative to positive
    ds = ds.reindex(lat=list(reversed(ds.lat)))

    ds.to_netcdf(output_path, format='NETCDF4', engine='netcdf4')

#####################################################
# apply all function to get netcdf
#####################################################

# use all six steps above to convert the data into a netcdf
def main():

    # loop through zips and tifs
    multiyears = []
    dstamps = []
    years = ['1982_1990', '1991_2000', '2001_2010', '2011_2020']
    for year in years:
        
        remote_path = f'https://zenodo.org/records/8281930/files/GIMMS_LAI4g_AVHRR_MODIS_consolidated_{year}.zip?download=1'
        local_path = f'GIMMS_LAI4g_AVHRR_MODIS_consolidated_{year}.zip'
        
        zipdir, dstamp = download_zip(local_path, remote_path)
        
        grouped_tifs = group_tifs_by_month(zipdir)
        
        opened_tifs = open_grouped_tifs(grouped_tifs)
        
        monthly_means = calculate_monthly_mean_with_time(opened_tifs)
        
        monthly_means_over_time = concatenate_along_time(monthly_means)
        
        multiyears.append(monthly_means_over_time)
        dstamps.append(dstamp)

        del zipdir, dstamp, grouped_tifs, opened_tifs, monthly_means, monthly_means_over_time

    ds = xr.concat(multiyears, dim="time")
    create_netcdf(ds, var, sdate, dstamps, local_path, remote_path, github_path, output_path)

if __name__ == "__main__":
    main()