import os
import time
import datetime
import xarray as xr
import rioxarray as rxr
import numpy as np
import cftime as cf
from osgeo import gdal
import pandas as pd
import xarray as xr
from dask.distributed import Client, LocalCluster
import sqlite3
import matplotlib.pyplot as plt
import warnings

# main parameters
chunksize = 3000
var = 'cSoil'
long_name = 'carbon mass in soil pool'
layers = ['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7'] # 0-bottom
pools = [1,2,3,4,5,6,7,8,9,10,11,12]
sdate = datetime.datetime(1960, 1, 1)
edate = datetime.datetime(2022, 1, 1)

# data sources (paths)
remote_rast = 'https://s3.eu-west-1.amazonaws.com/data.gaezdev.aws.fao.org/HWSD/HWSD2_RASTER.zip'
local_rast = 'HWSD2_RASTER/HWSD2.bil'
remote_data = 'https://www.isric.org/sites/default/files/HWSD2.sqlite'
local_data = 'HWSD2.sqlite'
github_path = 'https://github.com/rubisco-sfa/ILAMB-Data/blob/master/HWSD2/convert.py'

# function to calculate carbon stock (kg m-2)
def calculate_stock(df, depth, bulk_density_g_cm3, cf, organic_carbon):

    df['stock'] = (
        df[bulk_density_g_cm3] * # Mg/m3 == g/cm3
        (1 - df[cf] / 100) * # percent to decimal
        df[depth] * 0.01 * # cm to m
        df[organic_carbon] # percent
    )

    return df['stock']

# function to calculate weighted mean given a value and weight
def weighted_mean(values, weights):
    
    return (values * weights).sum() / weights.sum()

# function to map 
def map_uid_to_orgc(uid):
    return uid_to_var.get(uid, float('nan'))

