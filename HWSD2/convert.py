import datetime
import os
import sqlite3
import subprocess
import sys
import time
import warnings

import numpy as np
import pandas as pd
import rioxarray as rxr
import xarray as xr
from dask.distributed import Client, LocalCluster
from osgeo import gdal

# Determine the parent directory (ILAMB-DATA)
project_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

# Now you can import the helper_funcs module from the scripts package.
from scripts import biblatex_builder as bb
from scripts import helper_funcs as hf

#####################################################
# Set Parameters
#####################################################

# main parameters
VAR = "cSoil"
# VAR = "cSoilAbove1m"
LAYERS = ["D1", "D2", "D3", "D4", "D5", "D6", "D7"]  # cSoil
# LAYERS = ["D1", "D2", "D3", "D4", "D5"]  # cSoilAbove1m
POOLS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  # soil types
SDATE = datetime.datetime(1960, 1, 1)
EDATE = datetime.datetime(2022, 1, 1)

# dask parameters -- adjust these to fit your computer's capabilities
# chatgpt can optimize n_workers, n_threads, and mem_limit if you provide your computer specs!
CHUNKSIZE = 3000
N_WORKERS = 4
N_THREADS = 2
MEM_LIMIT = "16GB"

# paths to files
REMOTE_RAST = (
    "https://s3.eu-west-1.amazonaws.com/data.gaezdev.aws.fao.org/HWSD/HWSD2_RASTER.zip"
)
LOCAL_RAST = "HWSD2_RASTER/HWSD2.bil"
REMOTE_DATA = "https://www.isric.org/sites/default/files/HWSD2.sqlite"
LOCAL_DATA = "HWSD2.sqlite"
GITHUB_PATH = "https://github.com/rubisco-sfa/ILAMB-Data/blob/master/HWSD2/convert.py"

# suppress specific warnings
warnings.filterwarnings("ignore", message="invalid value encountered in cast")
gdal.DontUseExceptions()

#####################################################
# functions in the order that they are used in main()
#####################################################


# 1. download raster and sql database to connect to raster
def download_data(remote_rast, remote_data):
    # check for raster directory
    rast_dir = os.path.splitext(os.path.basename(remote_rast))[0]
    if not os.path.isdir(rast_dir) or not any(
        fname.endswith(".bil") for fname in os.listdir(rast_dir)
    ):
        subprocess.run(["mkdir", rast_dir])
        subprocess.run(["curl", "-L", remote_rast, "-o", os.path.basename(remote_rast)])
        subprocess.run(["unzip", os.path.basename(remote_rast), "-d", rast_dir])
    # check for database
    sql_database = os.path.basename(remote_data)
    if not os.path.isfile(sql_database):
        subprocess.run(["curl", "-L", remote_data, "-o", sql_database])
    else:
        print(
            f"Raster {rast_dir} and Database {sql_database} are already downloaded to current directory."
        )


# 2. initialize the dask multiprocessing client; the link can be used to track worker progress
def initialize_client(n_workers, n_threads, mem_limit):
    cluster = LocalCluster(
        n_workers=n_workers, threads_per_worker=n_threads, memory_limit=mem_limit
    )
    client = Client(cluster)
    print(f"Dask dashboard link: {client.dashboard_link}")
    return client


# 3. load the raster we use to connect with HWSDv2 data
def load_raster(path, chunksize):
    rast = rxr.open_rasterio(
        path,
        band_as_variable=True,
        mask_and_scale=True,
        chunks={"x": chunksize, "y": chunksize},
    )
    rast = (
        rast.astype("int16").drop_vars("spatial_ref").rename_vars(band_1="HWSD2_SMU_ID")
    )
    return rast


# 4. load the table with data from the sqlite database
def load_layer_table(db_path, table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {table_name}"
    layer_df = pd.read_sql_query(query, conn)
    conn.close()
    return layer_df


# 5(a). function to calculate carbon stock
def calculate_stock(
    df, top_depth, bottom_depth, bulk_density_g_cm3, cf, organic_carbon
):
    thickness_cm = df[bottom_depth] - df[top_depth]
    df["stock"] = (
        (df[bulk_density_g_cm3] * 1000)  # g to kg
        * (1 - df[cf] / 100)
        * (thickness_cm * 0.01)  # cm to meter
        * (df[organic_carbon] / 100)
    )
    return df["stock"]


# 5(b). function to calculate weighted mean
def weighted_mean(values, weights):
    return (values * weights).sum() / weights.sum()


# 5. process each soil layer by selecting the layer & pools of interest,
# removing erroneous negative values, calculating C stock, and getting
# the weighted mean of the pools
def process_layers(layer_df, layers, pools, var):
    dfs = []
    for layer in layers:
        sel = layer_df[
            [
                "HWSD2_SMU_ID",
                "LAYER",
                "SEQUENCE",
                "ORG_CARBON",
                "BULK",
                "BOTDEP",
                "TOPDEP",
                "COARSE",
                "SHARE",
            ]
        ]
        df = sel[sel["LAYER"] == layer].drop(columns=["LAYER"])
        df = df[df["SEQUENCE"].isin(pools)]
        for attr in ["ORG_CARBON", "BULK", "SHARE"]:
            df[attr] = df[attr].where(df[attr] > 0, np.nan)
        df[var] = calculate_stock(
            df, "TOPDEP", "BOTDEP", "BULK", "COARSE", "ORG_CARBON"
        )
        grouped = (
            df.groupby("HWSD2_SMU_ID")
            .apply(
                lambda x: pd.Series({var: weighted_mean(x["stock"], x["SHARE"])}),
                include_groups=False,
            )
            .reset_index()
        )
        dfs.append(grouped)
    return dfs


# 6. combine all the layers by summing, and set the data types
def combine_and_summarize(dfs, var):
    total_df = pd.concat(dfs)
    total_df = total_df.groupby("HWSD2_SMU_ID")[var].agg("sum").reset_index(drop=False)
    total_df["HWSD2_SMU_ID"] = total_df["HWSD2_SMU_ID"].astype("int16")
    total_df[var] = total_df[var].astype("float32")
    return total_df


# 7(a). function to map the soil unit ID to the cSoil variable
def map_uid_to_var(uid, uid_to_var):
    return uid_to_var.get(uid, float("nan"))


# 7. create a variable in the rast dataset containing cSoil data
def apply_mapping(rast, total_df, var):
    uid_to_var = total_df.set_index("HWSD2_SMU_ID")[var].to_dict()
    mapped_orgc = xr.apply_ufunc(
        map_uid_to_var,
        rast["HWSD2_SMU_ID"],
        input_core_dims=[[]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=["float32"],
        kwargs={"uid_to_var": uid_to_var},
    )
    rast = rast.assign({var: mapped_orgc})
    return rast


# 8. save the rast dataset as a tif
def save_raster(rast, var, layers, pools):
    output_path = f"hwsd2_{var}_{layers[0]}-{layers[-1]}_seq{pools[0]}-{pools[-1]}.tif"
    rast[[var]].rio.to_raster(output_path)
    return output_path


# 9. resample the 250m resolution to 0.5deg resolution
def resample_raster(input_path, output_path, xres, yres, interp, nan):
    gdal.SetConfigOption("GDAL_CACHEMAX", "500")
    ds = gdal.Warp(
        output_path,
        input_path,
        xRes=xres,
        yRes=yres,
        resampleAlg=interp,
        outputType=gdal.GDT_Float32,
        dstNodata=nan,
        outputBounds=(-180.0, -90.0, 180.0, 90.0),
    )
    del ds


# 10. create a netcdf of the 0.5deg resolution raster
def create_netcdf(
    input_path, var, sdate, edate, local_data, remote_data, github_path, pools, layers
):
    # open the .tif file
    ds = rxr.open_rasterio(input_path, band_as_variable=True, mask_and_scale=True)
    ds = ds.rename({"x": "lon", "y": "lat", "band_1": var})

    # create time dimension
    ds = hf.add_time_bounds_single(ds, sdate, edate)
    ds = hf.set_time_attrs(ds)
    ds = hf.set_lat_attrs(ds)
    ds = hf.set_lon_attrs(ds)

    # Get variable attribute info via ESGF CMIP variable information
    info = hf.get_cmip6_variable_info(var)

    # Set variable attributes
    ds = hf.set_var_attrs(
        ds,
        var=var,
        units=info["variable_units"],
        standard_name=info["cf_standard_name"],
        long_name=info["variable_long_name"],
    )

    # create the global attributes
    generate_stamp = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(local_data))
    )

    # create correctly formatted citation
    data_citation = bb.generate_biblatex_techreport(
        cite_key="Nachtergaele2023",
        author=[
            "Nachtergaele, Freddy",
            "van Velthuizen, Harrij",
            "Verelst, Luc",
            "Wiberg, Dave",
            "Henry, Matieu",
            "Chiozza, Federica",
            "Yigini, Yusuf",
            "Aksoy, Ece",
            "Batjes, Niels",
            "Boateng, Enoch",
            "Fischer, Günther",
            "Jones, Arwyn",
            "Montanarella, Luca",
            "Shi, Xuezheng",
            "Tramberend, Sylvia",
        ],
        title="Harmonized World Soil Database",
        institution="Food and Agriculture Organization of the United Nations and International Institute for Applied Systems Analysis, Rome and Laxenburg",
        year=2023,
        number="version 2.0",
    )

    history = f"""
{generate_stamp}: downloaded source from {remote_data}
{generate_stamp}: filtered data to soil dominance sequence(s) {pools}; where 1 is the dominant soil type
{generate_stamp}: masked invalid negative organic_carbon_pct_wt and bulk_density_g_cm3 with np.nan
{generate_stamp}: calculated cSoilLevels in kg m-2 for each level {layers}: (bulk_density_g_cm3 * 1000) * (1 - coarse_fragment_pct_vol / 100) * (thickness_cm * 0.01) * (organic_carbon_pct_wt / 100)
{generate_stamp}: calculated {var} by getting the weighted mean of all pools in a level and summing {layers} cSoilLevels where levles are 0–20cm (D1), 20–40cm (D2), 40–60cm (D3), 60–80cm (D4), 80–100cm (D5), 100–150cm (D6), 150–200 cm (D7)
{generate_stamp}: resampled to 0.5 degree resolution using mean
{generate_stamp}: created CF-compliant metadata
{generate_stamp}: exact details on this process can be found at {github_path}
"""

    ds = hf.set_cf_global_attributes(
        ds,
        title=f"Harmonized World Soil Database version 2.0 (HWSD v2.0) {var}",
        institution="International Soil Reference and Information Centre (ISRIC)",
        source="Harmonized international soil profiles from WISE30sec 2015 with 7 soil layers and expanded soil attributes",
        history=history,
        references=data_citation,
        comment="",
        conventions="CF 1.12",
    )

    # export as netcdf
    ds.to_netcdf(
        "{variable}_{frequency}_{source_id}_{st_date}-{en_date}.nc".format(
            variable=var,
            frequency="fx",
            source_id="HWSD2",
            st_date=sdate.strftime("%Y%m%d"),
            en_date=edate.strftime("%Y%m%d"),
        ),
        encoding={VAR: {"zlib": True}},
    )


# use all nine steps above to convert the data into a netcdf
def main():
    download_data(REMOTE_RAST, REMOTE_DATA)

    client = initialize_client(N_WORKERS, N_THREADS, MEM_LIMIT)

    rast = load_raster(LOCAL_RAST, CHUNKSIZE)

    layer_df = load_layer_table(LOCAL_DATA, "HWSD2_LAYERS")

    dfs = process_layers(layer_df, LAYERS, POOLS, VAR)

    total_df = combine_and_summarize(dfs, VAR)

    rast = apply_mapping(rast, total_df, VAR)

    output_path = save_raster(rast, VAR, LAYERS, POOLS)

    resample_raster(
        output_path,
        f"hwsd2_{VAR}_{LAYERS[0]}-{LAYERS[-1]}_seq{POOLS[0]}-{POOLS[-1]}_resamp.tif",
        0.5,
        0.5,
        "average",
        0,
    )

    create_netcdf(
        f"hwsd2_{VAR}_{LAYERS[0]}-{LAYERS[-1]}_seq{POOLS[0]}-{POOLS[-1]}_resamp.tif",
        VAR,
        SDATE,
        EDATE,
        LOCAL_DATA,
        REMOTE_DATA,
        GITHUB_PATH,
        POOLS,
        LAYERS,
    )

    client.close()


if __name__ == "__main__":
    main()
