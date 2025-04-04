import glob
import os
import sys
import zipfile
from pathlib import Path

import numpy as np
import requests
import xarray as xr

# Determine the parent directory (ILAMB-DATA)
project_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

# Now you can import the helper_funcs module from the scripts package.
from scripts import biblatex_builder as bb
from scripts import helper_funcs as hf

######################################################################
# Set Parameters
######################################################################

VAR = "burntFractionAll"

######################################################################
# Download Data
######################################################################

# Specify the dataset title you are looking for
dataset_title = "Global Fire Emissions Database (GFED5) Burned Area"

# Build the query string to search by title
params = {"q": f'title:"{dataset_title}"'}

# Define the Zenodo API endpoint
base_url = "https://zenodo.org/api/records"

# Send the GET request
response = requests.get(base_url, params=params)
if response.status_code != 200:
    print("Error during search:", response.status_code)
    exit(1)

# Parse the JSON response
data = response.json()

# Select the first record; there should only be one
records = data["hits"]["hits"]
i = 0
for record in records:
    i += 1
    title = record["metadata"].get("title")
    print(f"\n{i}. Dataset title: {title}")
record = data["hits"]["hits"][0]

# Download and get timestamps
hf.download_from_zenodo(record)
path_to_zip_file = Path("_temp/BA.zip")
download_stamp = hf.gen_utc_timestamp(path_to_zip_file.stat().st_mtime)
generate_stamp = hf.gen_utc_timestamp()

# Unzip downloaded data file (BA.zip)
full_path = os.path.abspath(path_to_zip_file)
full_path_without_zip, _ = os.path.splitext(full_path)
with zipfile.ZipFile(path_to_zip_file, "r") as zip_ref:
    zip_ref.extractall(full_path_without_zip)

######################################################################
# Open netcdfs
######################################################################

# Get a list of all netCDF files in the unzipped folder
data_dir = "_temp/BA"
all_files = glob.glob(os.path.join(data_dir, "*.nc"))

# Get separate lists for coarse data and fine data
coarse_files = []  # For 1997-2000 (1 degree)
fine_files = []  # For 2001-2020 (0.25 degree)
for f in all_files:
    basename = os.path.basename(f)  # e.g., "BA200012.nc"
    # Extract the year from the filename; here characters at positions 2:6.
    year = int(basename[2:6])
    if year < 2001:
        coarse_files.append(f)
    else:
        fine_files.append(f)

# Load the coarse and fine datasets separately
ds_coarse = xr.open_mfdataset(coarse_files, combine="by_coords")
ds_fine = xr.open_mfdataset(fine_files, combine="by_coords")

# Load burnable area (and mask) datasets
da_coarse_mask = xr.open_dataset("_temp/BurnableArea_preMOD.nc")["BurableArea"]
da_fine_mask = xr.open_dataset("_temp/BurnableArea.nc")["BurableArea"]

######################################################################
# Process netcdfs
######################################################################

# Calculate burned fraction of burnable area as a percent
percent_burned_coarse = (ds_coarse["Total"] / da_coarse_mask) * 100
ds_coarse = ds_coarse.assign({VAR: percent_burned_coarse})
percent_burned_fine = (ds_fine["Total"] / da_fine_mask) * 100
ds_fine = ds_fine.assign({VAR: percent_burned_fine})

# Mask the datasets
percent_burned_coarse_masked = ds_coarse.where(da_coarse_mask > 0)
percent_burned_fine_masked = ds_fine.where(da_fine_mask > 0)

# Interpolate coarse 1 degree data to 0.25 degrees
res = 0.25
newlon = np.arange(-179.875, 180, res)
newlat = np.arange(-89.875, 90, res)

# Interpolate 1 degree data to 0.25 degrees
percent_burned_coarse_masked_interp = percent_burned_coarse_masked.interp(
    lat=newlat, lon=newlon
)

# Combine coarse-interpolated and fine data into one dataset at 0.25 degree resolution
ds = xr.concat(
    [percent_burned_fine_masked, percent_burned_coarse_masked_interp], dim="time"
)
ds = ds.sortby("time")
ds = ds[VAR].to_dataset()

######################################################################
# Set CF compliant netcdf attributes
######################################################################

# Set dimension attributes and encoding
ds = hf.set_time_attrs(ds)
ds = hf.set_lat_attrs(ds)
ds = hf.set_lon_attrs(ds)

# Get variable attribute info via ESGF CMIP variable information
info = hf.get_cmip6_variable_info(VAR)

# Set variable attributes
ds = hf.set_var_attrs(
    ds,
    var=VAR,
    units=info["variable_units"],
    standard_name=info["cf_standard_name"],
    long_name=info["variable_long_name"],
)

# Add time bounds
ds = hf.add_time_bounds_monthly(ds)
time_range = f"{ds['time'].min().dt.year:d}{ds['time'].min().dt.month:02d}"
time_range += f"-{ds['time'].max().dt.year:d}{ds['time'].max().dt.month:02d}"

# Define global attribute citation information
data_citation = bb.generate_biblatex_dataset(
    cite_key="Chen2023",
    author=[
        "Chen, Yang",
        "Hall, Joanne",
        "van Wees, Dave",
        "Andela, Niels",
        "Hantson, Stijn",
        "Giglio, Louis",
        "van der Werf, Guido R.",
        "Morton, Douglas C.",
        "Randerson, James T.",
    ],
    title="Global Fire Emissions Database (GFED5) Burned Area (0.1)",
    year=2023,
    url="https://zenodo.org/records/7668424",
    doi="10.5281/zenodo.7668424",
)
article_citation = bb.generate_biblatex_article(
    cite_key="Chen2023",
    author=[
        "Chen, Yang",
        "Hall, Joanne",
        "van Wees, Dave",
        "Andela, Niels",
        "Hantson, Stijn",
        "Giglio, Louis",
        "van der Werf, Guido R.",
        "Morton, Douglas C.",
        "Randerson, James T.",
    ],
    title="Multi-decadal trends and variability in burned area from the fifth version of the Global Fire Emissions Database (GFED5)",
    journal="Earth Syst. Sci. Data",
    year=2023,
    volume=15,
    number=11,
    pages=[5227, 5259],
    doi="https://doi.org/10.5194/essd-15-5227-2023",
)

citations = data_citation + "\n\n" + article_citation

# Set global netcdf attributes
ds = hf.set_cf_global_attributes(
    ds,
    title="Global Fire Emissions Database (GFED5) Burned Area",
    institution="Global Fire Emissions Database",
    source="Combine MODIS, Landsat, and Sentinel-2 to create a 24-year record of global burned area",
    history=f"""downloaded: {download_stamp}\nformatted: {generate_stamp}""",
    references=citations,
    comment="",
    conventions="CF 1.12",
)

######################################################################
# Export
######################################################################

ds.to_netcdf(
    "{variable}_{frequency}_{source_id}_{time_mark}.nc".format(
        variable=VAR, frequency="mon", source_id="GFED5", time_mark=time_range
    ),
    encoding={VAR: {"zlib": True}},
)
