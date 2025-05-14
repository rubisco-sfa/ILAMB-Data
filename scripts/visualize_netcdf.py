from pathlib import Path

import matplotlib.pyplot as plt
import xarray as xr

# set parameters
wdir = "../HWSD2"
file_path = f"{wdir}/cSoil_fx_HWSD2_19600101-20220101.nc"
tstep = 0
vmin = 0  # Minimum value for colormap
vmax = 100  # Maximum value for colormap
var = "cSoil"

# open netcdf and select time step
base_name = Path(file_path).stem
data = xr.open_dataset(file_path)
da = data[var].isel(time=tstep)

# create and save full map
plt.figure(figsize=(10, 6))
p = da.plot(vmin=vmin, vmax=vmax)
plt.savefig(f"{wdir}/{base_name}_timestep_{tstep}.png", dpi=300, bbox_inches="tight")
plt.close()

#### Create zoomed-in map ####
# Define Southeastern US bounding box
lon_min, lon_max = -95, -75
lat_min, lat_max = 25, 37

# Determine proper slicing directions
lat_vals = da["lat"].values
lon_vals = da["lon"].values

lat_slice = (
    slice(lat_min, lat_max) if lat_vals[0] < lat_vals[-1] else slice(lat_max, lat_min)
)
lon_slice = (
    slice(lon_min, lon_max) if lon_vals[0] < lon_vals[-1] else slice(lon_max, lon_min)
)

# Clip the data dynamically
da_se = da.sel(lon=lon_slice, lat=lat_slice)

# Plot the clipped region
plt.figure(figsize=(8, 6))
p = da_se.plot(vmin=vmin, vmax=vmax)
plt.savefig(f"{wdir}/{base_name}_SE-US_zoom.png", dpi=300, bbox_inches="tight")
plt.close()
