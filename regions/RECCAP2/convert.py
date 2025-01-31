import xarray as xr

# Load data and extract region labels and names
ds = xr.open_dataset("RECCAP2_region_masks_all_v20221025.nc")
labels = ["atlantic", "pacific", "indian", "arctic", "southern"]
names = [f"{lbl.capitalize()} Ocean" for lbl in labels]

# Check that the ocean masks are mutually exclusive
for a, lbla in enumerate(labels):
    for b, lblb in enumerate(labels):
        if a > b:
            assert ~((ds[lbla] > 0) & (ds[lblb] > 0)).sum()

# The ILAMB region system is 0-based, so here we flag as -1 all 0's and then non-zeros
# are assigned the basin ID. Then we can concat them into 1 taking the max across
# basins.
da = xr.concat(
    [
        xr.where(ds[ocean] == 0, -1, basin_id)
        for basin_id, ocean in enumerate(
            ["atlantic", "pacific", "indian", "arctic", "southern"]
        )
    ],
    dim="ocean",
).max(dim="ocean")

# Now structure the dataset to what ILAMB is expecting
da.attrs = {"_FillValue": -1, "labels": "labels", "names": "names"}
ds = da.to_dataset(name="ids")
ds["labels"] = labels
ds["names"] = names
ds.to_netcdf("RECCAP2.nc")
