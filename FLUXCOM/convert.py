import glob
import os
import time
from pathlib import Path

import cftime as cf
import numpy as np
import xarray as xr
from ilamb3.dataset import compute_cell_measures


def fix_time_monthly(ds: xr.Dataset) -> xr.Dataset:
    # Re-encode time to use the ILAMB standard noleap calendar and include bounds
    if "time_bounds" in ds:
        ds = ds.drop("time_bounds")
    ds["time"] = [cf.DatetimeNoLeap(t.dt.year, t.dt.month, 15) for t in ds["time"]]
    ds["time_bnds"] = (
        ("time", "nbnds"),
        np.asarray(
            [
                [cf.DatetimeNoLeap(t.dt.year, t.dt.month, 1) for t in ds["time"]],
                [
                    cf.DatetimeNoLeap(
                        t.dt.year + 1 * (t.dt.month == 12),
                        t.dt.month + 1 if (t.dt.month < 12) else 1,
                        1,
                    )
                    for t in ds["time"]
                ],
            ]
        ).T,
    )
    return ds


# FluxCom provides a land fraction mask which we will incorporate as cell measures for
# ilamb to use in spatial integration.
cm = xr.open_dataset("raw/landfraction.720.360.nc") * 0.01
cm *= compute_cell_measures(cm)
cm = cm.rename(dict(landfraction="cell_measures", latitude="lat", longitude="lon"))
cm = cm.pint.dequantify()
cm = cm["cell_measures"]
cm.attrs = {"long_name": "land_area", "units": "m2"}

# Loop through the variables and create the datasets.
fluxcom_to_cmip = dict(GPP="gpp", H="hfss", LE="hfls", TER="reco")
generate_stamp = time.strftime("%Y-%m-%d")
for fluxcom, cmip in fluxcom_to_cmip.items():
    # What files are we using?
    files = glob.glob(f"raw/{fluxcom}.*.nc")
    if not files:
        continue
    download_stamp = time.strftime(
        "%Y-%m-%d", time.localtime(os.path.getctime(files[0]))
    )

    # Merge into a single dataset
    ds = xr.concat([xr.load_dataset(f) for f in files], data_vars="minimal", dim="time")
    ds = ds.rename_vars({fluxcom: cmip})

    # Fix some locations that are all zero for all time
    ds[cmip] = xr.where(
        ~((np.abs(ds[cmip]) < 1e-15).all(dim="time")), ds[cmip], np.nan, keep_attrs=True
    )

    # Fix units, I get it but unit conversion systems don't understand this
    if "gC" in ds[cmip].attrs["units"]:
        ds[cmip].attrs["units"] = ds[cmip].attrs["units"].replace("gC", "g")
    ds.pint.quantify()

    # Add measures and bounds
    ds["cell_measures"] = cm
    ds = fix_time_monthly(ds)
    if "lat_bnds" not in ds:
        ds = ds.cf.add_bounds(["lat", "lon"])
    if "lat_bounds" in ds:
        ds = ds.rename(dict(lat_bounds="lat_bnds", lon_bounds="lon_bnds"))
    ds["lat"].attrs["bounds"] = "lat_bnds"
    ds["lon"].attrs["bounds"] = "lon_bnds"
    ds[cmip] = ds[cmip].transpose("time", "lat", "lon")

    # Add attributes
    ds.attrs = dict(
        title="FLUXCOM (RS+METEO) Global Land Carbon Fluxes using CRUNCEP climate data",
        version="1",
        institutions="Department Biogeochemical Integration, Max Planck Institute for Biogeochemistry, Germany",
        source="""Data generated by Artificial Neural Networks and forced with CRUNCEPv6 meteorological data and MODIS (RS+METEO)
ftp://ftp.bgc-jena.mpg.de/pub/outgoing/FluxCom/CarbonFluxes_v1_2017/RS+METEO/CRUNCEPv6/raw/monthly/
ftp://ftp.bgc-jena.mpg.de/pub/outgoing/FluxCom/EnergyFluxes/RS_METEO/member/CRUNCEP_v8/monthly/""",
        history=f"""
{download_stamp}: downloaded {[Path(f).name for f in files]};
{generate_stamp}: converted to netCDF, additionally we apply a mask where |var|<1e-15 for all time.""",
        references="""
@ARTICLE{Jung2019,
  author = {Jung, M., S. Koirala, U. Weber, K. Ichii, F. Gans, Gustau-Camps-Valls, D. Papale, C. Schwalm, G. Tramontana, and M. Reichstein},
  title = {The FLUXCOM ensemble of global land-atmosphere energy fluxes},
  journal = {Scientific Data},
  year = {2019},
  volume = {6},
  issue = {1},
  page = {74},
  doi = {https://doi.org/10.1038/s41597-019-0076-8}
}
@ARTICLE{Tramontana2016,
  author = {Tramontana, G., M. Jung, C.R. Schwalm, K. Ichii, G. Camps-Valls, B. Raduly, M. Reichstein, M.A. Arain, A. Cescatti, G. Kiely, L. Merbold, P. Serrano-Ortiz, S. Sickert, S. Wolf, and D. Papale},
  title = {Predicting carbon dioxide and energy fluxes across global FLUXNET sites with regression algorithms},
  journal = {Biogeosciences},
  year = {2016},
  number = {13},
  page = {4291-4313},
  doi = {https://doi.org/10.5194/bg-13-4291-2016}
}""",
    )
    ds.to_netcdf(
        f"{cmip}.nc",
        encoding={
            "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
            "time_bnds": {"units": "days since 1850-01-01"},
            cmip: {"zlib": True},
        },
    )
