from pathlib import Path

import cftime
import numpy as np
import xarray as xr

# read variables and rename
ds = xr.open_dataset("CERES_EBAF_Ed4.2_Subset_200003-202306.nc")
ds = ds.rename(
    {
        "sfc_sw_down_all_mon": "rsds",
        "sfc_sw_up_all_mon": "rsus",
        "sfc_lw_down_all_mon": "rlds",
        "sfc_lw_up_all_mon": "rlus",
        "sfc_net_sw_all_mon": "rsns",
        "sfc_net_lw_all_mon": "rlns",
        "sfc_net_tot_all_mon": "rns",
    }
)
ds["albedo"] = ds["rsus"] / ds["rsds"]
ds["albedo"].attrs = {"long_name": "Albedo", "units": "1"}

# modify calendar and add bounds
ds["time"] = [cftime.DatetimeNoLeap(t.dt.year, t.dt.month, t.dt.day) for t in ds.time]
tb = np.array(
    [
        [cftime.DatetimeNoLeap(t.dt.year, t.dt.month, 1) for t in ds["time"]],
        [
            cftime.DatetimeNoLeap(
                t.dt.year if t.dt.month < 12 else t.dt.year + 1,
                (t.dt.month + 1) if t.dt.month < 12 else 1,
                1,
            )
            for t in ds["time"]
        ],
    ]
).T
ds["time_bnds"] = xr.DataArray(tb, dims=("time", "nb"))

# add to attributes
attrs = ds.attrs
attrs[
    "references"
] = """
@ARTICLE{
    author = {Loeb, N. G., D. R. Doelling, H. Wang, W. Su, C. Nguyen, J. G. Corbett, L. Liang, C. Mitrescu, F. G. Rose, and S. Kato},
    title = {Clouds and the Earth's Radiant Energy System (CERES) Energy Balanced and Filled (EBAF) Top-of-Atmosphere (TOA) Edition-4.0 Data Product},
    journal = {J. Climate},
    year = {2018},
    doi = {doi:10.1175/JCLI-D-17-0208.1}
}
@ARTICLE{
    author = {Kato, S., F. G. Rose, D. A. Rutan, T. E. Thorsen, N. G. Loeb, D. R. Doelling, X. Huang, W. L. Smith, W. Su, and S.-H. Ham},
    title = {Surface irradiances of Edition 4.0 Clouds and the Earth's Radiant Energy System (CERES) Energy Balanced and Filled (EBAF) data product},
    journal = {J. Climate},
    year = {2018},
    doi = {doi:10.1175/JCLI-D-17-0523.1}
}"""

# output
for name, da in ds.items():
    path = Path(f"DATA/{name}/CERESed4.2/")
    path.mkdir(parents=True, exist_ok=True)
    for a in ["valid_min", "valid_max"]:
        if a not in da.attrs:
            continue
        da.attrs[a] = float(da.attrs[a].strip())
    da = da.to_dataset()
    da["time_bnds"] = ds["time_bnds"]
    da.attrs = attrs
    da.to_netcdf(
        path / f"{name}.nc",
        encoding={
            name: {"zlib": True},
            "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
            "time_bnds": {"units": "days since 1850-01-01"},
        },
    )
