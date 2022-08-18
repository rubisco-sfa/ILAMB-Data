"""Download and bring the NCSCD data up to CF-standards. Based on Figure 2 in
the paper (10.5194/essd-5-393-2013) and plots of the data:

* 'cSoilAbove1m' is from 'NCSCDv2_Circumpolar_WGS84_SOCC100_05deg.nc'
* 'cSoil' is the sum of [NCSCDv2_Circumpolar_WGS84_SOCC100_05deg.nc,
                         NCSCDv2_Circumpolar_WGS84_SOCC200_05deg.nc,
                         NCSCDv2_Circumpolar_WGS84_SOCC300_05deg.nc].

We multiply the data by 0.1 to convert from [hg m-2] to [kg m-2] and also mask
out where the data is less than 1e-3. Otherwise, Greenland and other areas masked
in Fig. 2 show up as 0's in the data.
"""
import os
import time
from urllib.request import urlretrieve

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import xarray as xr
from matplotlib import colors
from numpy import nan

SHOW_PLOTS = False

# download and unzip source if not present, hg/m2
# pylint: disable=invalid-name
remote_source = (
    "https://bolin.su.se/data/ncscd/data/v3/netCDF/NCSCDv2_Circumpolar_netCDF_05deg.zip"
)
local_source = os.path.basename(remote_source)
if not os.path.isfile(local_source):
    urlretrieve(remote_source, local_source)
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")
os.system(f"unzip -oq {local_source}")

layers = {}
depth_horizons = [30, 100, 200, 300]
for depth in depth_horizons:
    filename = f"NCSCDv2_Circumpolar_WGS84_SOCC{depth}_05deg.nc"
    pathname = "NCSCDv2_Circumpolar_netCDF_05deg"
    ds = xr.open_dataset(os.path.join(pathname, filename))
    layers[depth] = ds["NCSCDv2"] * 0.1  # hg -> kg
    layers[depth] = layers[depth].sortby("lat")  # assumed in ILAMB v2.6
    layers[depth] = xr.where(layers[depth] < 1e-3, nan, layers[depth])

if SHOW_PLOTS:
    # reproducing Fig 2 from 10.5194/essd-5-393-2013
    import matplotlib.pyplot as plt

    # setup colormaps used in the paper
    cmap = colors.ListedColormap(["grey", "yellow", "orange", "brown"]).with_extremes(
        under="blue", over="red"
    )
    norm = colors.BoundaryNorm(
        [0.1, 10, 25, 50, 100],
        cmap.N,
    )

    fig, axs = plt.subplots(
        figsize=(20, 6),
        ncols=3,
        tight_layout=True,
        subplot_kw={
            "projection": ccrs.Orthographic(central_latitude=+90, central_longitude=0)
        },
    )
    layers[100].plot(ax=axs[0], cmap=cmap, norm=norm, transform=ccrs.PlateCarree())
    layers[200].plot(ax=axs[1], cmap=cmap, norm=norm, transform=ccrs.PlateCarree())
    layers[300].plot(ax=axs[2], cmap=cmap, norm=norm, transform=ccrs.PlateCarree())
    for ax in axs:
        ax.add_feature(
            cfeature.NaturalEarthFeature(
                "physical", "land", "110m", edgecolor="face", facecolor="0.875"
            ),
            zorder=-1,
        )
        ax.add_feature(
            cfeature.NaturalEarthFeature(
                "physical", "ocean", "110m", edgecolor="face", facecolor="0.750"
            ),
            zorder=-1,
        )
    plt.show()


cSoilAbove1m = layers[100]
cSoilAbove1m.name = "cSoilAbove1m"
cSoilAbove1m.attrs = {"long_name": "Soil Carbon in the top 1m", "units": "kg m-2"}

cSoil = layers[100] + layers[200] + layers[300]
cSoil.name = "cSoil"
cSoil.attrs = {"long_name": "Soil Carbon in the top 3m", "units": "kg m-2"}

global_attrs = {
    "title": "The Northern Circumpolar Soil Carbon Database (NCSCD) Soil Carbon Storage",
    "version": "2",
    "institutions": "Bolin Center for Climate Research",
    "source": f"{remote_source}",
    "history": "when comparing to figure 2 of the reference paper, it appears that hard 0's (we used a < 1e-3 test) should be masked, converted the units to [kg m-2]",
    "references": """
@ARTICLE{Hugelius2013a,
  author = {Hugelius G., Bockheim J.G., Camill P., Elberling B., Grosse G., Harden J.W., Johnson K., Jorgenson T., Koven C.D., Kuhry P., Michaelson G., Mishra U., Palmtag J., Ping C.-L., O'Donnell J., Schirrmeister L., Schuur E.A.G., Sheng Y., Smith L.C., Strauss J. and Yu Z.},
  title= {A new data set for estimating organic carbon storage to 3 m depth in soils of the northern circumpolar permafrost region},
  journal = {Earth System Science Data},
  volume = {5},
  year = {2013},
  page = {393--402},
  doi = {10.5194/essd-5-393-2013}
}
@ARTICLE{Hugelius2013b,
  author = {Hugelius, G., Tarnocai, C., Broll, G., Canadell, J. G., Kuhry, P., and Swanson, D. K.},
  title= {A new data set for estimating organic carbon storage to 3 m depth in soils of the northern circumpolar permafrost region},
  journal = {Earth System Science Data},
  volume = {5},
  year = {2013},
  page = {3--13},
  doi = {10.5194/essd-5-3-2013}
}
""",
}

for var in [cSoil, cSoilAbove1m]:
    out = xr.Dataset({var.name: var})
    out.attrs = global_attrs
    out.to_netcdf(f"{var.name}.nc")
