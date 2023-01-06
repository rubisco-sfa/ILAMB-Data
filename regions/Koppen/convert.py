"""
Convert the Beck Koppen region tif files into a netCDF file for use as regions in ILAMB.
"""
import re

import intake
import xarray as xr

src = intake.open_rasterio(
    "zip+https://figshare.com/ndownloader/files/12407516/Beck_KG_V1.zip!Beck_KG_V1_present_0p5.tif"
)
da = src.read()
da = da.sel({"band": 1})
da = da.rename({"x": "lon", "y": "lat"}).drop(["band"])
da = da.astype(int) - 1
da = xr.where(da["lat"] > -58, da, -1)  # remove Antarctica

# At this time I am unable to figure out how to pull a single text file out of a
# remote zip file using intake and so we depend here on you having extracted the
# zip file referenced above locally in this directory.
labels = []
names = []
lines = open("legend.txt", encoding="ISO 8859-1").readlines()
for line in lines:
    m = re.match("\s*(\d+):\s+(\w*)\s+(.*)\s\[.*", line)
    if m:
        labels.append(m.group(2))
        names.append(m.group(3).strip())
da.attrs.update(_FillValue=-1, labels="labels", names="names")
ds = xr.Dataset({"ids": da, "labels": labels, "names": names})

ds.attrs = {
    "comment": "Antarctica has been removed from the original as the purpose of this map is to provide regions for benchmarking biogeochemical cycles",
    "source": "https://doi.org/10.6084/m9.figshare.6396959",
    "reference": """
@article{Beck2018,
  author  = {H.E. Beck and N.E. Zimmermann and T.R. McVicar and N. Vergopolan and A. Berg and E.F. Wood},
  title   = {Present and future K\"{o}ppen-Geiger climate classification maps at 1-km resolution},
  journal = {Nature Scientific Data},
  year    = {2018},
  doi     = {doi:10.1038/sdata.2018.214},
}
""",
}
ds.to_netcdf("Koppen.nc")
