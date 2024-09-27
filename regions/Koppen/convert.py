"""
Convert the Beck Koppen region tif files into a netCDF file for use as regions in ILAMB.
"""

import re

import numpy as np
import xarray as xr


def parse_legend(filename: str = "legend.txt") -> tuple[list[str], list[str]]:
    labels = []
    names = []
    lines = open(filename, encoding="ISO 8859-1").readlines()
    for line in lines:
        m = re.match(r"\s*(\d+):\s+(\w*)\s+(.*)\s\[.*", line)
        if m:
            labels.append(m.group(2))
            names.append(m.group(3).strip())
    return labels, names


ds = (
    xr.open_dataset("Beck_KG_V1_present_0p5.tif")
    .sel({"band": 1})
    .rename({"x": "lon", "y": "lat", "band_data": "ids"})
    .drop_vars(["band", "spatial_ref"])
)
ds["ids"] = ds["ids"].astype(int) - 1
ds["labels"], ds["names"] = parse_legend()
ds["ids"].attrs.update(_FillValue=-1, labels="labels", names="names")

ds.attrs = {
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
ds.to_netcdf("Koppen_detailed.nc")

# Also generate a coarsened region dataset where we only use top level groups
dsc = xr.Dataset(
    {
        "ids": xr.where(
            ds["ids"] >= 0,
            np.array(
                [
                    {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4}[str(lbl.values)[0]]
                    for lbl in ds["labels"]
                ]
            )[ds["ids"]],
            -1,
        ),
        "labels": ["tropical", "arid", "temperate", "cold", "polar"],
        "names": [
            "Tropical climates",
            "Desert and semi-arid climates",
            "Temperate climates",
            "Continental climates",
            "Polar and alpine climates",
        ],
    }
)
dsc["ids"].attrs.update(_FillValue=-1, labels="labels", names="names")
dsc.attrs = ds.attrs
dsc.to_netcdf("Koppen.nc")
