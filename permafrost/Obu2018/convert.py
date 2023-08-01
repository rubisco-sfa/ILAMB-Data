import os
import time

import cftime
import matplotlib.pyplot as plt
import numpy as np
import requests
import xarray as xr
from tqdm import tqdm


def download_file(remote_source):
    local_source = os.path.basename(remote_source)
    if not os.path.isfile(local_source):
        resp = requests.get(remote_source, stream=True)
        total_size = int(resp.headers.get("content-length"))
        with open(local_source, "wb") as fdl:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=local_source,
                ascii=True,
            ) as pbar:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk:
                        fdl.write(chunk)
                        pbar.update(len(chunk))


remote_source = "https://store.pangaea.de/Publications/ObuJ-etal_2018/UiO_PEX_5.0_20181127_2000_2016_25km.nc"
local_source = os.path.basename(remote_source)
download_file(remote_source)
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# open and reproject
ds = xr.open_dataset(local_source)
dsr = ds.rio.write_crs("EPSG:3995").rio.reproject("EPSG:4326")

# create the classification for continuous / discontinuous permafrost from Brown.
cls = (dsr["PerProb"] >= 0.9) * 1.0 + (dsr["PerProb"] < 0.9) * (
    dsr["PerProb"] >= 0.5
) * 2.0
cls = xr.where(cls < 1e-6, np.nan, cls)
cls = cls.expand_dims({"time": [cftime.DatetimeNoLeap(2000, 1, 1)]})

# convert to dataset and annotate
ds = cls.to_dataset(name="permafrost_extent")
ds = ds.rename({"x": "lon", "y": "lat"}).drop("spatial_ref")
ds["time_bnds"] = xr.DataArray(
    np.asarray(
        [[cftime.DatetimeNoLeap(2000, 1, 1), cftime.DatetimeNoLeap(2016, 1, 1)]]
    ),
    dims=("time", "nb"),
)
ds.attrs = {
    "title": "Ground Temperature Map, 2000-2016, Northern Hemisphere Permafrost",
    "version": "2019.04",
    "institutions": "Alfred Wegener Institute, Helmholtz Centre for Polar and Marine Research",
    "source": f"{remote_source}",
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/ILAMB-Data/blob/main/NSIDC/convert.py",
    "references": """
@ARTICLE{Obu2018,
    author = {Jaroslav Obu and Sebastian Westermann and Andreas Kääb and Annett Bartsc,
    title= {Ground Temperature Map, 2000-2016, Northern Hemisphere Permafrost},
    journal = {Alfred Wegener Institute, Helmholtz Centre for Polar and Marine Research, Bremerhaven},
    year = {2018},
    doi = {10.1594/PANGAEA.888600}
}
@ARTICLE{Obu2019,
    author = {Jaroslav Obu and Sebastian Westermann and Annett Bartsch and Nikolai Berdnikov and Hanne H. Christiansen and Avirmed Dashtseren and Reynald Delaloye and Bo Elberling and Bernd Etzelmüller and Alexander Kholodov and Artem Khomutov and Andreas Kääb and Marina O. Leibman and Antoni G. Lewkowicz and Santosh K. Panda and Vladimir Romanovsky and Robert G. Way and Andreas Westergaard-Nielsen and Tonghua Wu and Jambaljav Yamkhin and Defu Zou},
    title= {Northern Hemisphere permafrost map based on TTOP modelling for 2000–2016 at 1 km2 scale},
    journal = {Earth-Science Reviews},
    year = {2019},
    doi = {10.1016/j.earscirev.2019.04.023}
}""",
}
ds.to_netcdf(
    "Obu2018.nc",
    encoding={
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)

plt.show()
