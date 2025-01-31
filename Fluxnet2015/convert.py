"""This conversion script will automatically download Fluxnet2015 CC-By-4.0 Data and
reformat it into a CF-compliant netCDF file. However, you must still make the data
request manually. First, sign into your Fluxnet account and navigate to:

https://fluxnet.org/data/download-data/

Choose "SUBSET data product", select all sites, and check "BADM Zip File For All
FLUXNET2015 Dataset Sites". The click the "Download All Files" button. It will take some
time, but eventually the site will present you with a series of links you are supposed
to click to download. Instead, save this HTML page as "manifest.html" into the directory
where you will execute this script.
"""

import os
import time
from glob import glob
from pathlib import Path
from zipfile import ZipFile

import cftime as cf
import numpy as np
import pandas as pd
import requests
import xarray as xr
from bs4 import BeautifulSoup
from tqdm import tqdm

RAW_PATH = "_raw"


def download_file(remote_source: str, output_path: str = "_raw") -> str:
    output_path = Path(output_path)
    if not output_path.is_dir():
        output_path.mkdir(parents=True, exist_ok=True)
    local_source = output_path / os.path.basename(remote_source).split("?")[0]
    if os.path.isfile(local_source):
        return local_source
    resp = requests.get(remote_source, stream=True, timeout=10)
    resp.raise_for_status()
    with open(local_source, "wb") as fdl:
        with tqdm(
            total=int(resp.headers["Content-Length"]),
            unit="B",
            unit_scale=True,
            desc=str(local_source),
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fdl.write(chunk)
                    pbar.update(len(chunk))
    return local_source


# Download the files listed in "manifest.html", see instructions above.
html = open("manifest.html").read()
soup = BeautifulSoup(html, "html.parser")
links = [link.attrs["href"] for link in soup.find_all("a", {"class": "download-link"})]
for link in links:
    download_file(link, output_path=RAW_PATH)

# Unzip just the monthly data
for zipfile in tqdm(glob(f"{RAW_PATH}/*.zip"), desc="Unzipping"):
    csvfile = Path(zipfile.replace("SUBSET", "SUBSET_MM").replace("zip", "csv"))
    with ZipFile(zipfile) as fzip:
        if csvfile.is_file():
            continue
        if [fi for fi in fzip.filelist if fi.filename == csvfile.name]:
            fzip.extract(csvfile.name, path=RAW_PATH)
        else:
            fzip.extractall(path=RAW_PATH)

# Process the site info and store
site_info = Path("site_info.feather")
if site_info.is_file():
    dfi = pd.read_feather(site_info)
else:
    excels = [f for f in glob(f"{RAW_PATH}/*.xlsx") if "_MM_" in f]
    df = pd.read_excel(excels[0])
    df = df.rename(columns={"SITE_ID": "site"})
    q = df[(df.VARIABLE == "LOCATION_LAT") | (df.VARIABLE == "LOCATION_LONG")]
    # sites can have repeated locations in the database
    for lbl, grp in q.groupby("site"):
        q = q.drop(grp.sort_values("GROUP_ID").iloc[2:].index)
    dfi = q.pivot(columns="VARIABLE", index="site", values="DATAVALUE")
    dfi["LOCATION_LAT"] = dfi["LOCATION_LAT"].astype(float)
    dfi["LOCATION_LONG"] = dfi["LOCATION_LONG"].astype(float)
    dfi.to_feather(site_info)

# Concat all the csv files into a dataframe
csvs = glob(f"{RAW_PATH}/*.csv")
df = []
for csv in tqdm(csvs, desc="Concatenate csv's"):
    site = (csv.split("/")[-1]).split("_")[1]
    dfs = pd.read_csv(csv, na_values=-9999)
    dfs["site"] = site
    dfs = dfs.set_index(["TIMESTAMP", "site"])
    df.append(dfs)
df = pd.concat(df)

# As per https://fluxnet.org/data/fluxnet2015-dataset/variables-quick-start-guide/ we
# will use the difference in partitioning methods as uncertainty for carbon variables.
for var in ["GPP", "RECO"]:
    df[f"{var}_VUT_MEAN"] = 0.5 * (df[f"{var}_DT_VUT_REF"] + df[f"{var}_NT_VUT_REF"])
    df[f"{var}_VUT_UNCERT"] = 0.5 * np.abs(
        (df[f"{var}_DT_VUT_REF"] - df[f"{var}_NT_VUT_REF"])
    )

# Define the Fluxnet to CMOR mapping, hard coding units because their csv file is silly.
# You cannot read units out of the table because the are different depending on the
# temporal resolution.
dfv = pd.DataFrame(
    [
        {"standard_name": name, "cmor": cmor, "fluxnet": fluxnet, "units": units}
        for name, cmor, fluxnet, units in [
            ["ecosystem_respiration", "reco", "RECO_VUT_MEAN", "g m-2 d-1"],
            ["gross_primary_productivity", "gpp", "GPP_VUT_MEAN", "g m-2 d-1"],
            [
                "ecosystem_respiration standard_error",
                "reco_uncert",
                "RECO_VUT_UNCERT",
                "g m-2 d-1",
            ],
            [
                "gross_primary_productivity standard_error",
                "gpp_uncert",
                "GPP_VUT_UNCERT",
                "g m-2 d-1",
            ],
            ["latent_heat", "hfls", "LE_F_MDS", "W m-2"],
            ["net_ecosystem_exchange", "nee", "NEE_VUT_REF", "g m-2 d-1"],
            ["precipitation", "pr", "P_F", "mm d-1"],
            ["sensible_heat", "hfss", "H_F_MDS", "W m-2"],
            ["surface_air_temperature", "tas", "TA_F", "degC"],
            ["surface_downward_longwave_radiation", "rlds", "LW_IN_F", "W m-2"],
            ["surface_upward_longwave_radiation", "rlus", "LW_OUT", "W m-2"],
            ["surface_downward_shortwave_radiation", "rsds", "SW_IN_F", "W m-2"],
            ["surface_upward_shortwave_radiation", "rsus", "SW_OUT", "W m-2"],
            ["surface_net_radiation", "rns", "NETRAD", "W m-2"],
        ]
    ]
)

# Convert the dataframe to a dataset and cleanup
df = df[dfv["fluxnet"]]
ds = df.to_xarray()
for _, row in dfv.iterrows():
    ds[row["fluxnet"]].attrs = {
        "standard_name": row["standard_name"],
        "units": row["units"],
    }
ds = ds.rename({fluxnet: cmor for fluxnet, cmor in zip(dfv["fluxnet"], dfv["cmor"])})
ds = ds.rename({"TIMESTAMP": "time"})
lat = dfi.loc[ds["site"], "LOCATION_LAT"].to_xarray()
lon = dfi.loc[ds["site"], "LOCATION_LONG"].to_xarray()
lat.attrs = {"standard_name": "latitude", "units": "degrees_north"}
lon.attrs = {"standard_name": "longitude", "units": "degrees_east"}

# Rewrite time and include bounds
year = np.round(ds["time"] / 100).astype(int)
month = ds["time"] - year * 100
ds["time"] = [cf.DatetimeNoLeap(y, m, 15) for y, m in zip(year, month)]
ds["time_bnds"] = xr.DataArray(
    dims=["time", "nb"],
    data=np.asarray(
        [
            [cf.DatetimeNoLeap(y, m, 1) for y, m in zip(year, month)],
            [
                cf.DatetimeNoLeap(y + (m == 12), 1 if m == 12 else (m + 1), 1)
                for y, m in zip(year, month)
            ],
        ]
    ).T,
)
ds["time"].encoding["units"] = "days since 1850-01-01"
ds["time"].encoding["bounds"] = "time_bnds"
ds["time_bnds"].encoding["units"] = "days since 1850-01-01"
ds["site"].attrs["name"] = "Fluxnet site id"

# Add a long- and shortwave net radiation
ds["rsns"] = ds["rsds"] - ds["rsus"]
ds["rsns"].attrs = {
    "standard_name": "surface_net_shortwave_radiation",
    "units": "W m-2",
}
ds["rlns"] = ds["rlds"] - ds["rlus"]
ds["rlns"].attrs = {
    "standard_name": "surface_net_longwave_radiation",
    "units": "W m-2",
}

# Define the global attributes
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime("manifest.html"))
)
generate_stamp = time.strftime("%Y-%m-%d")
attrs = {
    "title": "Fluxnet2015",
    "version": 2015,
    "institutions": "The Fluxnet Community",
    "source": "Data downloaded from the Fluxnet community data portal https://fluxnet.org/data/download-data/",
    "history": f"""
{download_stamp}: Data downloaded;
{generate_stamp}: Converted to netcdf using https://github.com/rubisco-sfa/ILAMB-Data/blob/master/Fluxnet2015/convert.py""",
    "references": """
@ARTICLE{pastorello_fluxnet2015_2020,
	title = {The {FLUXNET2015} dataset and the {ONEFlux} processing pipeline for eddy covariance data},
	volume = {7},
	issn = {2052-4463},
	doi = {10.1038/s41597-020-0534-3},
	number = {1},
	journal = {Scientific Data},
	author = {Pastorello, Gilberto and Trotta, Carlo and Canfora, Eleonora and Chu, Housen and Christianson, Danielle and Cheah, You-Wei and Poindexter, Cristina and Chen, Jiquan and Elbashandy, Abdelrahman and Humphrey, Marty and Isaac, Peter and Polidori, Diego and Ribeca, Alessio and van Ingen, Catharine and Zhang, Leiming and Amiro, Brian and Ammann, Christof and Arain, M. Altaf and ArdÃ¶, Jonas and Arkebauer, Timothy and Arndt, Stefan K. and Arriga, Nicola and Aubinet, Marc and Aurela, Mika and Baldocchi, Dennis and Barr, Alan and Beamesderfer, Eric and Marchesini, Luca Belelli and Bergeron, Onil and Beringer, Jason and Bernhofer, Christian and Berveiller, Daniel and Billesbach, Dave and Black, Thomas Andrew and Blanken, Peter D. and Bohrer, Gil and Boike, Julia and Bolstad, Paul V. and Bonal, Damien and Bonnefond, Jean-Marc and Bowling, David R. and Bracho, Rosvel and Brodeur, Jason and BrÃ¼mmer, Christian and Buchmann, Nina and Burban, Benoit and Burns, Sean P. and Buysse, Pauline and Cale, Peter and Cavagna, Mauro and Cellier, Pierre and Chen, Shiping and Chini, Isaac and Christensen, Torben R. and Cleverly, James and Collalti, Alessio and Consalvo, Claudia and Cook, Bruce D. and Cook, David and Coursolle, Carole and Cremonese, Edoardo and Curtis, Peter S. and Dâ€™Andrea, Ettore and da Rocha, Humberto and Dai, Xiaoqin and Davis, Kenneth J. and De Cinti, Bruno and de Grandcourt, Agnes and De Ligne, Anne and De Oliveira, Raimundo C. and Delpierre, Nicolas and Desai, Ankur R. and Di Bella, Carlos Marcelo and di Tommasi, Paul and Dolman, Han and Domingo, Francisco and Dong, Gang and Dore, Sabina and Duce, Pierpaolo and DufrÃªne, Eric and Dunn, Allison and DuÅ¡ek, JiÅ™Ã­ and Eamus, Derek and Eichelmann, Uwe and ElKhidir, Hatim Abdalla M. and Eugster, Werner and Ewenz, Cacilia M. and Ewers, Brent and Famulari, Daniela and Fares, Silvano and Feigenwinter, Iris and Feitz, Andrew and Fensholt, Rasmus and Filippa, Gianluca and Fischer, Marc and Frank, John and Galvagno, Marta and Gharun, Mana and Gianelle, Damiano and Gielen, Bert and Gioli, Beniamino and Gitelson, Anatoly and Goded, Ignacio and Goeckede, Mathias and Goldstein, Allen H. and Gough, Christopher M. and Goulden, Michael L. and Graf, Alexander and Griebel, Anne and Gruening, Carsten and GrÃ¼nwald, Thomas and Hammerle, Albin and Han, Shijie and Han, Xingguo and Hansen, Birger Ulf and Hanson, Chad and Hatakka, Juha and He, Yongtao and Hehn, Markus and Heinesch, Bernard and Hinko-Najera, Nina and HÃ¶rtnagl, Lukas and Hutley, Lindsay and Ibrom, Andreas and Ikawa, Hiroki and Jackowicz-Korczynski, Marcin and JanouÅ¡, Dalibor and Jans, Wilma and Jassal, Rachhpal and Jiang, Shicheng and Kato, Tomomichi and Khomik, Myroslava and Klatt, Janina and Knohl, Alexander and Knox, Sara and Kobayashi, Hideki and Koerber, Georgia and Kolle, Olaf and Kosugi, Yoshiko and Kotani, Ayumi and Kowalski, Andrew and Kruijt, Bart and Kurbatova, Julia and Kutsch, Werner L. and Kwon, Hyojung and Launiainen, Samuli and Laurila, Tuomas and Law, Bev and Leuning, Ray and Li, Yingnian and Liddell, Michael and Limousin, Jean-Marc and Lion, Marryanna and Liska, Adam J. and Lohila, Annalea and LÃ³pez-Ballesteros, Ana and LÃ³pez-Blanco, EfrÃ©n and Loubet, Benjamin and Loustau, Denis and Lucas-Moffat, Antje and LÃ¼ers, Johannes and Ma, Siyan and Macfarlane, Craig and Magliulo, Vincenzo and Maier, Regine and Mammarella, Ivan and Manca, Giovanni and Marcolla, Barbara and Margolis, Hank A. and Marras, Serena and Massman, William and Mastepanov, Mikhail and Matamala, Roser and Matthes, Jaclyn Hatala and Mazzenga, Francesco and McCaughey, Harry and McHugh, Ian and McMillan, Andrew M. S. and Merbold, Lutz and Meyer, Wayne and Meyers, Tilden and Miller, Scott D. and Minerbi, Stefano and Moderow, Uta and Monson, Russell K. and Montagnani, Leonardo and Moore, Caitlin E. and Moors, Eddy and Moreaux, Virginie and Moureaux, Christine and Munger, J. William and Nakai, Taro and Neirynck, Johan and Nesic, Zoran and Nicolini, Giacomo and Noormets, Asko and Northwood, Matthew and Nosetto, Marcelo and Nouvellon, Yann and Novick, Kimberly and Oechel, Walter and Olesen, JÃ¸rgen Eivind and Ourcival, Jean-Marc and Papuga, Shirley A. and Parmentier, Frans-Jan and Paul-Limoges, Eugenie and Pavelka, Marian and Peichl, Matthias and Pendall, Elise and Phillips, Richard P. and Pilegaard, Kim and Pirk, Norbert and Posse, Gabriela and Powell, Thomas and Prasse, Heiko and Prober, Suzanne M. and Rambal, Serge and Rannik, Ãœllar and Raz-Yaseef, Naama and Reed, David and de Dios, Victor Resco and Restrepo-Coupe, Natalia and Reverter, Borja R. and Roland, Marilyn and Sabbatini, Simone and Sachs, Torsten and Saleska, Scott R. and SÃ¡nchez-CaÃ±ete, Enrique P. and Sanchez-Mejia, Zulia M. and Schmid, Hans Peter and Schmidt, Marius and Schneider, Karl and Schrader, Frederik and Schroder, Ivan and Scott, Russell L. and SedlÃ¡k, Pavel and Serrano-OrtÃ­z, PenÃ©lope and Shao, Changliang and Shi, Peili and Shironya, Ivan and Siebicke, Lukas and Å igut, Ladislav and Silberstein, Richard and Sirca, Costantino and Spano, Donatella and Steinbrecher, Rainer and Stevens, Robert M. and Sturtevant, Cove and Suyker, Andy and Tagesson, Torbern and Takanashi, Satoru and Tang, Yanhong and Tapper, Nigel and Thom, Jonathan and Tiedemann, Frank and Tomassucci, Michele and Tuovinen, Juha-Pekka and Urbanski, Shawn and Valentini, Riccardo and van der Molen, Michiel and van Gorsel, Eva and van Huissteden, Ko and Varlagin, Andrej and Verfaillie, Joseph and Vesala, Timo and Vincke, Caroline and Vitale, Domenico and Vygodskaya, Natalia and Walker, Jeffrey P. and Walter-Shea, Elizabeth and Wang, Huimin and Weber, Robin and Westermann, Sebastian and Wille, Christian and Wofsy, Steven and Wohlfahrt, Georg and Wolf, Sebastian and Woodgate, William and Li, Yuelin and Zampedri, Roberto and Zhang, Junhui and Zhou, Guoyi and Zona, Donatella and Agarwal, Deb and Biraud, Sebastien and Torn, Margaret and Papale, Dario},
	month = jul,
	year = {2020},
	pages = {225}
}""",
}

for varname in tqdm(ds, desc="Writing netcdf files"):
    if "uncert" in varname or "_bnds" in varname:
        continue
    out = ds[varname].to_dataset(name=varname)
    uname = f"{varname}_uncert"
    if uname in ds:
        out[uname] = ds[uname]
        out[varname].attrs["ancillary_variables"] = uname
    out["lat"] = ("site",lat.data)
    out["lon"] = ("site",lon.data)
    out[varname].attrs["coordinates"] = "lat lon"
    out["time_bnds"] = ds["time_bnds"]
    out.attrs = attrs
    out.to_netcdf(f"{varname}.nc")
