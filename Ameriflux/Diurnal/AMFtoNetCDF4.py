"""
Converts csv files downloaded from the Ameriflux archive into a CF-compliant netCDF4 file.
"""
import argparse
import glob
import re
import xlrd
import numpy as np
import cftime as cf
from netCDF4 import Dataset
from cf_units import Unit

def ParseAMFUnitsTable():
    change = {"adimensional":"1", "deg C":"degC", "Decimal degrees":"degrees", "‰ (permil)":"1e-3"}
    prefix = {"n":"1e-9", "µ":"1e-6", "m":"1e-3"}

    desc = {}
    unit = {}
    for line in open("amf.txt").readlines():
        line = line.replace("\n", "")
        if "TIMESTAMP" in line: continue
        if line.count("\t") == 0: continue
        key, d, u = line.split("\t")

        match = re.search("(.*)mol(.*)", u.split()[0])
        if match:
            U = u.replace("%smol" % (match.group(1)),
                          "%s mol" % (prefix[match.group(1)]))
            u = U.replace(match.group(2), "")
            d = d + " " + match.group(2)
            
        for chg in change.keys():
            u = u.replace(chg,change[chg])
        
        try:
            Unit(u)
            desc[key] = d
            unit[key] = u
        except:
            pass
    return desc, unit

def FindBaseName(name, names):
    name = name.split("_")
    base = ""
    for i in range(len(name)):
        tmp = "_".join(name[:(i+1)])
        if tmp in names:
            base = tmp
    if base == "":
        print(name,names)
    return base

def ParseAMFExcelFile(filename):
    data = {}    
    with xlrd.open_workbook(filename) as wb:
        ws = wb.sheet_by_index(0)
        for i in range(1,ws.nrows):
            val = ws.cell_value(i,4)
            try:
                val = float(val)
                if val.is_integer(): val = int(val)
            except ValueError:
                pass
            data[ws.cell_value(i,3)] = val
    return data
    
def ToNetCDF4(dset,name,lat,lon,t,tb,v,attributes=None,prealloc=[]):

    if not dset.dimensions.keys():
        dset.createDimension("ndata",size=1)
        dset.createDimension("nb",size=2)
        dset.createDimension("time")
        T = dset.createVariable("time","double",("time"))
        T.setncattr("units","days since 1850-01-01 00:00:00")
        T.setncattr("calendar","standard")
        T.setncattr("axis","T")
        T.setncattr("long_name","time")
        T.setncattr("standard_name","time")
        T.setncattr("bounds","time_bnds")
        T[...] = t
        TB = dset.createVariable("time_bnds","double",("time","nb"))
        TB[...] = tb
        LA = dset.createVariable("lat","double",("ndata"))
        LA[...] = np.asarray([lat])
        LO = dset.createVariable("lon","double",("ndata"))
        LO[...] = np.asarray([lon])

    V = dset.createVariable(vname,"double",("time","ndata"),zlib=True,chunksizes=(t.size,1))
    V[...] = v.reshape((-1,1))
    if attributes:
       for key in attributes.keys():
            V.setncattr(key,attributes[key])

csvs = glob.glob("*.csv")
xlss = glob.glob("*.xlsx")
sites = [csv.split("_")[1] for csv in csvs]

parser = argparse.ArgumentParser(__doc__)
parser.add_argument('--sites', dest="sites", metavar='SITES', type=str, nargs="+", default=sites)
args = parser.parse_args()
desc, unit = ParseAMFUnitsTable()

for site in args.sites:
    csv = [f for f in csvs if site in f]
    xls = [f for f in xlss if site in f]
    if not csv:
        print("No csv file found for the site %s" % (site))
        continue
    if len(csv) > 1:
        print("Multiple csv files found for the site %s: " % (site), ", ".join(csvs))
        continue
    csv = csv[0]

    print("Parsing site %s..." % site)
    # parse the xlsx file
    if not xls:
        print("  No Excel file found")
        gattrs = {}
    else:
        print("    getting meta data from %s" % xls[0])
        gattrs = ParseAMFExcelFile(xls[0])
    version = float(csv.replace(".csv", "").split("_")[-1].replace("-", "."))
    gattrs["version"] = version

    # search for the site latitude/longitude
    lats = [key for key in gattrs.keys() if "LOCATION_LAT" in key]
    lons = [key for key in gattrs.keys() if "LOCATION_LONG" in key]
    if not ((len(lats) == 1) and (len(lons) == 1)):
        print("  Unknown site location, skipping")
        continue
    lat = gattrs[lats[0]]
    lon = gattrs[lons[0]]
    
    # parse the csv file
    rec = np.genfromtxt(csv, delimiter=",", skip_header=2, names=True) #, max_rows=10)
    t0 = rec['TIMESTAMP_START'].astype(str)
    t0 = cf.date2num([cf.datetime(int(t[:4]), int(t[4:6]), int(t[6:8]), int(t[8:10]), int(t[10:12]))
                      for t in t0], "days since 1850-01-01")
    tf = rec['TIMESTAMP_END'].astype(str)
    tf = cf.date2num([cf.datetime(int(t[:4]), int(t[4:6]), int(t[6:8]), int(t[8:10]), int(t[10:12]))
                      for t in tf], "days since 1850-01-01")
    tb = np.vstack([t0, tf]).T
    t = tb.mean(axis=1)
    vnames = [vname for vname in rec.dtype.names if "TIMESTAMP" not in vname]
    with Dataset("AMF_%s.nc" % site, mode="w") as dset:
        for attr in gattrs.keys():
            dset.setncattr(attr,gattrs[attr])
            
        for vname in vnames:
            v = np.ma.masked_values(rec[vname], -9999)
            if v.mask.all():
                # if all the data in a variable is marked invalid,
                # then we will skip the variable
                print("    skipping %s, all invalid" % vname)
                continue
            print("    encoding %s" % vname)
            base = FindBaseName(vname, unit.keys())
            ToNetCDF4(dset,vname,lat,lon,t,tb,v,attributes={"units":unit[base],
                                                            "standard_name":desc[base]},
                      prealloc = vnames)
