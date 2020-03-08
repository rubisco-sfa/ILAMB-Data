import argparse
import glob
from netCDF4 import Dataset
import numpy as np
import xlrd,os,time

remote_source = "doi:10.18160/GCP-2016"
gist_source = "blah"
local_source = "Global_Carbon_Budget_2016v1.0.xlsx"
stamp = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(local_source)))

# Parse information from the XLS workbook
land_sink_uncertainty      = 0.8 # Pg
landuse_change_uncertainty = 0.5 # Pg
uncertainty = np.linalg.norm([land_sink_uncertainty,landuse_change_uncertainty])
year = []; luc = []; ls = []
with xlrd.open_workbook(local_source) as wb:
    ws = wb.sheet_by_index(1)
    for i in range(22,79):
        year.append(float(ws.cell_value(i,0)))
        luc .append(float(ws.cell_value(i,2)))
        ls  .append(float(ws.cell_value(i,5)))
year = np.asarray(year)
luc  = np.asarray(luc )
ls   = np.asarray(ls  )

# Convert information into arrays ready to write
tb = (np.asarray([year,year+1]).T-1850)*365
t  = tb.mean(axis=1)
nbp = ls-luc
nbp_bnds = np.asarray([nbp-uncertainty,nbp+uncertainty]).T

with Dataset("nbp_%4d-%4d.nc" % (year.min(),year.max()+1), mode="w") as dset:
    
    # dimensions
    dset.createDimension("time", size=t.size)
    dset.createDimension("nb",   size=2)

    # time
    T = dset.createVariable("time", t.dtype, ("time"))
    T[...] = t
    T.units = "days since 1850-01-01 00:00:00"
    T.calendar = "noleap"
    T.bounds = "time_bounds"

    # time bounds
    TB = dset.createVariable("time_bounds", t.dtype, ("time", "nb"))
    TB[...] = tb
    
    # data
    D = dset.createVariable("nbp", nbp.dtype, ("time", ), fill_value = -99.99)
    D[...] = nbp
    D.units = "Pg yr-1"
    D.standard_name = "surface_net_downward_mass_flux_of_carbon_dioxide_expressed_as_carbon_due_to_all_land_processes"
    D.actual_range = np.asarray([nbp.min(),nbp.max()])
    D.bounds = "nbp_bnds"
    D.comment = "Computed by subtracting land use change emissions from the land sink"

    # data bounds
    DB = dset.createVariable("nbp_bnds", nbp.dtype, ("time", "nb"), fill_value = -99.99)
    DB[...] = nbp_bnds
    DB.units = "Pg yr-1"
    DB.standard_name = "uncertainty bounds for global net downward land carbon flux"

    # global attributes
    dset.title = "Land anthropogenic carbon flux estimates"
    dset.institution = "Global Carbon Project"
    dset.history = """
%s: downloaded source from %s
%s: converted to netCDF with %s""" % (stamp, remote_source, stamp, gist_source)
    dset.references = """
@Article{essd-8-605-2016,
author = {Le Quéré, C. and Andrew, R. M. and Canadell, J. G. and Sitch, S. and Korsbakken, J. I. and Peters, G. P. and Manning, A. C. and Boden, T. A. and Tans, P. P. and Houghton, R. A. and Keeling, R. F. and Alin, S. and  Andrews, O. D. and Anthoni, P. and Barbero, L. and Bopp, L. and Chevallier, F. and Chini, L. P. and Ciais, P. and Currie, K. and Delire, C. and Doney, S. C. and Friedlingstein, P. and Gkritzalis, T. and  Harris, I. and Hauck, J. and Haverd, V. and Hoppema, M. and Klein Goldewijk, K. and Jain, A. K. and Kato, E. and Körtzinger, A. and Landschützer, P. and Lefèvre, N. and  Lenton, A. and Lienert, S. and Lombardozzi, D. and Melton, J. R. and Metzl, N. and Millero, F. and Monteiro, P. M. S. and Munro, D. R. and Nabel, J. E. M. S. and Nakaoka, S. and O’Brien, K. and Olsen, A. and Omar, A. M. and  Ono, T. and Pierrot, D. and Poulter, B. and Rödenbeck, C. and Salisbury, J. and Schuster, U. and Schwinger, J. and Séférian, R. and Skjelvan, I. and Stocker, B. D. and Sutton, A. J. and Takahashi, T. and Tian, H. and Tilbrook, B. and van der Laan-Luijkx, I. T. and van der Werf, G. R. and Viovy, N. and Walker, A. P. and Wiltshire, A. J. and Zaehle, S.},
title = {Global Carbon Budget 2016},
journal = {Earth System Science Data},
volume = {8},
year = {2016},
pages = {605--649},
url = {http://www.earth-syst-sci-data.net/8/605/2016/},
doi = {doi:10.5194/essd-8-605-2016}
}"""

