from ILAMB.Variable import Variable
import os
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

def CreateVariableComparisonArray(data,cmap,name,filename,ncolors=7):

    # initialization stuff
    mean_cmap = plt.cm.get_cmap(cmap,ncolors)
    bias_cmap = plt.cm.get_cmap("seismic",ncolors)
    sources = sorted(list(data.keys()))
    n = len(data)
    
    # interpolate all variables to a composed grid
    lat = None; lon = None
    for i in range(n):
        if lat is None:
            lat = data[sources[i]].lat
            lon = data[sources[i]].lon
        else:
            lat = np.hstack([lat,data[sources[i]].lat])
            lon = np.hstack([lon,data[sources[i]].lon])
    lat = np.unique(lat)
    lon = np.unique(lon)
    for i in range(n):
        data[sources[i]] = data[sources[i]].interpolate(lat=lat,lon=lon)
        
    # find limits of the difference
    bias = None
    for i in range(n):
        for j in range(n):
            if i < j:
                a = data[sources[i]]
                b = data[sources[j]]
                if bias is None:
                    bias = np.abs(a.data-b.data).compressed()
                else:
                    bias = np.hstack([bias,np.abs(a.data-b.data).compressed()])
    bias = np.percentile(bias,98)
    
    # find limits of the mean
    values = None
    for key in data:
        if values is None:
            values = data[key].data.compressed()
        else:
            values = np.hstack([values,data[key].data.compressed()])
    limits = np.percentile(values,[2,98])
    
    # plots
    f = 1.5
    fig = plt.figure(figsize=(f*4*n,(f+0.5*(n==2))*2.2*n),dpi=200)
    mean_ax = None
    bias_ax = None
    for i in range(n):
        for j in range(n):
            if i == j:
                a = data[sources[i]]
                lat = np.hstack([a.lat_bnds[:,0],a.lat_bnds[-1,-1]])
                lon = np.hstack([a.lon_bnds[:,0],a.lon_bnds[-1,-1]])
                ax = fig.add_subplot(n,n,n*i+j+1,projection=ccrs.Robinson())
                if i == 0: mean_ax = ax
                mean_plot = ax.pcolormesh(lon,lat,a.data,cmap=mean_cmap,vmin=limits[0],vmax=limits[1],transform=ccrs.PlateCarree())
                ax.set_title(sources[i])
                ax.add_feature(cfeature.NaturalEarthFeature('physical','land','110m',
                                                            edgecolor='face',
                                                            facecolor='0.875'),zorder=-1)
                ax.add_feature(cfeature.NaturalEarthFeature('physical','ocean','110m',
                                                            edgecolor='face',
                                                            facecolor='0.750'),zorder=-1)
            else:
                a = data[sources[i]]
                b = data[sources[j]]
                if i < j:
                    ax = fig.add_subplot(n,n,n*i+j+1,projection=ccrs.Robinson())
                    if i == 0 and j == (n-1): bias_ax = ax
                    bias_plot = ax.pcolormesh(lon,lat,a.data-b.data,vmin=-bias,vmax=+bias,cmap=bias_cmap,transform=ccrs.PlateCarree())
                    ax.set_title(sources[i])
                    ax.add_feature(cfeature.NaturalEarthFeature('physical','land','110m',
                                                                edgecolor='face',
                                                                facecolor='0.875'),zorder=-1)
                    ax.add_feature(cfeature.NaturalEarthFeature('physical','ocean','110m',
                                                                edgecolor='face',
                                                                facecolor='0.750'),zorder=-1)
                    ax.set_title("%s - %s" % (sources[i],sources[j]))
                else:
                    ax = fig.add_subplot(n,n,n*i+j+1)
                    mask = a.data.mask + b.data.mask
                    x = np.ma.masked_array(a.data,mask=mask).compressed()
                    y = np.ma.masked_array(b.data,mask=mask).compressed()
                    ax.plot([limits[0],limits[1]],[limits[0],limits[1]],'--r')
                    ax.scatter(x,y,color='k',s=0.6,alpha=0.1,linewidths=0)
                    ax.set_xlim(limits[0],limits[1])
                    ax.set_ylim(limits[0],limits[1])
                    ax.set_xlabel(sources[i])
                    ax.set_ylabel(sources[j])
                    ax.spines['right'].set_color('none')
                    ax.spines['top'].set_color('none')

    axins = inset_axes(mean_ax,
                       width="100%",
                       height="30%",
                       bbox_to_anchor=(0.0,1.2,1.0,0.3),
                       bbox_transform=mean_ax.transAxes,
                       borderpad=0)
    fig.colorbar(mean_plot,orientation='horizontal',cax=axins,label="%s [%s]" % (name,a.unit))

    axins_bias = inset_axes(bias_ax,
                            width="100%",
                            height="30%",
                            bbox_to_anchor=(0.0,1.2,1.0,0.3),
                            bbox_transform=bias_ax.transAxes,
                            borderpad=0)
    fig.colorbar(bias_plot,orientation='horizontal',cax=axins_bias,label="Differences [%s]" % (a.unit))

    fig.savefig(filename)
    plt.close()

    
if __name__ == "__main__":

    # setup
    from ILAMB.Regions import Regions
    r = Regions()
    r.addRegionNetCDF4(os.path.join(os.environ['ILAMB_ROOT'],"DATA/regions/GlobalLand.nc"))
    data_dir = "./"
    """
    data = {}
    for fname in [os.path.join(os.environ['ILAMB_ROOT'],'DATA/reco/GBAF/reco_0.5x0.5.nc'),
                  os.path.join(data_dir,"FLUXCOM/reco.nc")]:
        source = fname.split("/")[-2]
        data[source] = Variable(filename=fname,variable_name="reco").integrateInTime(mean=True).convert("g m-2 d-1")
    CreateVariableComparisonArray(data,"Greens","Respiration","reco.png")
    
    data = {}
    for fname in [os.path.join(os.environ['ILAMB_ROOT'],'DATA/pr/CMAP/pr_0.5x0.5.nc'),
                  os.path.join(os.environ['ILAMB_ROOT'],'DATA/pr/GPCP2/pr_0.5x0.5.nc'),
                  os.path.join(data_dir,"GPCC/pr.nc"),
                  os.path.join(data_dir,"CLASS/pr.nc")]:
        source = fname.split("/")[-2]
        data[source] = Variable(filename=fname,variable_name="pr").integrateInTime(mean=True).convert("mm d-1")
        data[source].data.mask += r.getMask("global",data[source])
    CreateVariableComparisonArray(data,"Blues","Precipitation","pr.png")

    data = {}
    for fname in [os.path.join(os.environ['ILAMB_ROOT'],'DATA/runoff/LORA/LORA.nc'),
                  os.path.join(data_dir,"CLASS/mrro.nc")]:
        source = fname.split("/")[-2]
        data[source] = Variable(filename=fname,variable_name="mrro").integrateInTime(mean=True).convert("mm d-1")
    CreateVariableComparisonArray(data,"Blues","Runoff","mrro.png")

    data = {}
    for fname in [os.path.join(os.environ['ILAMB_ROOT'],'DATA/rns/CERES/rns_0.5x0.5.nc'),
                  os.path.join(os.environ['ILAMB_ROOT'],'DATA/rns/GEWEX.SRB/rns_0.5x0.5.nc'),
                  os.path.join(data_dir,"FLUXCOM/rns.nc"),
                  os.path.join(data_dir,"CLASS/rs.nc")]:
        source = fname.split("/")[-2]
        data[source] = Variable(filename=fname,variable_name="rns",alternate_vars=["rs"]).integrateInTime(mean=True).convert("W m-2")
        data[source].data.mask += r.getMask("global",data[source])
    CreateVariableComparisonArray(data,"RdPu","Net Radiation","rns.png")
    
    data = {}
    for fname in [os.path.join(os.environ['ILAMB_ROOT'],'DATA/le/GBAF/le_0.5x0.5.nc'),
                  os.path.join(data_dir,"FLUXCOM/hfls.nc"),
                  os.path.join(data_dir,"WECANN/hfls.nc"),
                  os.path.join(data_dir,"CLASS/hfls.nc")]:
        source = fname.split("/")[-2]
        data[source] = Variable(filename=fname,variable_name="hfls",alternate_vars=["le"]).integrateInTime(mean=True).convert("W m-2")
    CreateVariableComparisonArray(data,"Oranges","Latent Heat","hfls.png")

    data = {}
    for fname in [os.path.join(os.environ['ILAMB_ROOT'],'DATA/sh/GBAF/sh_0.5x0.5.nc'),
                  os.path.join(data_dir,"FLUXCOM/hfss.nc"),
                  os.path.join(data_dir,"WECANN/hfss.nc"),
                  os.path.join(data_dir,"CLASS/hfss.nc")]:
        source = fname.split("/")[-2]
        data[source] = Variable(filename=fname,variable_name="hfss",alternate_vars=["sh"]).integrateInTime(mean=True).convert("W m-2")
    CreateVariableComparisonArray(data,"Oranges","Sensible Heat","hfss.png")

    data = {}
    for fname in [os.path.join(os.environ['ILAMB_ROOT'],'DATA/gpp/GBAF/gpp_0.5x0.5.nc'),
                  os.path.join(data_dir,"FLUXCOM/gpp.nc"),
                  os.path.join(data_dir,"WECANN/gpp.nc"),
                  os.path.join(data_dir,"Kumar/gpp_0.5x0.5.nc")]:
        source = fname.split("/")[-2]
        data[source] = Variable(filename=fname,variable_name="gpp").integrateInTime(mean=True).convert("g m-2 d-1")
    CreateVariableComparisonArray(data,"Greens","Gross Primary Production","gpp.png")

    data = {}
    for fname in [os.path.join(data_dir,"Wang2021/mrsos_ec.nc"),
                  os.path.join(data_dir,"Wang2021/mrsos_olc.nc")]:
        token = fname.split("/")
        source = token[-2] + token[-1].replace("mrsos_","").replace(".nc","").upper() 
        data[source] = Variable(filename=fname,variable_name="mrsos").integrateInTime(mean=True)
    CreateVariableComparisonArray(data,"Blues","Surface Soil Moisture","mrsos.png")
        
    data = {}
    for fname in [#os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/GEOCARBON/biomass_0.5x0.5.nc'),
                  os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/GLOBAL.CARBON/biomass_0.5x0.5.nc'),
                  os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/NBCD2000/biomass_0.5x0.5.nc'),
                  os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/Thurner/biomass_0.5x0.5.nc'),
                  os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/Tropical/biomass_0.5x0.5.nc'),
                  os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/US.FOREST/biomass_0.5x0.5.nc'),
                  os.path.join(data_dir,"ESACCI/biomass.nc")]:
        token = fname.split("/")
        source = token[-2]
        data[source] = Variable(filename=fname,variable_name="biomass").integrateInTime(mean=True).convert("kg m-2")
    CreateVariableComparisonArray(data,"Greens","Above Ground Biomass","cVeg.png")
    
    data = {}
    data['HWSD'] = Variable(filename=os.path.join(os.environ['ILAMB_ROOT'],'DATA/cSoil/HWSD/soilc_0.5x0.5.nc'),variable_name="cSoilAbove1m") # cSoil
    data['Mishra'] = Variable(filename="Mishra/cSoil.nc",variable_name="cSoil")
    data['NCSCD'] = Variable(filename="NCSCD/cSoil.nc",variable_name="cSoil")
    for source,v in data.items():
        if v.temporal: v = v.integrateInTime(mean=True)
        data[source] = v
    CreateVariableComparisonArray(data,"Purples","Soil Carbon","cSoil.png")
    
    data = {}
    data['NCSCDV22'] = Variable(filename=os.path.join(os.environ['ILAMB_ROOT'],'DATA/cSoil/NCSCDV22/soilc_0.5x0.5.nc'),variable_name="cSoilAbove1m")
    data['Mishra'] = Variable(filename="Mishra/cSoilAbove1m.nc",variable_name="cSoilAbove1m")
    data['NCSCD'] = Variable(filename="NCSCD/cSoilAbove1m.nc",variable_name="cSoilAbove1m")
    for source,v in data.items():
        if v.temporal: v = v.integrateInTime(mean=True)
        data[source] = v
    CreateVariableComparisonArray(data,"Purples","Soil Carbon","cSoilAbove1m.png")
    
    """

    total_to_agb = 0.7
    total_to_carbon = 0.5
    data = {
        'GEOCARBON': os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/GEOCARBON/biomass_0.5x0.5.nc'),
        'Thurner2013':os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/Thurner/biomass_0.5x0.5.nc'),
        'Saatchi2011':os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/Tropical/biomass_0.5x0.5.nc'),
        'USForest': os.path.join(os.environ['ILAMB_ROOT'],'DATA/biomass/US.FOREST/biomass_0.5x0.5.nc'),
        'ESACCI': os.path.join(data_dir,"ESACCI/biomass.nc"),
        'XuSaatchi2021': os.path.join(data_dir,"XuSaatchi/XuSaatchi.nc")
    }    
    for source in data:
        data[source] = Variable(filename=data[source],variable_name="biomass").integrateInTime(mean=True).convert("kg m-2")

    # make needed adjustments
    for key in ['GEOCARBON','USForest','ESACCI']:
        data[key].data *= (total_to_carbon / total_to_agb)
        data[f"{key}*"] = data.pop(key)
        
    CreateVariableComparisonArray(data,"Greens","Total Biomass in Carbon Units","cVeg_adjusted.png")
