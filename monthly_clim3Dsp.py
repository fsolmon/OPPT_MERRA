import os
import re
import sys
import glob
#from pylab import *
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from scipy import stats
import numpy as np
import cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import xarray as xr
import datetime
#import xesmf as xe
from dateutil.relativedelta import relativedelta
from dask.distributed import Client,LocalCluster

# this is to create the dask local cluster
# on clmip run with 32 proc = workers 
# define the xarray / dask chunksize accordingly : you want 32 chunks   
# put enough memory per worker ...


#from dask.distributed import Client,LocalCluster
#cluster = LocalCluster(n_workers=64, threads_per_worker=1,memory_limit='64GB')
#client = Client(cluster)




def main():

 parallel = 1 

 if(parallel ==1):  
   cluster = LocalCluster(n_workers=9,threads_per_worker=1,memory_limit='100GB')
   client = Client(cluster)


 #match xarray chuksize to cluster config
 #chunksize = {'time':1,'lev':9}  # this makes 8/1 * 72/9 = 64 chunks 
 chunksize = {'lev':8}

 # calculate run tme for monitoring script computing time 
 begin_time = datetime.datetime.now()


 # wave band 
 wab = 19 
 # date 
# define how many files to process and dates
 start_date = datetime.date(1980, 1, 15)
 end_date = datetime.date(2021, 1, 15)
 date_list = []
 cur_date=start_date
 month = 0
 while (cur_date < end_date) :
     cur_date = (start_date + relativedelta(months=month))
     a_date = cur_date.strftime('%Y%m')
     date_list.append(a_date)
     month=month+1


# define aerosol species list and some related  dict for  conveniency 


 aertype = {'BC': ['BCPHILIC', 'BCPHOBIC'],\
           'DU': ['DU001', 'DU002', 'DU003', 'DU004', 'DU005'],\
           'OC': ['OCPHILIC', 'OCPHOBIC'],\
           'SU': ['SO4'],\
           'SS': ['SS001', 'SS002', 'SS003', 'SS004', 'SS005']}

#  start looping on mixing ratio files 

 for d in date_list: 
   file  =  "dailysp/MERRA2_OPPDAY_wb%s.%s*.nc"%(wab,d)
   try :
     DS = xr.open_mfdataset(file, parallel = True,chunks = chunksize,combine ='nested', concat_dim = 'day' ).astype('float32').rename({'EXTOT':'EXTTOT'})
   except :
     print('cant open %s skip... '%file)
     continue
#calculate total SSA 
   DS['SSATOT'] = DS['SSABC'] * 0.
   for atyp in aertype.keys():
     DS['SSATOT'] =  DS['SSATOT']  + DS['EXT%s'%atyp]/DS['EXTTOT']* DS['SSA%s'%atyp ]

# calculate  total g
   DS['GTOT'] = DS['GBC']*0.
   for atyp in aertype.keys():
     DS['GTOT'] =  DS['GTOT'] + ( DS['EXT%s'%atyp] * DS['SSA%s'%atyp] ) \
                              /(DS['EXTTOT'] * DS['SSATOT']) * DS['G%s'%atyp]

# assignn attributes 
   DSM = DS.mean('day')
   DSM['AIRDENS'] =  DSM['AIRDENS'].assign_attrs({'units' : 'Kg.M-3', 'long_name' : 'Air Density'} )
   DSM['PS'] = DSM['PS'].assign_attrs({'units' : 'Pa', 'long_name' : 'Surface Pressure'} )
   DSM['DELP']=DSM['DELP'].assign_attrs({'units' : 'Pa', 'long_name' : 'Layer Thickness'} )
   for typ in ['BC','OC','SU','DU','SS','TOT']:
      DSM['EXT%s'%typ]=DSM['EXT%s'%typ].assign_attrs({'units' : 'M-1', 'long_name' : 'Layer %s Aerosol Extinction'%typ} )
      DSM['SSA%s'%typ]=DSM['SSA%s'%typ].assign_attrs({'units' : 'none', 'long_name' : 'Layer %s Aerosol Single Scattering Albedo'%typ} )
      DSM['G%s'%typ]=DSM['G%s'%typ].assign_attrs({'units' : 'none', 'long_name' : 'Layer %s Aerosol Assimetry Parameter'%typ} )

   DSM['AOD']=DSM['AOD'].assign_attrs({'units' : 'none', 'long_name' : ' Total Aerosol OPtical Depth'} )
   DSM = DSM.assign_attrs({'File_Info':'Aerosol Optical Properties (at wl) computed from MERRA2 reanalysis 3 hourly aerosol mixing ratio, species level optical properties (NASA/GOCART) and including humidy effect ',   'Contact' : 'fabien.solmon@aero.obs-mip.fr'})

   DSM.to_netcdf("./monthlysp2/wb%s/MERRA2_OPPMONTH_wb%s.%s.nc"%(wab,wab,d))
   print("monthlysp2/wb%s/MERRA2_OPPMONTH_wb%s.%s.nc processed "%(wab,wab,d))

#
   DS.close()

 if (parallel ==1) :
   cluster.close()
   client.close()
 print(datetime.datetime.now() - begin_time)



if __name__ == "__main__":
    # execute only if run as a script
    main()


