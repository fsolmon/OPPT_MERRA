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

from dask.distributed import Client,LocalCluster

# this is to create the dask local cluster
# on clmip run with 32 proc = workers 
# define the xarray / dask chunksize accordingly : you want 32 chunks   
# put enough memory per worker ...


#from dask.distributed import Client,LocalCluster
#cluster = LocalCluster(n_workers=64, threads_per_worker=1,memory_limit='64GB')
#client = Client(cluster)




# this function returns a 4D array of optical properties matching the 4D humidity field
def bext_hum (opp, wavb, DS, RHB,rus,p = 'hl') :
 

 if(opp =='ext'):
   if p == 'hb' :
       EXTCS = DS.bext.isel(radius=rus).sel(rh =DS.rh[0].values , wl = wavb,  method = 'nearest').astype('float32')  
   else :  
       EXTCS =  xr.where(RHB == 1,  DS.bext.isel(radius=rus).sel(rh =DS.rh[0].values ,wl = wavb,  method ='nearest')  ,0).astype('float32')
       for i in np.arange(2,37,1) : 
         EXTCS = EXTCS + xr.where(RHB == i,  DS.bext.isel(radius=rus).sel(rh =DS.rh[i-1].values ,wl = wavb, method ='nearest'), 0).astype('float32')    
   return EXTCS
 elif( opp =='ssa'): 
   if p == 'hb' :
       SSACS = (DS.bsca/DS.bext).isel(radius=rus).sel(rh =DS.rh[0].values ,wl= wavb,  method = 'nearest').astype('float32')
   else :
       SSACS =  xr.where(RHB == 1,  (DS.bsca/DS.bext).isel(radius=rus).sel(rh =DS.rh[0].values ,wl = wavb, method ='nearest'),0).astype('float32')
       for i in np.arange(2,37,1) :
         SSACS = SSACS + xr.where(RHB == i,  (DS.bsca/DS.bext).isel(radius=rus).sel(rh =DS.rh[i-1].values ,wl = wavb, method ='nearest') ,0).astype('float32')
   return SSACS
 elif (opp == 'g') : 
   if p == 'hb' :
       GFACS = DS.g.isel(radius=rus).sel(rh =DS.rh[0].values , wl = wavb,  method = 'nearest').astype('float32')
   else :
       GFACS =  xr.where(RHB == 1,  DS.g.isel(radius=rus).sel(rh =DS.rh[0].values , wl = wavb, method ='nearest'),0).astype('float32') 
       for i in np.arange(2,37,1) :
         GFACS = GFACS + xr.where(RHB == i,  DS.g.isel(radius=rus).sel(rh =DS.rh[i-1].values , wl = wavb, method ='nearest'),0).astype('float32')
   return GFACS



def main():

 parallel = 1 

 if(parallel ==1):  
   cluster = LocalCluster(n_workers=18,threads_per_worker=1,memory_limit='100GB')
   client = Client(cluster)


 #match xarray chuksize to cluster config
 #chunksize = {'time':1,'lev':9}  # this makes 8/1 * 72/9 = 64 chunks 
 chunksize = {'time':4,'lev':8}

 # define how many files to process and dates
 start_date = datetime.date(2019,8,1)
 end_date = datetime.date(2021, 1, 1)
 date_list = []
 cur_date=start_date
 day = 0
 while (cur_date < end_date) :
     cur_date = (start_date + datetime.timedelta(days = day))
     a_date = cur_date.strftime('%Y%m%d')
     date_list.append(a_date)
     day=day+1

# not used unless problems 
 if(False) :
  date_list=[]
  f= open('daymissing.txt','r')
  lines = f.readlines()
  for l in lines :
    a = l.replace("MERRA2_OPPDAY_wb10.","")
    a=a.replace(".nc","")
    a=a.strip("\n")
    date_list.append(a)
 
 # calculate run tme for monitoring script computing time 
 begin_time = datetime.datetime.now()


 #FRIRST : open open  files and rename lambada to avoid conflict with built in python func.
 DSOPTBC = xr.open_mfdataset('opticsBands_BC.v1_5.RRTMG.nc').astype('float32').rename_dims({'lambda': 'wl'}).rename({'lambda': 'wl'})
 DSOPTOC = xr.open_mfdataset('opticsBands_OC.v1_5.RRTMG.nc').astype('float32').rename_dims({'lambda': 'wl'}).rename({'lambda': 'wl'})
 DSOPTSS = xr.open_mfdataset('opticsBands_SS.v3_5.RRTMG.nc').astype('float32').rename_dims({'lambda': 'wl'}).rename({'lambda': 'wl'})
 DSOPTSU = xr.open_mfdataset('opticsBands_SU.v2_5.RRTMG.nc').astype('float32').rename_dims({'lambda': 'wl'}).rename({'lambda': 'wl'})
 DSOPTDU = xr.open_mfdataset('opticsBands_DU.v15_5.RRTMG.nc').astype('float32').rename_dims({'lambda': 'wl'}).rename({'lambda': 'wl'})

# define aerosol species list and some related  dict for  conveniency 

 species  =  ['BCPHILIC', 'BCPHOBIC', 'DU001', 'DU002', 'DU003', 'DU004', 'DU005', 'OCPHILIC', 'OCPHOBIC', 'SO4', 'SS001', 'SS002', 'SS003', 'SS004', 'SS005']

 aertype = {'BC': ['BCPHILIC', 'BCPHOBIC'],\
           'DU': ['DU001', 'DU002', 'DU003', 'DU004', 'DU005'],\
           'OC': ['OCPHILIC', 'OCPHOBIC'],\
           'SU': ['SO4'],\
           'SS': ['SS001', 'SS002', 'SS003', 'SS004', 'SS005']}

 opp  =  {'BCPHILIC': DSOPTBC, 'BCPHOBIC':DSOPTBC, 'DU001': DSOPTDU , 'DU002':DSOPTDU , 'DU003':DSOPTDU , 'DU004':DSOPTDU , 'DU005':DSOPTDU , 'OCPHILIC':DSOPTOC , 'OCPHOBIC':DSOPTOC, 'SO4':DSOPTSU , 'SS001': DSOPTSS , 'SS002': DSOPTSS , 'SS003': DSOPTSS , 'SS004': DSOPTSS , 'SS005': DSOPTSS }

 iradius = {'BCPHILIC': 1, 'BCPHOBIC':0, 'DU001': 0 , 'DU002':1 , 'DU003':2 , 'DU004':3 , 'DU005':4 , 'OCPHILIC':1 , 'OCPHOBIC':0, 'SO4':0 , 'SS001': 0 , 'SS002': 1 , 'SS003': 2 , 'SS004': 3 , 'SS005': 4 } # refer to radius ndex in OPP files	

 phi= {'BCPHILIC': 'hl', 'BCPHOBIC':'hb', 'DU001': 'hb' , 'DU002':'hb' , 'DU003':'hb' , 'DU004':'hb' , 'DU005':'hb' , 'OCPHILIC':'hl' , 'OCPHOBIC':'hb', 'SO4':'hl' , 'SS001': 'hl' , 'SS002': 'hl' , 'SS003': 'hl' , 'SS004': 'hl', 'SS005': 'hl' } # refer to radius ndex in OPP files    


 #define humidity bins used later on:  same bins for all species in OPP files
 rhh = DSOPTBC.rh.values
 for i in np.arange(0,rhh.size)-1 :
     rhh[i] = DSOPTBC.rh[i].values + 0.5*(DSOPTBC.rh[i+1]-  DSOPTBC.rh[i]).values
 rhh[35] = 1.1
 hbins = np.concatenate(([0], rhh))


#  start looping on mixing ratio files 
 wavb= 19 # RRTMG wave band ( 10 = visible)

 for d in date_list: 
   file = './raw/MERRA2_*0*.inst3_3d_aer_Nv.%s.nc4'%d
   try :
     DS = xr.open_mfdataset(file, parallel = True,chunks = chunksize ).astype('float32')
   except :
     print('cant open %s skip... '%file)
     continue

   # DSOUT will be written to netcdf
   DSOUT =DS.drop(list(DS.keys()))
   DSOUT['zero'] = DS['AIRDENS'] * 0.
   DSOUT['PS'] = DS['PS']
   DSOUT['DELP'] = DS['DELP']
   DSOUT['AIRDENS'] = DS['AIRDENS']
 # create 4D RHB containing index of humidity bins imatching the humidity field 
   RHB = xr.apply_ufunc(np.digitize, DS.RH, hbins ,dask='allowed')

 # now calculate extinction ( in m-1)
   for atyp in aertype.keys() :
      DSOUT['EXT%s'%atyp] = DSOUT['zero']
      DSOUT['SSA%s'%atyp] = DSOUT['zero']
      DSOUT['G%s'%atyp] = DSOUT['zero']

      for sp in aertype[atyp]:
        DSOUT['EXT%s'%sp] = DS[sp]*bext_hum ('ext',wavb, opp[sp], RHB, rus = iradius[sp],  p= phi[sp] )* DS.AIRDENS
        DSOUT['EXT%s'%atyp] = DSOUT['EXT%s'%atyp] +  DSOUT['EXT%s'%sp]

 # calculate species and total ssa
      for sp in aertype[atyp]:
        DSOUT['SSA%s'%sp] =  bext_hum ('ssa',wavb, opp[sp], RHB, rus = iradius[sp],  p= phi[sp] )
        DSOUT['SSA%s'%atyp] =  DSOUT['SSA%s'%atyp]  + DSOUT['EXT%s'%sp]/DSOUT['EXT%s'%atyp]* DSOUT['SSA%s'%sp]

 # calculate species and total g
      for sp in aertype[atyp]:
        WRK =  bext_hum ('g',wavb,opp[sp], RHB, rus = iradius[sp],  p= phi[sp] )     
        DSOUT['G%s'%atyp] =  DSOUT['G%s'%atyp] + ( DSOUT['EXT%s'%sp] * DSOUT['SSA%s'%sp] ) \
                              /(DSOUT['EXT%s'%atyp] * DSOUT['SSA%s'%atyp]) * WRK


 # total AOD
   DSOUT['EXTOT'] = DSOUT['zero']
   DSOUT['AOD'] = DSOUT['zero'].isel(lev = 1)
   for atyp in aertype.keys():
      DSOUT['EXTOT'] = DSOUT['EXTOT'] + DSOUT['EXT%s'%atyp]
      DSOUT['AOD'] = DSOUT['AOD'] + (DSOUT['EXT%s'%atyp] * DS.DELP / (DS.AIRDENS * 9.81 )).sum('lev')


# perform the daily average and output to netcdf 

   for sp in species: 
      DSOUT = DSOUT.drop('EXT%s'%sp).drop('SSA%s'%sp)
   
   DSOUT =  DSOUT.drop_vars('rh').drop_vars('radius').drop('zero')

   DSOUT.mean('time').to_netcdf("./dailysp/MERRA2_OPPDAY_wb%s.%s.nc"%(wavb,d))
   print("MERRA2_OPPDAY_wb%s.%s.nc processed"%(wavb,d))

#
   DS.close()
   DSOUT.close()

 if (parallel ==1) :
   cluster.close()
   client.close()
 print(datetime.datetime.now() - begin_time)



if __name__ == "__main__":
    # execute only if run as a script
    main()


