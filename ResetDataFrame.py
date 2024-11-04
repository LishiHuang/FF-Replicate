# %%
import pandas as pd
import numpy as np
import datetime as dt
import wrds
#import psycopg2 
import matplotlib.pyplot as plt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from scipy import stats

# %%
crsp3 = pd.read_csv(document_path+'crsp3.csv', low_memory=False, index_col=0)
comp = pd.read_csv(document_path+'comp.csv', low_memory=False, index_col=0)
decme = pd.read_csv(document_path+'decme.csv', low_memory=False, index_col=0)

# %%
# Info as of June
crsp3_jun = crsp3[crsp3['month']==6]

crsp_jun = pd.merge(crsp3_jun, decme, how='inner', on=['permno','year'])
crsp_jun=crsp_jun[['permno','date', 'jdate', 'shrcd','exchcd','retadj','me','wt','cumretx','mebase','lme','dec_me','year','prc','dec_prc']]
crsp_jun=crsp_jun.sort_values(by=['permno','jdate']).drop_duplicates()


# %%
conn=wrds.Connection()

# %%
#######################
# CCM Block           #
#######################
ccm=conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where substr(linktype,1,1)='L'
                  and (linkprim ='C' or linkprim='P')
                  """, date_cols=['linkdt', 'linkenddt'])

# if linkenddt is missing then set to today date
ccm['linkenddt']=ccm['linkenddt'].fillna(pd.to_datetime('today'))
ccm['gvkey']=ccm['gvkey'].astype(int)

ccm1=pd.merge(comp,ccm,how='left',on=['gvkey'])

ccm1['datadate']=pd.to_datetime(ccm1['datadate'])
ccm1['yearend']=ccm1['datadate']+YearEnd(0)
ccm1['jdate']=ccm1['yearend']+MonthEnd(6)

# %%
# set link date bounds
ccm2=ccm1[(ccm1['jdate']>=ccm1['linkdt'])&(ccm1['jdate']<=ccm1['linkenddt'])]

# link comp and crsp
crsp_jun['jdate']=pd.to_datetime(crsp_jun['jdate'])
ccm_jun=pd.merge(crsp_jun, ccm2, how='inner', on=['permno', 'jdate'])
ccm_jun['beme']=ccm_jun['be']*1000/ccm_jun['dec_me']

merged_df = pd.merge(ccm_jun,crsp_jun,how='left',on=['permno','jdate'],suffixes=('', '_right'))
columns_to_keep = [col for col in merged_df.columns if not col.endswith('_right')]
ccm2_jun = merged_df[columns_to_keep]
ccm2_jun.to_csv(document_path+'ccm.csv')
# %%
