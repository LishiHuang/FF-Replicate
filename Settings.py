# %%
##########################################
# Fama French 3 Factors                  #
# Qingyi (Freda) Song Drechsler          #
# Date: April 2018                       #
# Updated: June 2020                     #
##########################################

import pandas as pd
import numpy as np
import datetime as dt
import wrds
#import psycopg2 
import matplotlib.pyplot as plt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from scipy import stats
import psycopg2

# %% [markdown]
# # Connect to WRDS #

# %%
try:
    conn = wrds.Connection()  # create a WRDS connection
except:
    print("Fail to connect to WRDS.")

# %% [markdown]
# # Compustat fundermental #

# %%
#  data of fundermental 
# https://wrds-www.wharton.upenn.edu/pages/get-data/center-research-security-prices-crsp/annual-update/crspcompustat-merged/security-monthly/
if original_index=='N':
    comp = conn.raw_sql("""
                        select gvkey, datadate, at, pstkl, txditc, act, bkvlps
                        pstkrv, seq, pstk,ppegt,invt,revt,cogs,xsga, xint
                        from comp.funda
                        where indfmt='INDL' 
                        and datafmt='STD'
                        and popsrc='D'
                        and consol='C'
                        and datadate >= '{}'
                        """.format(start_date),date_cols=['datadate'])
else:
    comp = conn.raw_sql("""
                        select gvkey, datadate, at, pstkl, txditc, act, bkvlps
                        pstkrv, seq, pstk,ppegt,invt,revt,cogs,xsga, xint, {}
                        from comp.funda
                        where indfmt='INDL' 
                        and datafmt='STD'
                        and popsrc='D'
                        and consol='C'
                        and datadate >= '{}'
                        """.format(original_index,start_date), date_cols=['datadate'])

comp['year']=comp['datadate'].dt.year

# %%
# create preferrerd stock
comp['ps']=np.where(comp['pstkrv'].isnull(), comp['pstkl'], comp['pstkrv'])
comp['ps']=np.where(comp['ps'].isnull(),comp['pstk'], comp['ps'])
comp['ps']=np.where(comp['ps'].isnull(),0,comp['ps'])
comp['txditc']=comp['txditc'].fillna(0)
comp

# %%
# create book equity
comp['be']=comp['seq']+comp['txditc']-comp['ps']
comp['be']=np.where(comp['be']>0, comp['be'], np.nan)
comp

# %%
# number of years in Compustat
comp=comp.sort_values(by=['gvkey','datadate'])
comp['count']=comp.groupby(['gvkey']).cumcount()
comp['ppei']=(comp['ppegt']-comp['ppegt'].shift(1))/comp['ppegt'].shift(1)
comp['op']=(comp['revt']-comp['cogs']-comp['xsga']-comp['xint'])/comp['be'].shift(1)
comp

# %% [markdown]
# # Marktet Stock File #

# %%
###################
# CRSP Block      #
###################
# sql similar to crspmerge macro
crsp_m = conn.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                      a.ret, a.retx, a.shrout, a.prc
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '{}' and '{}'
                      and b.exchcd between 1 and 3
                      """.format(start_date,end_date),date_cols=['date']) 

# change variable format to int
crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)

# Line up date to be end of month
crsp_m['jdate']=crsp_m['date']+MonthEnd(0)

# %% [markdown]
# ## retadj, me ##

# %%
# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """, date_cols=['dlstdt'])

dlret.permno=dlret.permno.astype(int)
#dlret['dlstdt']=pd.to_datetime(dlret['dlstdt'])
dlret['jdate']=dlret['dlstdt']+MonthEnd(0)

crsp = pd.merge(crsp_m, dlret, how='left',on=['permno','jdate'])
crsp['dlret']=crsp['dlret'].fillna(0)
crsp['ret']=crsp['ret'].fillna(0)

# retadj factors in the delisting returns
crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1

# calculate market equity
crsp['me']=crsp['prc'].abs()*crsp['shrout'] 
crsp=crsp.drop(['dlret','dlstdt','shrout'], axis=1)
crsp=crsp.sort_values(by=['jdate','permco','me'])


# %% [markdown]
# ## log return ##

# %%
# Calculate log returns
crsp['log_ret'] = np.log(1 + pd.to_numeric(crsp['ret'], errors='coerce'))
crsp

# %% [markdown]
# ## Aggregate Market Cap ##

# %%

# sum of me across different permno belonging to same permco a given date
crsp_summe = crsp.groupby(['jdate','permco'])['me'].sum().reset_index()

# largest mktcap within a permco/date
crsp_maxme = crsp.groupby(['jdate','permco'])['me'].max().reset_index()

# join by jdate/maxme to find the permno
crsp1=pd.merge(crsp, crsp_maxme, how='inner', on=['jdate','permco','me'])

# drop me column and replace with the sum me
crsp1=crsp1.drop(['me'], axis=1)

# join with sum of me to get the correct market cap info
crsp2=pd.merge(crsp1, crsp_summe, how='inner', on=['jdate','permco'])

# sort by permno and date and also drop duplicates
crsp2=crsp2.sort_values(by=['permno','jdate']).drop_duplicates()
crsp2 = crsp.copy()

# %% [markdown]
# ## keep market cap at Dec

# %%
# keep December market cap
crsp2['year']=crsp2['jdate'].dt.year
crsp2['month']=crsp2['jdate'].dt.month
decme=crsp2[crsp2['month']==12]
decme=decme[['permno','date','jdate','me','year','prc']].rename(columns={'me':'dec_me','prc':'dec_prc'})

### July to June dates
crsp2['ffdate']=crsp2['jdate']+MonthEnd(-6)
crsp2['ffyear']=crsp2['ffdate'].dt.year
crsp2['ffmonth']=crsp2['ffdate'].dt.month
crsp2['1+retx']=1+crsp2['retx']
crsp2=crsp2.sort_values(by=['permno','date'])
crsp2


# %% [markdown]
# ## cumulative return, lag value

# %%
# cumret by stock
crsp2['cumretx']=crsp2.groupby(['permno','ffyear'])['1+retx'].cumprod()

# lag cumret
crsp2['lcumretx']=crsp2.groupby(['permno'])['cumretx'].shift(1)

# lag market cap
crsp2['lme']=crsp2.groupby(['permno'])['me'].shift(1)

# if first permno then use me/(1+retx) to replace the missing value
crsp2['count']=crsp2.groupby(['permno']).cumcount()
crsp2['lme']=np.where(crsp2['count']==0, crsp2['me']/crsp2['1+retx'], crsp2['lme'])

# baseline me
mebase=crsp2[crsp2['ffmonth']==1][['permno','ffyear', 'lme']].rename(columns={'lme':'mebase'})

# merge result back together
crsp3=pd.merge(crsp2, mebase, how='left', on=['permno','ffyear'])
crsp3['wt']=np.where(crsp3['ffmonth']==1, crsp3['lme'], crsp3['mebase']*crsp3['lcumretx'])

decme['year']=decme['year']+1
decme=decme[['permno','year','dec_me','dec_prc']]

crsp3.to_csv(document_path+'crsp3.csv')
print('crsp saved.')


# %%
decme.to_csv(document_path+'decme.csv')
comp.to_csv(document_path+'comp.csv')
print('decme and comp saved.')


