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
crsp_jun=crsp_jun[['permno','date', 'jdate', 'shrcd','exchcd','retadj','me','wt','cumretx','mebase','lme','dec_me','year']]
crsp_jun=crsp_jun.sort_values(by=['permno','jdate']).drop_duplicates()
crsp_jun

# %%
# Info as of June
crsp3_jun = crsp3[crsp3['month']==6]

crsp_jun = pd.merge(crsp3_jun, decme, how='inner', on=['permno','year'])
crsp_jun=crsp_jun[['permno','date', 'jdate', 'shrcd','exchcd','retadj','me','wt','cumretx','mebase','lme','dec_me','year']]
crsp_jun=crsp_jun.sort_values(by=['permno','jdate']).drop_duplicates()
crsp_jun

# %%
try:
    conn = wrds.Connection(warn_on_error=False)
except:
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

ccm1=pd.merge(comp[['gvkey','datadate','be', 'count','act','ppei','invt','op']],ccm,how='left',on=['gvkey'])

ccm1['datadate']=pd.to_datetime(ccm1['datadate'])
ccm1['yearend']=ccm1['datadate']+YearEnd(0)
ccm1['jdate']=ccm1['yearend']+MonthEnd(6)
ccm1

# %%
# set link date bounds
ccm2=ccm1[(ccm1['jdate']>=ccm1['linkdt'])&(ccm1['jdate']<=ccm1['linkenddt'])]
ccm2=ccm2[['gvkey','permno','datadate','yearend', 'jdate','be', 'count','act','ppei','invt','op']]

# link comp and crsp
crsp_jun['jdate']=pd.to_datetime(crsp_jun['jdate'])
ccm_jun=pd.merge(crsp_jun, ccm2, how='inner', on=['permno', 'jdate'])
ccm_jun['beme']=ccm_jun['be']*1000/ccm_jun['dec_me']

merged_df = pd.merge(ccm_jun,crsp_jun,how='left',on=['permno','jdate'],suffixes=('', '_right'))
columns_to_keep = [col for col in merged_df.columns if not col.endswith('_right')]
ccm2_jun = merged_df[columns_to_keep]
ccm2_jun

# %% [markdown]
# ### total assets

# %%
if pindex == 'invt':
    ccm2_jun = ccm2_jun[ccm2_jun[pindex] != 0]

# %%
ccm3_jun= ccm2_jun.copy()
ccm3_jun = ccm3_jun.dropna(subset=[pindex])

for year, group in ccm3_jun.groupby('year'):
    if year == 2022:
        print(f"\nYear: {year}")
        bins,edges=pd.qcut(group[pindex], 10, labels=False, duplicates='raise',retbins=True)
        # print(f"Decile bins: {edges}")
        # print(f"Decile bins: {bins.value_counts().sort_index()}")
        
ccm3_jun['decile'] = ccm3_jun.groupby('year')[pindex].transform(
    lambda x: pd.qcut(x, 10, labels=False, duplicates='drop')
)


# %%
# Merge June ME and decile information back into the main dataframe
all_merged = crsp3.merge(ccm3_jun[['permno', 'year', 'decile', 'me', pindex]], on=['permno', 'year'], how='left', suffixes=('', '_june'))

# Forward fill June ME and decile data to apply them to the following months until next June
all_merged['me_june'] = all_merged.groupby('permno')['me_june'].ffill()
all_merged['decile'] = all_merged.groupby('permno')['decile'].ffill()

# Calculate monthly value-weighted returns for each decile using June ME
all_merged['weighted_ret'] = all_merged['me_june'] * all_merged['ret']

# Group by date and decile to calculate the sum of weighted returns and total market equity, then compute value-weighted return
monthly_decile_returns = all_merged.groupby(['date', 'decile']).agg(
    total_weighted_return=('weighted_ret', 'sum'),
    total_me_june=('me_june', 'sum')
).reset_index()
monthly_decile_returns['value_weighted_return'] = monthly_decile_returns['total_weighted_return'] / monthly_decile_returns['total_me_june']

# %%
#Convert date to a year-month format for better presentation
monthly_decile_returns['date'] = pd.to_datetime(monthly_decile_returns['date'])
monthly_decile_returns['Date'] = monthly_decile_returns['date'].dt.strftime('%Y%m')

# Pivot the table to match the desired format
pivot_table = monthly_decile_returns.pivot(
    index='Date',
    columns='decile',
    values='value_weighted_return'
)

# Rename the columns to match financial reporting standards
pivot_table.columns = ['Lo 10', '2-Dec', '3-Dec', '4-Dec', '5-Dec', '6-Dec', '7-Dec', '8-Dec', '9-Dec', 'Hi 10']
pindex = pindex.capitalize()
pivot_table.columns = pivot_table.columns.str.replace('Dec', pindex)

# Optionally, format the values to show percentage changes
pivot_table = pivot_table * 100  # Convert to percentage
pivot_table = pivot_table.round(2)  # Round to two decimal places

# Display the results
pivot_table.to_csv(document_path+f'{pindex} portfolio.csv')