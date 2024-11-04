# %%
import pandas as pd
import numpy as np

# %%
crsp3 = pd.read_csv(document_path+'crsp3.csv', low_memory=False, index_col=0)
decme = pd.read_csv(document_path+'decme.csv', low_memory=False, index_col=0)

# %%
# Info as of June
crsp3_jun = crsp3[crsp3['month']==6]

crsp_jun = pd.merge(crsp3_jun, decme, how='inner', on=['permno','year'])
crsp_jun=crsp_jun[['permno','date', 'jdate', 'shrcd','exchcd','retadj','me','wt','cumretx','mebase','lme','dec_me','year']]
crsp_jun=crsp_jun.sort_values(by=['permno','jdate']).drop_duplicates()
crsp_jun

# %%
crsp_jun['decile'] = crsp_jun.groupby('year')['me'].transform(
    lambda x: pd.qcut(x, 10, labels=False, duplicates='drop')
)

# Merge June ME and decile information back into the main dataframe
crsp3 = crsp3.merge(crsp_jun[['permno', 'year', 'decile', 'me']], on=['permno', 'year'], how='left', suffixes=('', '_june'))

# Forward fill June ME and decile data to apply them to the following months until next June
crsp3['me_june'] = crsp3.groupby('permno')['me_june'].ffill()
crsp3['decile'] = crsp3.groupby('permno')['decile'].ffill()

# Calculate monthly value-weighted returns for each decile using June ME
crsp3['weighted_ret'] = crsp3['me_june'] * crsp3['ret']

# Group by date and decile to calculate the sum of weighted returns and total market equity, then compute value-weighted return
monthly_decile_returns = crsp3.groupby(['date', 'decile']).agg(
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

# Optionally, format the values to show percentage changes
pivot_table = pivot_table * 100  # Convert to percentage
pivot_table = pivot_table.round(2)  # Round to two decimal places

# Display the results
print(pivot_table)

# %%
import pyvest as pv
data_reader = pv.FamaFrenchDataReader()
portfolio_sort = "Portfolios_Formed_on_ME"
start_date = "1926-07-01"
end_date = "2022-12-31"
# fama_size = data_reader.read_returns(portfolio_sort, start_date, end_date, weighting="equal")
fama_size = data_reader.read_returns(portfolio_sort, start_date, end_date, weighting="value")

# %%
if 'Date' in pivot_table.index.names:
    pivot_table.reset_index(inplace=True)
    
# Convert 'Date' to integer in both DataFrames
column_names=['Lo 10', '2-Dec', '3-Dec', '4-Dec', '5-Dec', '6-Dec', '7-Dec', '8-Dec', '9-Dec', 'Hi 10']
fama_size.rename(columns=dict(zip(fama_size.columns, column_names)), inplace=True)
pivot_table['Date'] = pivot_table['Date'].astype(int)
fama_size.index = fama_size.index.strftime('%Y%m')
fama_size.index = fama_size.index.astype(int)
fama_size

# %%
merged_df = pd.merge(fama_size, pivot_table,left_index=True,right_on='Date', suffixes=('_fama', '_pivot'))

# Drop rows with any missing values
merged_df.dropna(inplace=True)

# Calculate correlation between corresponding columns
correlation_matrix = merged_df.corr()

# Filter correlation values for desired columns
desired_cols_fama = [col for col in merged_df.columns if col.endswith('_fama')]
desired_cols_pivot = [col for col in merged_df.columns if col.endswith('_pivot')]
correlation_values = correlation_matrix.loc[desired_cols_fama, desired_cols_pivot]
# print(correlation_values)
# Extract diagonal values which represent the correlation of corresponding deciles
diagonal_correlation = correlation_values.values.diagonal()
# print("Correlation between corresponding columns:")
# print(diagonal_correlation)


# %%
crsp_jun.to_csv(document_path+'crsp_jun.csv')
pivot_table.to_csv(document_path+'size portfolio.csv')


