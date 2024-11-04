# %%
import pandas as pd
import numpy as np

# %%
crsp3 = pd.read_csv(document_path+'crsp3.csv', low_memory=False, index_col=0)
previous=crsp3.columns

# %%
# Shift the columns to get values from previous periods
crsp3['prc_t-13'] = crsp3.groupby('permno')['prc'].shift(13)
crsp3['ret_t-2'] = crsp3.groupby('permno')['ret'].shift(2)
crsp3['me_t-1'] = crsp3.groupby('permno')['me'].shift(1)

# Create a mask where any of the required values is NaN
condition = crsp3['prc_t-13'].isna() | crsp3['ret_t-2'].isna() | crsp3['me_t-1'].isna()

# Shift log returns to start from t-12
crsp3['log_ret_shifted'] = crsp3.groupby('permno')['log_ret'].shift(2)

# Calculate cumulative returns only if all conditions are met, else assign NaN
# Calculate cumulative log returns from t-12 to t-2 and ensure a minimum of eight monthly returns over the past 11 months
crsp3['cumulative_log_ret'] = crsp3['log_ret_shifted'].where(~condition).groupby(crsp3['permno']).rolling(window=11, min_periods=8).sum().reset_index(level=0, drop=True)
crsp3 = crsp3.dropna(subset=['cumulative_log_ret'])

# Calculate Decile Breakpoints and Assign Stocks to Deciles using the filtered data
crsp3['decile'] = crsp3.groupby('date')['cumulative_log_ret'].transform(lambda x: pd.qcut(x, 10, labels=range(1, 11)))

# %%
# Calculate value-weighted returns for each decile
crsp3['value_weighted_ret'] = crsp3['ret'] * crsp3['me_t-1']
crsp3 = crsp3.dropna(subset=['value_weighted_ret'])
decile_returns = crsp3.groupby(['date', 'decile'], observed=True).apply(
    lambda x: (x['value_weighted_ret'].sum()) / x['me_t-1'].sum()
)

decile_returns = decile_returns.reset_index(name='decile_return')

decile_returns['decile'] = decile_returns['decile'].map({
    1: 'Lo PRIOR', 2: 'PRIOR 2', 3: 'PRIOR 3', 4: 'PRIOR 4', 5: 'PRIOR 5',
    6: 'PRIOR 6', 7: 'PRIOR 7', 8: 'PRIOR 8', 9: 'PRIOR 9', 10: 'Hi PRIOR'
})
decile_returns['date'] = pd.to_datetime(decile_returns['date'])
decile_returns['date'] = decile_returns['date'].dt.strftime('%Y%m')

# Pivot the DataFrame to get the desired format
results = decile_returns.pivot(index='date', columns='decile', values='decile_return')
results = results * 100
results = results.round(2)

# %%
column_names = ['Lo PRIOR', 'PRIOR 2', 'PRIOR 3', 'PRIOR 4', 'PRIOR 5', 
                'PRIOR 6', 'PRIOR 7', 'PRIOR 8', 'PRIOR 9', 'Hi PRIOR']
import pyvest as pv
data_reader = pv.FamaFrenchDataReader()
portfolio_sort = "10_Portfolios_Prior_12_2"
start_date = "1964-01-31"
end_date = "2022-12-31"
# fama_momentum = data_reader.read_returns(portfolio_sort, start_date, end_date, weighting="equal")
fama_momentum = data_reader.read_returns(portfolio_sort, start_date, end_date, weighting="value")
# fama = pd.read_csv('10_Portfolios_Prior.csv', skiprows=11, nrows=1164,names=column_names)

# %%
fama_momentum.rename(columns=dict(zip(fama_momentum.columns, column_names)), inplace=True)

fama_momentum.index = fama_momentum.index.strftime('%Y%m')
# fama_momentum.index = fama_momentum.index.astype(int)
# Merge the two dataframes based on 'date' column
merged_momentum = pd.merge(fama_momentum, results, left_index=True, right_on='date', suffixes=('_fama', '_estimation'))
merged_momentum.dropna(inplace=True)

# Calculate correlation between corresponding columns
correlation_matrix = merged_momentum.corr()

# Filter correlation values for desired columns
correlation_values = correlation_matrix.loc['Lo PRIOR_fama':'Hi PRIOR_fama', 'Lo PRIOR_estimation':'Hi PRIOR_estimation']

# print(correlation_values)

diagonal_correlation = correlation_values.values.diagonal()
# print("Correlation between corresponding columns:")
# print(diagonal_correlation)

# %%
p=previous.to_list()
p.append('cumulative_log_ret')

# %%
results.to_csv(document_path+'momentum portfolio.csv')
crsp3_with_momentum =crsp3[p]
crsp3_with_momentum.to_csv(document_path+'crsp3_with_momentum.csv')


