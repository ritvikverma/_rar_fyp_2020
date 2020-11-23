import numpy as np
import pandas as pd
import dateutil


train_number = "01"
quantile = .95
start_date = pd.to_datetime("01/11/2019 8:48:00 AM", format="%d/%m/%Y %I:%M:%S %p")
end_date = pd.to_datetime("01/11/2019  8:58:00 AM", format="%d/%m/%Y %I:%M:%S %p")

dataframe = pd.read_csv('SICP/daily/01Nov19.csv')

dataframe['act_arr_time'] = pd.to_datetime(dataframe['act_arr_time'] , format="%Y-%m-%d %H:%M:%S.%f").astype('datetime64[s]')
mask = (dataframe['act_arr_time'] > start_date) & (dataframe['act_arr_time'] <= end_date)

quantile_mask = (dataframe['act_travelling_time'] > dataframe['act_travelling_time'].quantile(quantile)) | (dataframe['act_occupied_time'] > dataframe['act_occupied_time'].quantile(quantile))

query = dataframe[dataframe['train'].str.match('..' + train_number) & mask & quantile_mask].sort_values(['act_arr_time'])
query.reset_index(drop=True, inplace=True)

