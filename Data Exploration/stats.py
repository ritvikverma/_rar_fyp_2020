import numpy as np
import pandas as pd

station_list = [
    "WHA",
    "HOM",
    "YMT",
    "MOK",
    "PRE",
    "SKM",
    "KOT",
    "LOF",
    "WTS",
    "DIH",
    "CHH",
    "KOB",
    "NTK",
    "KWT",
    "LAT",
    "YAT",
    "TIK"
]

df = pd.read_csv('../SICP/lstm_only_test_result_mar11.csv')
cols = [2, 4, 20, 21, 22]
df = df[df.columns[cols]]
df = df[df["lstm_mae"] >= 60]

# Per weekday per hour

# dataframe = pd.DataFrame(columns=['day_in_week','hour_in_day','mean','st_dev','no_of_entries'])
#
# for day in range(0,7):
#     df1 = df[df["day_in_week"] == day]
#     hours = df1["hour_in_day"].unique().tolist()
#
#     for h in hours:
#         li=df1[df1["hour_in_day"]==h]['lstm_mae'].to_numpy()
#         mean = np.mean(li,axis=0)
#         std = np.std(li,axis=0)
#         dataframe.loc[len(dataframe.index)]=[day,h,mean,std,li.shape[0]]
#
# dataframe.to_csv ('../Data Statistics/stats_perday_perhour.csv', index = False, header=True)
# print(dataframe)
# dataframe.iloc[0:0]
#
# #Per individual date
#
# dataframe = pd.DataFrame(columns=['date','day_in_week','mean','st_dev','no_of_entries'])
#
# df['date'] = df['current_time'].str.slice(0,10)
# dates = df["date"].unique().tolist()
# for date in dates:
#     li = df[df["date"] == date]['lstm_mae'].to_numpy()
#     mean = np.mean(li,axis=0)
#     std = np.std(li,axis=0)
#     daynum=df[df['date']==date]['day_in_week'].values[0]
#     dataframe.loc[len(dataframe.index)] = [date, daynum, mean, std, li.shape[0]]
#
# dataframe.to_csv('../Data Statistics/statsperday.csv', index=False, header=True)
#
# print(dataframe)
# dataframe.iloc[0:0]
# # #per station per hour
#
# dataframe = pd.DataFrame(columns=['station','hour_in_day','mean','st_dev','no_of_entries'])
#
# for sn in station_list:
#     df1 = df[df["Sn"] == sn]
#     hours = df1["hour_in_day"].unique().tolist()
#
#     for h in hours:
#         li=df1[df1["hour_in_day"]==h]['lstm_mae'].to_numpy()
#         mean = np.mean(li,axis=0)
#         std = np.std(li,axis=0)
#         dataframe.loc[len(dataframe.index)]=[sn,h,mean,std,li.shape[0]]
#
# dataframe.to_csv ('../Data Statistics/stats_per_station_per_hour.csv', index = False, header=True)
#
# print(dataframe)
# dataframe.iloc[0:0]

# per station per day per hour
dataframe = pd.DataFrame(columns=['station', 'day_in_week', 'hour_in_day', 'mean', 'st_dev', 'no_of_entries'])

for sn in station_list:
    print(sn)
    df1 = df[df["Sn"] == sn]
    days = df1["day_in_week"].unique().tolist()

    for day in days:
        df2 = df1[df1['day_in_week'] == day]
        hours = df2["hour_in_day"].unique().tolist()
        for h in hours:
            li = df2[df2["hour_in_day"] == h]['lstm_mae'].to_numpy()
            mean = np.mean(li, axis=0)
            std = np.std(li, axis=0)
            dataframe.loc[len(dataframe.index)] = [sn, day, h, mean, std, li.shape[0]]

dataframe.to_csv('../Data Statistics/stats_perst_perday_perhour.csv', index=False, header=True)

print(dataframe)
