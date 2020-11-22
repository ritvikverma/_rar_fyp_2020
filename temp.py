import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

data = pd.read_csv('/Users/adwithyamagow/Desktop/daily/02Nov19.csv')

track_data = data.loc[(data['station'] == 'MOK') & (data['track'] == '38')]

plt.clf()
plt.xlim(0, len(track_data))
plt.plot(range(len(track_data)), track_data['act_occupied_time'], 'bo', markersize=2)
plt.hlines(track_data['act_occupied_time'].quantile(0.99), 0, 80000, colors='black')
plt.hlines(track_data['act_occupied_time'].quantile(0.01), 0, 80000, colors='black')
plt.ylabel('Actual Occupied Time')
plt.title('Actual Occupied Time of Trains for MOK track 38 on 02 Nov 2019')

outliers = track_data.loc[(track_data['act_occupied_time'] >= track_data['act_occupied_time'].quantile(0.99)) | (track_data['act_occupied_time'] <= track_data['act_occupied_time'].quantile(0.01))]


track_data = data.loc[data['station'] == 'MOK']

plt.figure()
sns.boxplot(x='track',y='act_occupied_time' , data=track_data)
plt.title('Boxplot of Actual Occupied Time of Trains VS. Track MOK on 02 Nov 2019')

track_data = data.loc[data['train'].str.match('..06')].sort_values('act_arr_time')
outliers2 = track_data.loc[(track_data['act_occupied_time'] >= track_data['act_occupied_time'].quantile(0.99))].sort_values('act_arr_time') 

track_data = data.loc[(data['station'] == 'MOK') & (data['track'] == '35')]
plt.figure()
plt.xlim(0, len(track_data))
plt.plot(range(len(track_data)), track_data['act_occupied_time'], 'bo', markersize=2)
plt.hlines(track_data['act_occupied_time'].quantile(0.99), 0, 80000, colors='black')
plt.hlines(track_data['act_occupied_time'].quantile(0.01), 0, 80000, colors='black')
plt.ylabel('Actual Occupied Time')
plt.title('Actual Occupied Time of Trains for MOK track 35 on 02 Nov 2019')

outlier = track_data.loc[track_data['act_occupied_time'] > 250]

