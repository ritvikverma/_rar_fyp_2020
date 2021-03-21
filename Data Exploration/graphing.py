import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

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
sns.set(style="darkgrid")

df = df[df["current_time"].str.contains("2019-11-03")]
df = df[df["lstm_mae"] >= 60]

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

x = np.array([station_list.index(element) for element in df['Sn']])
y = df['hour_in_day']
z = df['lstm_mae']

ax.set_xlabel("station")
ax.set_ylabel("hour in day")
ax.set_zlabel("lstm_mae")

ax.scatter(x, y, z)

plt.show()
