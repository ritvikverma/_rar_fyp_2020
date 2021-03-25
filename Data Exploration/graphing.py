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

# df = df[df["current_time"].str.contains("2019-11-03")]
cols = [2, 20, 21, 22]
df = df[df.columns[cols]]
df = df[df["day_in_week"] == 1]
df = df[df["lstm_mae"] >= 60]

x = np.array([station_list.index(element) for element in df['Sn']])
y = df['hour_in_day']
z = df['lstm_mae']

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

ax.set_xlabel("station")
ax.set_ylabel("hour in day")
ax.set_zlabel("lstm_mae")

ax.scatter(x, y, z, c=z)

plt.show()
plt.savefig()

plt.scatter(x, y, c=z, s=5)
cbar = plt.colorbar()
plt.xlabel("station")
plt.ylabel("hour in day")
plt.show()
