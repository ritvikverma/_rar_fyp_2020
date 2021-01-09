import json
import numpy
import pandas as pd
import os

# URI for the files
relative_uri_SICP = os.path.join("..", "SICP", "daily")
relative_uri_json = ""
quantile_per_track = {}
quantile = 0.90
output_file = "act_occupied_time_quantile.json"
quantile_column = "act_occupied_time"

for dir_name in os.listdir(relative_uri_SICP):
    relative_uri_csv = relative_uri_SICP + "/" + dir_name
    dataframe = pd.read_csv(relative_uri_csv)
    for index, row in dataframe.iterrows():
        track_no = dataframe.loc[[index], "track"].values[0]
        station = dataframe.loc[[index], "station"].values[0]
        track_station_pair = str(track_no) + "-" + str(station)
        if track_station_pair not in quantile_per_track:
            quantile_per_track[track_station_pair] = []
        quantile_per_track[track_station_pair].append(
            dataframe.loc[
                (dataframe["track"] == track_no) & (dataframe["station"] == station)
            ][quantile_column]
        )

for key in quantile_per_track:
    quantile_per_track[key] = numpy.array(quantile_per_track[key]).quantile(quantile)

with open(output_file, "w") as file:
    json.dump(quantile_per_track, file)
