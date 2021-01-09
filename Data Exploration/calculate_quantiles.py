import json
import numpy as np
import pandas as pd
import os

# URI for the files
relative_uri_SICP = os.path.join("..", "SICP", "daily")
relative_uri_json = ""
quantile_per_station_track = {}
quantile = 0.90
output_file = "act_occupied_time_quantile.json"
quantile_column = "act_occupied_time"


def fn_get_group_by_list(dataframe):
    key_station, key_track, values = (
        dataframe.sort_values(["station", "track"])
        .filter(["station", "track", quantile_column])
        .values.T
    )
    keys = [key_station[i] + "-" + key_track[i] for i in range(len(key_station))]
    ukeys, index = np.unique(keys, True)
    arrays = np.split(values, index[1:])
    return {"station-track": ukeys, "act_occupied_time": [list(a) for a in arrays]}


for dir_name in os.listdir(relative_uri_SICP):
    relative_uri_csv = relative_uri_SICP + "/" + dir_name
    dataframe = pd.read_csv(relative_uri_csv)
    return_dict = fn_get_group_by_list(dataframe)
    for i in range(len(return_dict["station-track"])):
        if return_dict["station-track"][i] not in quantile_per_station_track:
            quantile_per_station_track[return_dict["station-track"][i]] = []
        quantile_per_station_track[return_dict["station-track"][i]] += return_dict[
            quantile_column
        ][i]

for key in quantile_per_station_track:
    quantile_per_station_track[key] = np.nanquantile(
        quantile_per_station_track[key], quantile
    )

with open(output_file, "w") as file:
    json.dump(quantile_per_station_track, file)
