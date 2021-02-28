import json
import os
import math

import numpy as np
from numpy.core.numeric import NaN
import pandas as pd

# URI for the files
relative_uri_SICP = os.path.join("..", "SICP", "incident")
relative_uri_json = ""
quantile_per_station_track = {}
quantiles = [.95, .90, .85, .80, .75, .70]
quantile_columns = ["act_travelling_time", "act_occupied_time"]


def get_group_by_list(dataframe):
    key_station, key_track, values = (
        dataframe.sort_values(["station", "track"])
        .filter(["station", "track", quantile_column])
        .values.T
    )
    keys = [key_station[i] + "-" + key_track[i]
            for i in range(len(key_station))]
    ukeys, index = np.unique(keys, True)
    arrays = np.split(values, index[1:])
    return {"station-track": ukeys, quantile_column: [list(a) for a in arrays]}


for quantile_column in quantile_columns:
    for dir_name in os.listdir(relative_uri_SICP):
        dataframe = pd.DataFrame()
        relative_uri_csv = relative_uri_SICP + "/" + dir_name
        dataframe = dataframe.append(pd.read_csv(relative_uri_csv))
    return_dict = get_group_by_list(dataframe)
    for i in range(len(return_dict["station-track"])):
        if return_dict["station-track"][i] not in quantile_per_station_track:
            quantile_per_station_track[return_dict["station-track"][i]] = []
        quantile_per_station_track[return_dict["station-track"][i]] += return_dict[
            quantile_column
        ][i]

    for quantile in quantiles:
        dict = {}
        for key in quantile_per_station_track:
            dict[key] = np.nanquantile(
                quantile_per_station_track[key], quantile)
        output_file = f"Quantiles/{quantile_column}_quantile_{int(quantile * 100)}.json"

        dict = {k: v for k, v in dict.items() if not math.isnan(v)}
        with open(output_file, "w") as file:
            json.dump(dict, file)
