import json
import pandas as pd
from datetime import timedelta


def format_date_json(event_time):
    event_time = event_time.split("/")
    event_time[-1] = "20" + event_time[-1]
    event_time = ("/").join(event_time)
    return pd.to_datetime(event_time, format="%d/%m/%Y %H:%M")


def initialize_quantile_dicts(config):
    list_of_quant_dicts = ()
    for x in config["all_quantiles"]:
        file_name = f"Quantiles/{config['quantile_column_being_checked']}_quantile_{x}.json"
        f = open(file_name)
        list_of_quant_dicts += (json.load(f), )
    return list_of_quant_dicts


# Creates the date mask corresponding to the event time
def get_datetime_mask(dataframe, incident_event_time, col_name, time_range, is_json_date=False):
    SICP_date_format = "%Y-%m-%d %H:%M:%S.%f"
    if is_json_date:
        event_time = format_date_json(incident_event_time)
    else:
        event_time = pd.to_datetime(
            incident_event_time, format=SICP_date_format
        )
    start_date = event_time - \
        timedelta(minutes=time_range)
    end_date = event_time + \
        timedelta(minutes=time_range)

    act_arr_time_series = pd.to_datetime(
        dataframe[col_name], format=SICP_date_format
    ).astype("datetime64[s]")
    return (act_arr_time_series > start_date) & (act_arr_time_series <= end_date)


# Calculates the quantile for every track and station pair
def check_quantile_track(all_quantiles, list_of_quant_dicts, column_being_checked, index, dataframe):
    track_no = dataframe.loc[[index], "track"].values[0]
    station = dataframe.loc[[index], "station"].values[0]
    track_station_pair = str(station) + "-" + str(track_no)

    for i, value in enumerate(all_quantiles):
        quantile_value = list_of_quant_dicts[i][track_station_pair]
        row_value = dataframe.loc[[index],
                                  column_being_checked].values[0]
        if row_value >= quantile_value:
            return (True, value)
    return (False, 0)
