import json
from datetime import timedelta

import pandas as pd

station_list = [
    ("Whampoa", "WHA"),
    ("Ho Man Tin", "HOM"),
    ("Yau Ma Tei", "YMT"),
    ("Mong Kok", "MOK"),
    ("Prince Edward", "PRE"),
    ("Shek Kip Mei", "SKM"),
    ("Kowloon Tong", "KOT"),
    ("Lok Fu", "LOF"),
    ("Wong Tai Sin", "WTS"),
    ("Diamond Hill", "DIH"),
    ("Choi Hung", "CHH"),
    ("Kowloon Bay", "KOB"),
    ("Ngau Tau Kok", "NTK"),
    ("Kwun Tong", "KWT"),
    ("Lam Tin", "LAT"),
    ("Yau Tong", "YAT"),
    ("Tiu Keng Leng", "TIK"),
]

SICP_date_format = "%Y-%m-%d %H:%M:%S.%f"


def get_quantile_range(up, down, jump):
    """ Range is [up, down] """
    if jump > 0:
        raise Exception("Jump should be negative only")
    return tuple(range(up, down - 1, jump))


def format_date_json(event_time):
    event_time = event_time.split("/")
    event_time[-1] = "20" + event_time[-1]
    event_time = ("/").join(event_time)
    return pd.to_datetime(event_time, format="%d/%m/%Y %H:%M")


def initialize_quantile_dicts(config):
    column_quantile_dicts = ()
    for i in range(len(config['quantile_columns_being_checked'])):
        list_of_quant_dicts = ()
        column = config['quantile_columns_being_checked'][i]
        for x in config["all_quantiles"]:
            file_name = (
                f"Quantiles/{column}_quantile_{x}.json"
            )
            f = open(file_name)
            list_of_quant_dicts += (json.load(f),)
        column_quantile_dicts += (list_of_quant_dicts, )
    return column_quantile_dicts


# Creates the date mask corresponding to the event time
def get_datetime_mask(
        dataframe,
        incident_event_time,
        col_name,
        time_range_minutes=0,
        time_range_seconds=0,
        is_json_date=False,
):
    if is_json_date:
        event_time = format_date_json(incident_event_time)
    else:
        event_time = pd.to_datetime(
            incident_event_time, format=SICP_date_format)
    start_date = event_time - timedelta(
        minutes=time_range_minutes, seconds=time_range_seconds
    )
    end_date = event_time + timedelta(
        minutes=time_range_minutes, seconds=time_range_seconds
    )

    act_arr_time_series = pd.to_datetime(
        dataframe[col_name], format=SICP_date_format
    ).astype("datetime64[s]")
    return (act_arr_time_series > start_date) & (act_arr_time_series <= end_date)


# Calculates the quantile for every track and station pair
def check_quantile_track(
        all_quantiles, list_of_quant_dicts, columns_being_checked, index, dataframe
):
    track_no = dataframe.loc[[index], "track"].values[0]
    station = dataframe.loc[[index], "station"].values[0]
    track_station_pair = str(station) + "-" + str(track_no)

    for i, value in enumerate(all_quantiles):
        quantile_value = list_of_quant_dicts[i][track_station_pair]
        row_value = dataframe.loc[[index], columns_being_checked].values[0]
        if row_value >= quantile_value:
            return (True, value)
    return (False, 0)


def update_SICP_row(dataframe, columns_added, index, added_tuple):
    for i in range(len(columns_added)):
        dataframe.at[index, columns_added[i]] = added_tuple[i]


def get_abr_station_list():
    return [station[1] for station in station_list]


def find_station_index(station):
    return get_abr_station_list().index(station)
