import json
import os
from datetime import timedelta

import pandas as pd

# URI for the files
column_being_checked = "act_occupied_time"
quantile = 90
time_range = 1
relative_uri_SICP = os.path.join("../SICP", "daily")
relative_uri_accidents_records = os.path.join("../accidents_record", "logs", "TCSS ")
relative_uri_json = ""
column_name_added = f"quantile_{quantile}_{column_being_checked}"

# import Utils.csv_to_json_file_name as csv_to_json
# from _rar_fyp_2020.Utils.csv_to_json_file_name import csv_to_json_file_name as csv_to_json
file_name = f"Quantiles/{column_being_checked}_quantile_{quantile}.0.json"
f = open(file_name)
quantile_per_track = json.load(f)

data_points = {}


def fn_csv_file_name_to_json_file_name(dir_name):
    date = dir_name[0:2]
    month = dir_name[2:5].lower()
    year = "20" + dir_name[5:7]
    extension = ".json"
    if month == "nov":
        month = "11"
    elif month == "dec":
        month = "12"
    else:
        return None
    return year + month + date + extension


def fn_read_csv(relative_uri):
    dataframe = pd.read_csv(relative_uri)
    if column_name_added not in dataframe:
        dataframe[column_name_added] = False

    return dataframe


def fn_name_mask(dataframe, col_name, train_number):
    return dataframe["train"].str.match(".." + train_number)


def fn_quantile_mask(dataframe, col_name, quantile):
    if type(col_name) is str:
        return dataframe[col_name] > dataframe[col_name].quantile(quantile)
    if type(col_name) is list and len(col_name) == 2:
        return (dataframe[col_name[0]] > dataframe[col_name[0]].quantile(quantile)) | (
            dataframe[col_name[1]] > dataframe[col_name[1]].quantile(quantile)
        )
    raise Exception("incorrect input")


def fn_date_mask(dataframe, col_name, start_date, end_date):
    act_arr_time_series = pd.to_datetime(
        dataframe[col_name], format="%Y-%m-%d %H:%M:%S.%f"
    ).astype("datetime64[s]")
    return (act_arr_time_series > start_date) & (act_arr_time_series <= end_date)


def fn_format_date(event_time):
    event_time = event_time.split("/")
    event_time[-1] = "20" + event_time[-1]
    event_time = ("/").join(event_time)
    return pd.to_datetime(event_time, format="%d/%m/%Y %H:%M")

#Calculates the quantile for every track and station pair
def check_quantile_track(index, dataframe):
    track_no = dataframe.loc[[index], "track"].values[0]
    station = dataframe.loc[[index], "station"].values[0]
    track_station_pair = str(station) + "-" + str(track_no)
    # if track_station_pair not in quantile_per_track:
    #     df=dataframe.loc[(dataframe['track'] == track_no) & (dataframe['station'] == station)]
    #     quantile_value= df[column_being_checked].quantile(quantile)
    #     quantile_per_track[track_station_pair]=quantile_value
    try:
        if dataframe.loc[[index], column_being_checked].values[0] >= quantile_per_track[track_station_pair]:
            print(quantile_per_track[track_station_pair])
            off_by_quant = dataframe.loc[[index], column_being_checked].values[0] - quantile_per_track[
                track_station_pair]
            print("off by quantile=" + str(off_by_quant))
            print(dataframe.loc[[index], "act_arr_time"].values[0])
            return True, off_by_quant
    except:
        return False, 0
    return False, 0

def fn_detect_incidents(relative_uri_csv, relative_uri_json):
    dataframe = fn_read_csv(relative_uri_csv)
    dataframe.to_csv(relative_uri_csv, index=False)
    count=0
    df_indices = []
    try:
        with open(relative_uri_json) as json_file:
            data = json.load(json_file)
            for event in data["events"]:
                for desc in event["event_descriptions"]:
                    if desc["Train No"] != "":

                        train_number = desc["Train No"]

                        event_time = fn_format_date(desc["Event Time"])
                        start_date = event_time - timedelta(minutes=time_range)
                        end_date = event_time + timedelta(minutes=time_range)

                        date_mask = fn_date_mask(
                            dataframe, "act_arr_time", start_date, end_date
                        )

                        name_mask = fn_name_mask(dataframe, "train", str(train_number))

                        query = dataframe.index[
                            name_mask & date_mask
                            ].sort_values(["act_arr_time"])

                        print(desc["Item"])
                        print(event_time)
                        num = 0
                        max = 0
                        for index in query[0]:
                            is_incident, off_by_quant = check_quantile_track(index, dataframe)
                            if is_incident:
                                num += 1
                                if off_by_quant > max:
                                    max = off_by_quant
                                df_indices.append(index)
                                dataframe.at[index, column_name_added] = True
                        print(num, "\n")
                        name_for_plot = str(event_time) + str(desc["Item"])
                        if max > 0:
                            data_points[name_for_plot] = max

        df_new = dataframe.take(df_indices)
        dataframe.to_csv(relative_uri_csv, index=False)
    except os.error as e:
        print("File not found " + e.filename)


# count=0
for dir_name in os.listdir(relative_uri_SICP):

    # count+=1
    relative_uri_csv = relative_uri_SICP + "/" + dir_name
    json_file = fn_csv_file_name_to_json_file_name(dir_name)
    if json_file is not None:
        relative_uri_json = relative_uri_accidents_records + json_file

    fn_detect_incidents(relative_uri_csv, relative_uri_json)
    print(dir_name)
    # if count>3:
    #     break

# lists = data_points.items() # return a list of tuples
#
# x, y = zip(*lists) # unpack a list of pairs into two tuples
#
# plt.plot(x, y, marker='.', linestyle='none')
# plt.xticks([])
# # plt.ylim(0, 500)
# plt.grid(True)
# plt.show()
