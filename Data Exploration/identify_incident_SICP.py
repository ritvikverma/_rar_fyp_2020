import json
import os
from datetime import timedelta

import pandas as pd

# Parameters to be edited
column_being_checked = "act_travelling_time"
quantile = 85
time_range = 1

# URI for the files
relative_uri_SICP = os.path.join("../SICP", "daily")
relative_uri_accidents_records = os.path.join("../accidents_record", "logs", "TCSS ")
relative_uri_json = ""

# import Utils.csv_to_json_file_name as csv_to_json
# from _rar_fyp_2020.Utils.csv_to_json_file_name import csv_to_json_file_name as csv_to_json
file_name = f"Quantiles/{column_being_checked}_quantile_{quantile}.0.json"
f = open(file_name)
quantile_per_track = json.load(f)

# For checking with all quantiles
all_quantiles = [95, 90, 85, 80, 75, 70]
list_of_quant_dicts = []
for x in all_quantiles:
    file_name = f"Quantiles/{column_being_checked}_quantile_{x}.0.json"
    f = open(file_name)
    list_of_quant_dicts.append(json.load(f))

# Column being added to the CSV files
# column_name_added = f"quantile_{quantile}_{column_being_checked}"
column_name_added = "all_quantile"
count_for_each = []
total_count = []


# Finds json incident file corresponding to the SICP file
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


# Returns csv file as dataframe
def fn_read_csv(relative_uri):
    dataframe = pd.read_csv(relative_uri)
    if column_name_added not in dataframe:
        dataframe[column_name_added] = str(False)

    return dataframe


# Creates the name mask corresponding to the train number
def fn_name_mask(dataframe, col_name, train_number):
    return dataframe["train"].str.match(".." + train_number)


## Uncomment the following code to use a quantile Mask
## Creates the quantile mask
# def fn_quantile_mask(dataframe, col_name, quantile):
#     if type(col_name) is str:
#         return dataframe[col_name] > dataframe[col_name].quantile(quantile)
#     if type(col_name) is list and len(col_name) == 2:
#         return (dataframe[col_name[0]] > dataframe[col_name[0]].quantile(quantile)) | (
#             dataframe[col_name[1]] > dataframe[col_name[1]].quantile(quantile)
#         )
#     raise Exception("incorrect input")


# Creates the date mask corresponding to the event time
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

    ## Uncomment this if quantile values are not being loaded from a json file
    #
    # if track_station_pair not in quantile_per_track:
    #     df=dataframe.loc[(dataframe['track'] == track_no) & (dataframe['station'] == station)]
    #     quantile_value= df[column_being_checked].quantile(quantile)
    #     quantile_per_track[track_station_pair]=quantile_value
    #
    try:

        for ind, value in enumerate(all_quantiles):
            a = dataframe.loc[[index], column_being_checked].values[0]
            b = list_of_quant_dicts[ind][track_station_pair]
            if dataframe.loc[[index], column_being_checked].values[0] >= list_of_quant_dicts[ind][track_station_pair]:
                return "True-" + str(value)
                break

        # if dataframe.loc[[index], column_being_checked].values[0] >= quantile_per_track[track_station_pair]:
        #     return True
    except:
        return False
    return False


# Detects the incidents for the passed file
def fn_detect_incidents(relative_uri_csv, relative_uri_json):
    dataframe = fn_read_csv(relative_uri_csv)
    dataframe.to_csv(relative_uri_csv, index=False)
    try:
        total = 0
        num_found = 0
        with open(relative_uri_json) as json_file:
            data = json.load(json_file)
            for event in data["events"]:
                for desc in event["event_descriptions"]:
                    if desc["Train No"] != "":

                        total += 1
                        temp = 0

                        train_number = desc["Train No"]

                        event_time = fn_format_date(desc["Event Time"])
                        start_date = event_time - timedelta(minutes=time_range)
                        end_date = event_time + timedelta(minutes=time_range)

                        date_mask = fn_date_mask(
                            dataframe, "act_arr_time", start_date, end_date
                        )

                        name_mask = fn_name_mask(dataframe, "train", str(train_number))

                        queryx = dataframe.index[
                            name_mask
                        ].sort_values(["act_arr_time"])

                        query = dataframe.index[
                            name_mask & date_mask
                            ].sort_values(["act_arr_time"])

                        for index in query[0]:
                            returned_val = check_quantile_track(index, dataframe)
                            if returned_val != "False":
                                temp += 1
                                dataframe.at[index, column_name_added] = returned_val
                            else:
                                dataframe.at[index, column_name_added] = False
                        if temp != 0:
                            num_found += 1
        total_count.append(total)
        count_for_each.append(num_found)
        dataframe.to_csv(relative_uri_csv, index=False)
    except os.error as e:
        print("File not found " + e.filename)


# Iterates through every SICP  file
count = 0
for dir_name in os.listdir(relative_uri_SICP):

    relative_uri_csv = relative_uri_SICP + "/" + dir_name
    json_file = fn_csv_file_name_to_json_file_name(dir_name)
    if json_file is not None:
        relative_uri_json = relative_uri_accidents_records + json_file

    fn_detect_incidents(relative_uri_csv, relative_uri_json)
    print(dir_name)
    count += 1
    # if count==4:
    #     break

print(total_count)
print(count_for_each)
print("Total number of files processed: " + str(count))

tot = sum(total_count)
print("Match Percentage:" + str((sum(count_for_each) / tot) * 100))
