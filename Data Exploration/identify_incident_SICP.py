import json
import os
from datetime import timedelta

import pandas as pd


def initialize_quantile_dicts(config):
    list_of_quant_dicts = ()
    for x in config["all_quantiles"]:
        file_name = f"Quantiles/{config['column_being_checked']}_quantile_{x}.json"
        f = open(file_name)
        list_of_quant_dicts += (json.load(f), )
    return list_of_quant_dicts


def initialize_variables():
    config = {}
    # Parameters to be edited
    config["column_being_checked"] = "act_travelling_time"
    config["time_range"] = 1

    # URI for the files
    config["relative_uri_SICP"] = os.path.join("../SICP", "incident")
    config["relative_uri_accidents_records"] = os.path.join(
        "../accidents_record", "logs", "TCSS ")
    config["relative_uri_json"] = ""

    # For checking with all quantiles
    config["all_quantiles"] = (95, 90, 85, 80, 75, 70)
    config["list_of_quant_dicts"] = initialize_quantile_dicts(config)

    # Column being added to the CSV files
    config["columns_added"] = ("incident", "quantile")
    config["columns_added_default_value"] = (False, 0)
    config["count_for_each"] = ()
    config["total_count"] = ()

    config["debug"] = True
    return config


# Finds json incident file corresponding to the SICP file
def csv_file_name_to_json_file_name(dir_name):
    date = dir_name[0:2]
    month = dir_name[2:5].lower()
    year = "20" + dir_name[5:7]
    extension = ".json"
    if month == "nov":
        month = "11"
    elif month == "dec":
        month = "12"
    else:
        raise Exception("cannot convert csv to JSON file")
    return year + month + date + extension


# Returns csv file as dataframe
def read_csv(config, relative_uri):
    dataframe = pd.read_csv(relative_uri)

    dataframe['destination_code_train'] = dataframe['train'].str[0]
    dataframe['timetable_code_train'] = dataframe['train'].str[1]
    dataframe['number_train'] = dataframe['train'].str[2:]

    for i in range(len(config["columns_added"])):
        if config["columns_added"][i] not in dataframe:
            dataframe[config["columns_added"][i]
                      ] = config["columns_added_default_value"][i]

    return dataframe


# Creates the name mask corresponding to the train number
def get_name_mask(dataframe, train_number):
    return dataframe["number_train"].str.match(train_number)


# Creates the quantile mask
def get_quantile_mask(dataframe, col_name, quantile):
    if type(col_name) is str:
        return dataframe[col_name] > dataframe[col_name].quantile(quantile)
    if type(col_name) is list and len(col_name) == 2:
        return (dataframe[col_name[0]] > dataframe[col_name[0]].quantile(quantile)) | (
            dataframe[col_name[1]] > dataframe[col_name[1]].quantile(quantile)
        )
    raise Exception("incorrect input")


# Creates the date mask corresponding to the event time
def get_datetime_mask(dataframe, col_name, start_date, end_date):
    act_arr_time_series = pd.to_datetime(
        dataframe[col_name], format="%Y-%m-%d %H:%M:%S.%f"
    ).astype("datetime64[s]")
    return (act_arr_time_series > start_date) & (act_arr_time_series <= end_date)

# Converts date time in SICP to python datetime object


def format_date(event_time):
    event_time = event_time.split("/")
    event_time[-1] = "20" + event_time[-1]
    event_time = ("/").join(event_time)
    return pd.to_datetime(event_time, format="%d/%m/%Y %H:%M")


# Calculates the quantile for every track and station pair
def check_quantile_track(config, index, dataframe):
    track_no = dataframe.loc[[index], "track"].values[0]
    station = dataframe.loc[[index], "station"].values[0]
    track_station_pair = str(station) + "-" + str(track_no)

    for i, value in enumerate(config["all_quantiles"]):
        quantile_value = config["list_of_quant_dicts"][i][track_station_pair]
        row_value = dataframe.loc[[index],
                                  config["column_being_checked"]].values[0]
        if row_value >= quantile_value:
            return (True, value)
    return (False, 0)


# Detects the incidents for the passed file
def detect_incidents(config, relative_uri_csv, relative_uri_json):
    dataframe = read_csv(config, relative_uri_csv)
    try:
        total = 0
        num_found = 0
        with open(relative_uri_json) as json_file:
            data = json.load(json_file)
            for event in data["events"]:
                for desc in event["event_descriptions"]:
                    if desc["Train No"] != "":
                        train_number = desc["Train No"]

                        event_time = format_date(desc["Event Time"])
                        start_date = event_time - \
                            timedelta(minutes=config["time_range"])
                        end_date = event_time + \
                            timedelta(minutes=config["time_range"])

                        datetime_mask = get_datetime_mask(
                            dataframe, "act_arr_time", start_date, end_date
                        )

                        name_mask = get_name_mask(dataframe, str(train_number))

                        query = dataframe.index[
                            name_mask & datetime_mask
                        ].sort_values(["act_arr_time"])

                        for index in query[0]:
                            quantile_check = check_quantile_track(
                                config, index, dataframe)
                            is_incident = quantile_check[0]

                            if is_incident:
                                num_found += 1
                                for i in range(len(config["columns_added"])):
                                    dataframe.at[index,
                                                 config["columns_added"]] = quantile_check[i]

                        total += 1
        config["total_count"] += (total, )
        config["count_for_each"] += (num_found, )
        dataframe.to_csv(relative_uri_csv, index=False)
    except os.error as e:
        print("File not found " + e.filename)


def print_results():
    print(config["total_count"])
    print(config["count_for_each"])
    print("Total number of files processed: " + str(count))
    print("Match Percentage:" +
          str((sum(config["count_for_each"]) / sum(config["total_count"])) * 100))


if __name__ == '__main__':
    config = initialize_variables()
    # Iterates through every SICP  file
    count = 0
    for dir_name in os.listdir(config["relative_uri_SICP"]):
        relative_uri_csv = config["relative_uri_SICP"] + "/" + dir_name
        json_file = csv_file_name_to_json_file_name(dir_name)
        if json_file is not None:
            relative_uri_json = config["relative_uri_accidents_records"] + json_file
            detect_incidents(config, relative_uri_csv, relative_uri_json)
            count += 1
    print_results()
