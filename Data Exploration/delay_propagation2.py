import json
import os
from typing import Iterable
import pandas as pd
import numpy
import re

from utils import (
    get_datetime_mask,
    check_quantile_track,
    initialize_quantile_dicts,
    update_SICP_row,
    get_next_prev_station,
    station_list,
    get_abr_station_list
)


def initialize_variables():
    config = {}
    # Parameters to be edited
    config["quantile_column_being_checked"] = "act_travelling_time"
    config["time_range_minutes"] = 1
    config["time_range_seconds"] = 60

    # URI for the files
    config["relative_uri_SICP"] = os.path.join("..", "SICP", "incident")
    config["relative_uri_accidents_records"] = os.path.join(
        "..", "accidents_record", "logs", "TCSS "
    )
    config["count_found_total"] = os.path.join("Misc", "count_found_total.txt")

    # For checking with all quantiles
    config["all_quantiles"] = (95, 90, 85, 80, 75, 70)
    config["list_of_quant_dicts"] = initialize_quantile_dicts(config)

    # Column being added to the CSV files
    config["columns_added"] = (
        "incident",
        "quantile",
        "fault_description",
        "propagated",
    )
    config["columns_added_default_value"] = (False, 0, "", True)
    config["count_for_each"] = ()
    config["total_count"] = ()
    config["dir_name"] = ()

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

    dataframe["destination_code_train"] = dataframe["train"].str[0]
    dataframe["timetable_code_train"] = dataframe["train"].str[1]
    dataframe["number_train"] = dataframe["train"].str[2:]

    for i in range(len(config["columns_added"])):
        if config["columns_added"][i] not in dataframe:
            dataframe[config["columns_added"][i]] = config[
                "columns_added_default_value"
            ][i]

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


def first_or_nothing(array):
    if len(array) != 0:
        return array[0]
    return None


def get_track_from_station(dataframe, station):
    track = dataframe[dataframe["station"].isin(station)]["track"]
    return track.unique()


def get_station_SICP(dataframe, config, name_mask, event_time):
    datetime_mask = get_datetime_mask(
        dataframe,
        event_time,
        "act_arr_time",
        time_range_seconds=config["time_range_seconds"],
        is_json_date=True,
    )
    dataframe = dataframe[name_mask &
                          datetime_mask].sort_values(["act_arr_time"])
    return dataframe["station"].unique()


def get_station_mask(dataframe, station):
    if len(station) == 0:
        return numpy.full((dataframe.shape[0],), True)
    else:
        stations = get_next_prev_station(station)
    return dataframe["station"].isin(stations)


def get_numbers(string):
    return int(re.findall(r"[0-9]+", string)[0])


def is_even(x):
    return int(x) % 2 == 0


def get_track_mask(dataframe, track):
    even = False
    if type(track) is int:
        even = is_even(track)
    else:
        even = all([is_even(get_numbers(t)) for t in track])

    train_numbers = dataframe["number_train"].apply(get_numbers)
    if even:
        return train_numbers % 2 == 0
    else:
        return train_numbers % 2 == 1


def station_in_station_list(incident_location):
    station_abr_list = get_abr_station_list()
    return list(filter(lambda x: x in station_abr_list, incident_location.split()))


def fill_station_in_range(station1, station2):
    station_abr_list = get_abr_station_list()
    index1 = station_abr_list.index(station1)
    index2 = station_abr_list.index(station2)
    # print(station1, station2)
    # print(index1, min(index2 + 1, len(station_abr_list) - 1))
    # input()
    return station_abr_list[index1:index2]


# Detects the incidents for the passed file
def detect_incidents(config, relative_uri_csv, relative_uri_json):
    dataframe = read_csv(config, relative_uri_csv)
    try:
        total = 0
        num_found = 0
        with open(relative_uri_json) as json_file:
            data = json.load(json_file)
            for event in data["events"]:
                fault_desc = event["Fault Description"]
                for desc in event["event_descriptions"]:
                    if desc["Train No"] != "":
                        train_number = desc["Train No"]
                        train_number = str(train_number)

                        datetime_mask = get_datetime_mask(
                            dataframe,
                            desc["Event Time"],
                            "act_arr_time",
                            time_range_minutes=config["time_range_minutes"],
                            is_json_date=True,
                        )

                        name_mask = get_name_mask(dataframe, train_number)

                        # Get station logic
                        incident_station = station_in_station_list(
                            desc['Location'])
                        if len(incident_station) == 0:
                            station = get_station_SICP(
                                dataframe, config, name_mask, desc["Event Time"]
                            )
                        elif len(incident_station) == 1:
                            station = incident_station
                        else:
                            station = fill_station_in_range(
                                incident_station[0], incident_station[-1])

                        # print(incident_station)
                        # print(station)
                        # print()

                        station_mask = get_station_mask(dataframe, station)

                        track = get_track_from_station(dataframe, station)
                        track_mask = get_track_mask(dataframe, track)

                        query = dataframe.index[
                            station_mask & datetime_mask & track_mask
                        ].sort_values(["act_arr_time"])

                        incident_found = False
                        for index in query[0]:
                            is_incident, quantile = check_quantile_track(
                                config["all_quantiles"],
                                config["list_of_quant_dicts"],
                                config["quantile_column_being_checked"],
                                index,
                                dataframe,
                            )
                            if is_incident:
                                is_propagated = (
                                    dataframe.iloc[index]["number_train"]
                                    == train_number
                                )
                                added_tuple = (
                                    is_incident,
                                    quantile,
                                    fault_desc,
                                    is_propagated,
                                )
                                incident_found = True
                                update_SICP_row(
                                    dataframe,
                                    config["columns_added"],
                                    index,
                                    added_tuple,
                                )
                        if incident_found:
                            num_found += 1
                        total += 1
        if config["debug"]:
            config["total_count"] += (total,)
            config["count_for_each"] += (num_found,)
            config["dir_name"] += (relative_uri_csv,)
        dataframe.to_csv(relative_uri_csv, index=False)
    except os.error as e:
        print("File not found " + e.filename)


def print_results():
    print(config["total_count"])
    print(config["count_for_each"])
    print("Total number of files processed: " + str(count))
    print(
        "Match Percentage:"
        + str((sum(config["count_for_each"]) /
               sum(config["total_count"])) * 100)
    )


if __name__ == "__main__":
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
    if config["debug"]:
        print_results()
