from utils import *
import os
import re
import pandas as pd
import json
import numpy
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



def initialize_variables():
    config = {}
    # Parameters to be edited
    config["quantile_columns_being_checked"] = (
        "act_travelling_time", "act_occupied_time")
    config["time_range_minutes"] = 1
    config["time_range_seconds"] = 60

    # URI for the files
    config["relative_uri_SICP"] = os.path.join("..", "SICP", "incident")
    config["relative_uri_accidents_records"] = os.path.join(
        "..", "accidents_record", "logs", "TCSS "
    )
    config["count_found_total"] = os.path.join(
        "Misc", "delay_count_found_total.txt")

    # For checking with all quantiles
    config["all_quantiles"] = get_quantile_range(100, 70, -5)
    config["list_of_quant_dicts"] = initialize_quantile_dicts(config)

    # Column being added to the CSV files
    config["columns_added"] = (
        "incident",
        "fault_description",
        "fault_classification",
        "propagated",
    )
    config["columns_added"] += tuple(
        [f"{column}_quantile" for column in config["quantile_columns_being_checked"]])

    config["columns_added_default_value"] = (False, "", "", True)
    config["columns_added_default_value"] += tuple([
        0 for column in config["quantile_columns_being_checked"]])
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


def get_station_mask(dataframe, stations):
    if len(stations) == 0:
        return numpy.full((dataframe.shape[0],), True)
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
    # Get prev and next station
    if index1 < index2:
        index1 -= 1
        index2 += 2
        return station_abr_list[max(0, index1):min(index2, len(station_abr_list))]
    else:
        index2 -= 1
        index1 += 2
        return station_abr_list[max(0, index2):min(index1, len(station_abr_list))]


def get_dataframe(config, relative_uri_csv):
    return read_csv(config, relative_uri_csv)


def get_json_data(relative_uri_json):
    json_file = open(relative_uri_json)
    json_data = json.load(json_file)
    json_file.close()
    return json_data


# Detects the incidents for the passed file
def detect_incidents(config, dataframe, data):
    total = 0
    num_found = 0
    for event in data["events"]:
        fault_desc = event["Fault Description"]
        fault_classification = event["Allocation"]
        for desc in event["event_descriptions"]:
            if desc["Train No"] != "":
                train_number = str(desc["Train No"])

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
                    stations = get_station_SICP(
                        dataframe, config, name_mask, desc["Event Time"]
                    )
                elif len(incident_station) == 1:
                    stations = incident_station
                else:
                    stations = fill_station_in_range(
                        incident_station[0], incident_station[-1])

                station_mask = get_station_mask(dataframe, stations)

                # Get track from station
                track = get_track_from_station(dataframe, stations)
                track_mask = get_track_mask(dataframe, track)

                query = dataframe.index[
                    station_mask & datetime_mask & track_mask
                ].sort_values(["act_arr_time"])

                incident_found = False
                for index in query[0]:
                    is_incident = False
                    quantile = ()
                    for i in range(len(config["quantile_columns_being_checked"])):
                        column = config["quantile_columns_being_checked"][i]
                        column_is_incident, column_quantile = check_quantile_track(
                            config["all_quantiles"],
                            config["list_of_quant_dicts"][i],
                            column,
                            index,
                            dataframe,
                        )
                        quantile += (column_quantile, )
                        if column_is_incident:
                            is_incident = True
                    if is_incident:
                        is_propagated = (
                            dataframe.iloc[index]["number_train"]
                            == train_number
                        )
                        added_tuple = (
                            is_incident,
                            fault_desc,
                            fault_classification,
                            is_propagated,
                        )
                        added_tuple += quantile
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


def write_count_found_total():
    with open(config["count_found_total"], "w") as f:
        debug_arr = [(config["dir_name"][i], config["count_for_each"][i], config["total_count"][i]) for i in range(
            min(len(config["count_for_each"]), len(config["total_count"])))]
        for dirname, found, total in debug_arr:
            f.write(
                f"{dirname}\n Incidents Tagged {found}\n Total Incidents {total}\n Percentage : {found/max(total,1) * 100}%\n\n")


def print_results(config):
    print(config["total_count"])
    print(config["count_for_each"])
    print(f'Total number of files processed: {len(config["total_count"])}')
    print(
        f"Match Percentage:"
        + str((sum(config["count_for_each"]) /
               sum(config["total_count"])) * 100)
    )


if __name__ == "__main__":
    config = initialize_variables()
    # Iterates through every SICP  file
    for dir_name in os.listdir(config["relative_uri_SICP"]):
        relative_uri_csv = config["relative_uri_SICP"] + "/" + dir_name
        try:
            json_file = csv_file_name_to_json_file_name(dir_name)
            if json_file is not None:
                relative_uri_json = config["relative_uri_accidents_records"] + json_file
                json_data = get_json_data(relative_uri_json)
                dataframe = get_dataframe(
                    config, relative_uri_csv)
                detect_incidents(config, dataframe, json_data)
                dataframe.to_csv(relative_uri_csv, index=False)
        except Exception as e:
            logger.exception(e)
    if config["debug"]:
        write_count_found_total()
        print_results(config)
