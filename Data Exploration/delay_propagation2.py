import json
import os
import pandas as pd

from utils import get_datetime_mask, check_quantile_track, initialize_quantile_dicts, update_SICP_row, get_next_prev_station


def initialize_variables():
    config = {}
    # Parameters to be edited
    config["quantile_column_being_checked"] = "act_travelling_time"
    config["time_range"] = 1

    # URI for the files
    config["relative_uri_SICP"] = os.path.join("..", "SICP", "incident")
    config["relative_uri_accidents_records"] = os.path.join(
        "..", "accidents_record", "logs", "TCSS ")
    config["count_found_total"] = os.path.join("Misc", "count_found_total.txt")

    # For checking with all quantiles
    config["all_quantiles"] = (95, 90, 85, 80, 75, 70)
    config["list_of_quant_dicts"] = initialize_quantile_dicts(config)

    # Column being added to the CSV files
    config["columns_added"] = (
        "incident", "quantile", "fault_description", "propagated")
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

                        datetime_mask = get_datetime_mask(
                            dataframe, desc["Event Time"], "act_arr_time", config["time_range"], is_json_date=True
                        )

                        name_mask = get_name_mask(dataframe, str(train_number))
                        station_mask = get_next_prev_station(station)
                        query = dataframe.index[
                            name_mask & datetime_mask
                        ].sort_values(["act_arr_time"])

                        incident_found = False
                        for index in query[0]:
                            is_incident, quantile = check_quantile_track(
                                config["all_quantiles"], config["list_of_quant_dicts"], config["quantile_column_being_checked"], index, dataframe)
                            added_tuple = (
                                is_incident, quantile, fault_desc)
                            if is_incident:
                                incident_found = True
                                update_SICP_row(
                                    dataframe, config["columns_added"], index, added_tuple)
                        if incident_found:
                            num_found += 1
                        total += 1
        if config["debug"]:
            config["total_count"] += (total, )
            config["count_for_each"] += (num_found, )
            config["dir_name"] += (relative_uri_csv, )
        dataframe.to_csv(relative_uri_csv, index=False)
    except os.error as e:
        print("File not found " + e.filename)


def write_count_found_total():
    with open(config["count_found_total"], "w") as f:
        debug_arr = [(config["dir_name"][i], config["count_for_each"][i], config["total_count"][i]) for i in range(
            min(len(config["count_for_each"]), len(config["total_count"])))]
        for dirname, found, total in debug_arr:
            f.write(
                f"{dirname}\n Incidents Tagged {found}\n Total Incidents {total}\n Percentage : {found/total * 100}%\n\n")


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
    if config["debug"]:
        write_count_found_total()
        print_results()
