import os
import pandas as pd

from utils import get_datetime_mask, check_quantile_track, initialize_quantile_dicts


def initialize_variables():
    config = {}
    config["relative_uri_SICP"] = os.path.join("..", "SICP", "incident")

    # Algo dependent variables
    config["quantile_column_being_checked"] = "act_travelling_time"
    config["time_range_min"] = 5

    # Column being added to the CSV files
    config["columns_added"] = ("propagated_incident", "propagated_fault")
    config["columns_added_default_value"] = (False, "")

    # For checking with all quantiles
    config["all_quantiles"] = (95, 90, 85, 80, 75, 70)
    config["list_of_quant_dicts"] = initialize_quantile_dicts(config)

    config["physical_train_number_column"] = "number_train"

    return config


def read_csv(config, relative_uri):
    dataframe = pd.read_csv(relative_uri)

    for i in range(len(config["columns_added"])):
        if config["columns_added"][i] not in dataframe:
            dataframe[config["columns_added"][i]
                      ] = config["columns_added_default_value"][i]

    return dataframe


def get_value_from_incident(dataframe, incident, col_name):
    return incident[dataframe.columns.get_loc(
        col_name) + 1]


def filter_dataframe(config, dataframe, incident):
    station_mask = dataframe["station"] == get_value_from_incident(
        dataframe, incident, "station")
    track_mask = dataframe["track"] == get_value_from_incident(
        dataframe, incident, "track")
    datetime_mask = get_datetime_mask(
        dataframe, get_value_from_incident(
            dataframe, incident, "act_arr_time"), "act_arr_time", config["time_range_min"]
    )
    train_number_mask = dataframe[config["physical_train_number_column"]
                                  ] != get_value_from_incident(
        dataframe, incident, config["physical_train_number_column"])

    query = dataframe.index[station_mask & track_mask & datetime_mask & train_number_mask].sort_values(
        ["act_arr_time"])
    return query


def check_delay_propogation(config, dataframe, relative_uri_csv):
    incidents = dataframe[dataframe["incident"] == True]

    for incident in incidents.itertuples():
        query = filter_dataframe(config, dataframe, incident)
        for index in query[0]:
            is_incident, _ = check_quantile_track(
                config["all_quantiles"], config["list_of_quant_dicts"], config["quantile_column_being_checked"], index, dataframe)
            added_tuple = (is_incident, get_value_from_incident(
                dataframe, incident, "fault_description"))
            if is_incident:
                for i in range(len(config["columns_added"])):
                    dataframe.at[index,
                                 config["columns_added"][i]] = added_tuple[i]

    dataframe.to_csv(relative_uri_csv, index=False)


if __name__ == '__main__':
    config = initialize_variables()
    for dir_name in os.listdir(config["relative_uri_SICP"]):
        relative_uri_csv = os.path.join(config["relative_uri_SICP"], dir_name)
        dataframe = read_csv(config, relative_uri_csv)
        check_delay_propogation(config, dataframe, relative_uri_csv)
