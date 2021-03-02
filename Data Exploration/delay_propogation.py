import os

import pandas as pd


def initialize_variables():
    config = {}
    config["relative_uri_SICP"] = os.path.join("..", "SICP", "incident")

    # Column being added to the CSV files
    config["columns_added"] = ("propagated_incident", )
    config["columns_added_default_value"] = (False, )

    return config


def read_csv(config, relative_uri):
    dataframe = pd.read_csv(relative_uri)

    for i in range(len(config["columns_added"])):
        if config["columns_added"][i] not in dataframe:
            dataframe[config["columns_added"][i]
                      ] = config["columns_added_default_value"][i]

    return dataframe


def check_delay_propogation(dataframe):
    incidents = dataframe[dataframe["incident"] == True]

if __name__ == '__main__':
    config = initialize_variables()
    for dir_name in os.listdir(config["relative_uri_SICP"]):
        relative_uri_csv = os.path.join(config["relative_uri_SICP"], dir_name)
        dataframe = read_csv(config, relative_uri_csv)
        check_delay_propogation(dataframe)
