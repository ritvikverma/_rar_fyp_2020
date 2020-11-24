from datetime import timedelta
import pandas as pd
import json
import os

quantile = .95

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
    if 'incident' not in dataframe:
        dataframe['incident'] = False
    return dataframe

def fn_name_mask(dataframe, col_name, train_number):
    return dataframe['train'].str.match('..' + train_number)

def fn_quantile_mask(dataframe, col_name, quantile):
    if type(col_name) is str:
        return dataframe[col_name] > dataframe[col_name].quantile(quantile)
    if type(col_name) is list and len(col_name) == 2:
        return ((dataframe[col_name[0]] > dataframe[col_name[0]].quantile(quantile)) | (dataframe[col_name[1]] > dataframe[col_name[1]].quantile(quantile)))
    raise Exception('incorrect input')

def fn_date_mask(dataframe, col_name, start_date, end_date):
    act_arr_time_series = pd.to_datetime(dataframe[col_name] , format="%Y-%m-%d %H:%M:%S.%f").astype('datetime64[s]')
    return (act_arr_time_series > start_date) & (act_arr_time_series <= end_date)

def fn_format_date(event_time):
    event_time = event_time.split('/')
    event_time[-1] = "20" + event_time[-1]
    event_time = ("/").join(event_time)
    return pd.to_datetime(event_time, format="%d/%m/%Y %H:%M")

def fn_detect_incidents(relative_uri_csv, relative_uri_json):    
    dataframe = fn_read_csv(relative_uri_csv)
    try:
        with open(relative_uri_json) as json_file:
            data = json.load(json_file)
            for event in data['events']:
                for desc in event['event_descriptions']:
                    if desc['Train No'] != "":
                        
                        train_number = desc['Train No']
                        
                        event_time = fn_format_date(desc['Event Time'])
                        start_date = event_time - timedelta(minutes = 5)
                        end_date = event_time + timedelta(minutes = 5)
                        
                        date_mask = fn_date_mask(dataframe, 'act_arr_time', start_date, end_date)
                        quantile_mask = fn_quantile_mask(dataframe, ['act_travelling_time','act_occupied_time'], quantile)
                        name_mask = fn_name_mask(dataframe, 'train', str(train_number))
                        
                        query = dataframe.index[name_mask & date_mask & quantile_mask].sort_values(['act_arr_time'])
                        
                        for index in query[0]:
                            dataframe.at[index, 'incident'] = True
                            
        dataframe.to_csv(relative_uri_csv, index=False)
    except os.error as e:
        print('File not found ' + e.filename)

for dir_name in os.listdir("SICP/daily"):

    relative_uri_csv = "SICP/daily/" + dir_name
    json_file = fn_csv_file_name_to_json_file_name(dir_name)
    if json_file is not None:
        relative_uri_json = "accidents_record/logs/TCSS " + json_file

    fn_detect_incidents(relative_uri_csv, relative_uri_json)
