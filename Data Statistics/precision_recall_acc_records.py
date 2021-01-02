#URI for the files
relative_uri_SICP = "../SICP/daily"
relative_uri_accidents_records = "../accidents_record/logs/TCSS "

for dir_name in os.listdir(relative_uri_SICP):

    relative_uri_csv = relative_uri_SICP + "/" + dir_name
    json_file = fn_csv_file_name_to_json_file_name(dir_name)
    if json_file is not None:
        relative_uri_json = relative_uri_accidents_records + json_file

    fn_detect_incidents(relative_uri_csv, relative_uri_json)
