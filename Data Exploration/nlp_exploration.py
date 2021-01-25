import json
import os

relative_uri_accidents_records = os.path.join("../accidents_record", "logs")
accident_jsons = {}


def populate_accidents_dict():
    for accident_json_name in os.listdir(relative_uri_accidents_records):
        with open(os.path.join(relative_uri_accidents_records, accident_json_name)) as f:
            retrieved_json = json.load(f)
            accident_jsons[accident_json_name.split('.')[0].split()[1]] = retrieved_json['events']


if __name__ == '__main__':
    populate_accidents_dict()
    for day, events in accident_jsons.items():
        for event in events:
            print(event['Fault Description'])
