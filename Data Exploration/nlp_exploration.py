import json
import os
from collections import Counter

relative_uri_accidents_records = os.path.join("../accidents_record", "logs")
accident_jsons = {}


def populate_accidents_dict():
    for accident_json_name in os.listdir(relative_uri_accidents_records):
        with open(os.path.join(relative_uri_accidents_records, accident_json_name)) as f:
            retrieved_json = json.load(f)
            accident_jsons[accident_json_name.split('.')[0].split()[1]] = retrieved_json['events']


def get_repeated_data(text_descriptions):
    returnable = [k for k, v in Counter(text_descriptions).items() if v > 1]
    returnable = [description.lower() for description in returnable]
    return list(set(returnable))


if __name__ == '__main__':
    populate_accidents_dict()
    text_descriptions = []
    for day, events in accident_jsons.items():
        for event in events:
            has_train_in_any_event = False
            for desc in event['event_descriptions']:
                if desc['Train No'].strip() != "":
                    has_train_in_any_event = True
            if has_train_in_any_event:
                text_descriptions.append(event["Fault Description"])
    print(len(text_descriptions))
    print(get_repeated_data(text_descriptions))
