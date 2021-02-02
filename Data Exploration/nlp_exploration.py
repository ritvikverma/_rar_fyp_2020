import json
import os
import string
from collections import Counter

relative_uri_accidents_records = os.path.join("../accidents_record", "logs")
accident_jsons = {}


def populate_accidents_dict():
    for accident_json_name in os.listdir(relative_uri_accidents_records):
        with open(os.path.join(relative_uri_accidents_records, accident_json_name)) as f:
            retrieved_json = json.load(f)
            accident_jsons[accident_json_name.split('.')[0].split()[1]] = retrieved_json['events']


def preprocess(description):
    description = description.lower()
    description = description.translate(str.maketrans(
        "", "", string.punctuation))
    description = description.strip()
    description = tuple(description.split())
    return description


def get_count_data(text_descriptions):
    returnable = list(map(preprocess, text_descriptions))
    returnable = {" ".join(set(k)): v for k, v in Counter(returnable).items()}
    return returnable


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
    repeated_dict = get_count_data(text_descriptions)
    repeated_dict = dict(sorted(repeated_dict.items(), key=lambda x: x[1]))
    print(len(repeated_dict))
    # for k, v in reversed(repeated_dict.items()):
    #     print(v, k)
