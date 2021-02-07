import string
import json
import os
from collections import Counter

relative_uri_accidents_records = os.path.join("..", "accidents_record", "logs")
accident_jsons = {}


def populate_accidents_dict():
    for accident_json_name in os.listdir(relative_uri_accidents_records):
        with open(os.path.join(relative_uri_accidents_records, accident_json_name)) as f:
            retrieved_json = json.load(f)
            accident_jsons[accident_json_name.split(
                '.')[0].split()[1]] = retrieved_json['events']


def populate_accidents_list():
    accident_list = []
    for accident_json_name in os.listdir(relative_uri_accidents_records):
        with open(
            os.path.join(relative_uri_accidents_records, accident_json_name)
        ) as f:
            retrieved_json = json.load(f)
            for events in retrieved_json["events"]:
                has_train_no = False
                for desc in events["event_descriptions"]:
                    if desc["Train No"] != "":
                        has_train_no = True
                if has_train_no:
                    accident_list.append(events["Fault Description"])
    return accident_list


if __name__ == "__main__":
    # accident_list = populate_accidents_list()

    # # lower case
    # accident_list = list(map(
    #     lambda x: x.lower(), accident_list))

    # # remove punctuation
    # accident_list = list(map(
    #     lambda x: x.translate(str.maketrans(
    #         "", "", string.punctuation)), accident_list))

    # # trim
    # accident_list = list(map(
    #     lambda x: " ".join(x.split()), accident_list))

    # accident_list = sorted([f"{v}: {k}"for k, v in Counter(accident_list).items()], key =lambda x: x.split(":")[1])

    # with open("temp.txt", "w") as f:
    #     for accident in accident_list:
    #         f.write(f"{accident}\n")
    with open("classification.json", "r") as f:
        classification = json.load(f)
        total = 0
        for incident in classification:
            total += incident["total"]
        print(total)
