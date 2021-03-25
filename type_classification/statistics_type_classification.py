import os

import matplotlib.pyplot as plt
import numpy as np

from utils import *

classification_average_percentile_dict = {}

configuration = {"relative_uri_SICP": os.path.join("..", "SICP", "incident"),
                 "relative_uri_accumulation": os.path.join("..", "SICP", "incident", "accumulated_incidents.csv"),
                 "debug": True}

removable_classes = {'TS', 'RS/PUB', 'S&T/RS', 'SI', 'PUB/RS', 'RS/TBD/S&T', 'PUB/EXT', 'RS/TBD', 'RS/TS/TBD',
                     'S&T/TBD/RS', 'PD', 'OCC/PUB/TS', 'PSD/PUB', 'PUB/RS/PW', 'PSD/S&T/TBD', 'PUB/S&T'}

def clean_df(df):
    return df[df['incident']]


def read_csv(uri):
    return pd.read_csv(uri)


def draw_bar_plot(min, max, avg):
    labels = []
    minimums = []
    averages = []
    maximums = []

    for key, value in dict(sorted(avg.items(), key=lambda x: x[1])).items():
        labels.append(key)
        minimums.append(min[key])
        maximums.append(max[key])
        averages.append(value)

    x = np.arange(len(labels))  # the label locations
    width = 0.2  # the width of the bars

    fig, ax = plt.subplots(figsize=(16, 8))
    min_rects = ax.bar(x - width, minimums, width, label='Minimum')
    average_rects = ax.bar(x, averages, width, label='Average')
    max_rects = ax.bar(x + width, maximums, width, label='Maximum')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_title('Percentile of travelling times for incidents against incident class, sorted by average')
    ax.set_ylabel('Percentile of travelling times')
    ax.set_xlabel('MTR\'s incident classification')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(min_rects)
    autolabel(average_rects)
    autolabel(max_rects)

    fig.tight_layout()
    plt.show()


def limit_float(value):
    return round(value)


def preprocess(preprocessable):
    # Removing unnecessary keys
    for key in removable_classes:
        if key in preprocessable:
            preprocessable.pop(key)

    preprocessable = {k: limit_float(v) for k, v in preprocessable.items()}

    return preprocessable


def analyse(df):
    grouped_by_fault_classification = df.groupby("fault_classification")
    fault_classification_quantile_min = dict(grouped_by_fault_classification['quantile'].min())
    fault_classification_quantile_max = dict(grouped_by_fault_classification['quantile'].max())
    fault_classification_quantile_average = dict(grouped_by_fault_classification['quantile'].mean())
    fault_classification_quantile_min = preprocess(fault_classification_quantile_min)
    fault_classification_quantile_average = preprocess(fault_classification_quantile_average)
    fault_classification_quantile_max = preprocess(fault_classification_quantile_max)
    print(fault_classification_quantile_min)
    print(fault_classification_quantile_max)
    print(fault_classification_quantile_average)
    draw_bar_plot(fault_classification_quantile_min, fault_classification_quantile_max,
                  fault_classification_quantile_average)


def accumulate_dataframes():
    # We will accumulate dataframes to get combined statistics
    dataframes = []
    # Iterates through every SICP  file
    for dir_name in os.listdir(configuration["relative_uri_SICP"]):
        relative_uri_csv = configuration["relative_uri_SICP"] + "/" + dir_name
        dataframe_to_analyse = read_csv(relative_uri_csv)
        dataframe_to_analyse = clean_df(dataframe_to_analyse)
        dataframes.append(dataframe_to_analyse)
    accumulated_df = pd.concat(dataframes, ignore_index=True)
    accumulated_df.to_csv(configuration['relative_uri_accumulation'])
    return accumulated_df


def import_accumulated_incidents():
    return pd.read_csv(configuration['relative_uri_accumulation'])


if __name__ == "__main__":
    # accumulate_dataframes()
    accumulated_df = import_accumulated_incidents()
    analyse(accumulated_df)
