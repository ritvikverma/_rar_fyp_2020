import os

import matplotlib.pyplot as plt
import numpy as np

from utils import *

plt.figure(figsize=(20, 3))  # width:20, height:3
classification_average_percentile_dict = {}

configuration = {"relative_uri_SICP": os.path.join("..", "SICP", "incident"),
                 "relative_uri_accumulation": os.path.join("..", "SICP", "incident", "accumulated_incidents.csv"),
                 "debug": True}


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
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width / 2, minimums, width, label='Minimums')
    rects2 = ax.bar(x + width / 2, maximums, width, label='Maximums')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Percentile of travelling times')
    ax.set_title('Scores by group and gender')
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

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    plt.show()


def analyse(df):
    grouped_by_fault_classification = df.groupby("fault_classification")
    fault_classification_quantile_min = dict(grouped_by_fault_classification['quantile'].min())
    fault_classification_quantile_max = dict(grouped_by_fault_classification['quantile'].max())
    fault_classification_quantile_average = dict(grouped_by_fault_classification['quantile'].mean())
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
    accumulated_df = import_accumulated_incidents()
    analyse(accumulated_df)
