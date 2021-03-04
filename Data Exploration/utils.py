import pandas as pd


def format_date(event_time):
    event_time = event_time.split("/")
    event_time[-1] = "20" + event_time[-1]
    event_time = ("/").join(event_time)
    return pd.to_datetime(event_time, format="%d/%m/%Y %H:%M")
