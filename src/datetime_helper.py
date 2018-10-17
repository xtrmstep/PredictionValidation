import datetime as dt
import pandas as pd


def to_date_parts(str_date):
    date = dt.datetime.strptime(str_date.split(".")[0], '%Y-%m-%d %H:%M:%S')
    dt_year = date.year
    dt_month = date.month
    dt_day = date.day
    dt_day_of_year = date.toordinal() - dt.datetime(date.year, 1, 1).toordinal() + 1
    dt_day_of_week = date.weekday()
    dt_hour = date.hour
    return pd.Series([dt_year, dt_month, dt_day, dt_day_of_year, dt_day_of_week, dt_hour])

