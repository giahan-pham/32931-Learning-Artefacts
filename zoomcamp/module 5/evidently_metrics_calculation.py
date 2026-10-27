"""Calculate Evidently metrics for every day of March 2024 and store them in PostgreSQL.

Grafana reads the table `taxi_metrics` to draw the dashboard.
Needs the Docker services running (`docker compose up -d`) and
`models/lin_reg.bin` + `data/reference.parquet` from homework5.ipynb.
"""
import datetime
import time

import joblib
import pandas as pd
import psycopg
from prefect import flow, task

from evidently import ColumnMapping
from evidently.report import Report
from evidently.metrics import (
    ColumnDriftMetric,
    ColumnQuantileMetric,
    DatasetDriftMetric,
    DatasetMissingValuesMetric,
)

SEND_TIMEOUT = 1  # seconds to wait between days, so you can watch Grafana fill in
CONNECTION = "host=localhost port=5432 user=postgres password=example"

BEGIN = datetime.datetime(2024, 3, 1)
N_DAYS = 31

NUM_FEATURES = ['passenger_count', 'trip_distance', 'fare_amount', 'total_amount']
CAT_FEATURES = ['PULocationID', 'DOLocationID']

CREATE_TABLE = """
drop table if exists taxi_metrics;
create table taxi_metrics(
    timestamp timestamp,
    prediction_drift float,
    num_drifted_columns integer,
    share_missing_values float,
    fare_amount_median float
)
"""

column_mapping = ColumnMapping(
    prediction='prediction',
    numerical_features=NUM_FEATURES,
    categorical_features=CAT_FEATURES,
    target=None,
)

report = Report(metrics=[
    ColumnDriftMetric(column_name='prediction'),
    DatasetDriftMetric(),
    DatasetMissingValuesMetric(),
    ColumnQuantileMetric(column_name='fare_amount', quantile=0.5),
])


@task
def prep_db():
    # wait for PostgreSQL: it needs a few seconds after `docker compose up`
    for _ in range(30):
        try:
            psycopg.connect(CONNECTION).close()
            break
        except psycopg.OperationalError:
            time.sleep(2)

    # create the database "test" if it doesn't exist, then (re)create the table
    with psycopg.connect(CONNECTION, autocommit=True) as conn:
        res = conn.execute("SELECT 1 FROM pg_database WHERE datname='test'")
        if len(res.fetchall()) == 0:
            conn.execute("create database test;")
    with psycopg.connect(CONNECTION + " dbname=test", autocommit=True) as conn:
        conn.execute(CREATE_TABLE)


@task
def calculate_metrics(day_data, reference_data, model, day):
    day_data = day_data.copy()
    day_data['prediction'] = model.predict(day_data[NUM_FEATURES + CAT_FEATURES].fillna(0))

    report.run(reference_data=reference_data, current_data=day_data, column_mapping=column_mapping)
    result = report.as_dict()

    return (
        day,
        result['metrics'][0]['result']['drift_score'],
        result['metrics'][1]['result']['number_of_drifted_columns'],
        result['metrics'][2]['result']['current']['share_of_missing_values'],
        result['metrics'][3]['result']['current']['value'],
    )


@task
def save_metrics(row):
    with psycopg.connect(CONNECTION + " dbname=test", autocommit=True) as conn:
        conn.execute(
            "insert into taxi_metrics(timestamp, prediction_drift, num_drifted_columns, "
            "share_missing_values, fare_amount_median) values (%s, %s, %s, %s, %s)",
            row,
        )


@flow(name='batch-monitoring')
def batch_monitoring():
    reference_data = pd.read_parquet('data/reference.parquet')
    model = joblib.load('models/lin_reg.bin')
    raw_data = pd.read_parquet('green_tripdata_2024-03.parquet')

    prep_db()
    for i in range(N_DAYS):
        start = BEGIN + datetime.timedelta(days=i)
        end = start + datetime.timedelta(days=1)
        day_data = raw_data[(raw_data.lpep_pickup_datetime >= start) & (raw_data.lpep_pickup_datetime < end)]

        row = calculate_metrics(day_data, reference_data, model, start)
        save_metrics(row)
        print(f'{start.date()}  fare_amount median = {row[4]}')
        time.sleep(SEND_TIMEOUT)


if __name__ == '__main__':
    batch_monitoring()
