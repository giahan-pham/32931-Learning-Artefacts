import os
import sys
from datetime import datetime

import pandas as pd

# Localstack doesn't support the checksums that new boto3 versions send by default
os.environ.setdefault('AWS_REQUEST_CHECKSUM_CALCULATION', 'when_required')
os.environ.setdefault('AWS_RESPONSE_CHECKSUM_VALIDATION', 'when_required')
os.environ.setdefault('AWS_ACCESS_KEY_ID', 'test')
os.environ.setdefault('AWS_SECRET_ACCESS_KEY', 'test')
os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
options = {'client_kwargs': {'endpoint_url': S3_ENDPOINT_URL}}

INPUT_FILE_PATTERN = os.getenv('INPUT_FILE_PATTERN', 's3://nyc-duration/in/{year:04d}-{month:02d}.parquet')
OUTPUT_FILE_PATTERN = os.getenv('OUTPUT_FILE_PATTERN', 's3://nyc-duration/out/{year:04d}-{month:02d}.parquet')


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


# Q5: create the fake January 2023 input and upload it to Localstack
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
]
columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

input_file = INPUT_FILE_PATTERN.format(year=2023, month=1)
df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

# Q6: run batch.py for January 2023 and check its output
env = (
    f'S3_ENDPOINT_URL={S3_ENDPOINT_URL} '
    f'INPUT_FILE_PATTERN="{INPUT_FILE_PATTERN}" '
    f'OUTPUT_FILE_PATTERN="{OUTPUT_FILE_PATTERN}"'
)
exit_code = os.system(f'{env} "{sys.executable}" batch.py 2023 1')
assert exit_code == 0

output_file = OUTPUT_FILE_PATTERN.format(year=2023, month=1)
df_result = pd.read_parquet(output_file, storage_options=options)
print(df_result)
print('sum of predicted durations:', df_result.predicted_duration.sum())
