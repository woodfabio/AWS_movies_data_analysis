import os
import pandas as pd
import boto3
from datetime import datetime
#from time import sleep

# Give a time to the volume to build
#sleep(10)

# Local keys path
python_dir = "/app/python/"
keys = "keys.txt"

# Function to read AWS credentials from "keys.txt" file
keys_file_path = os.path.join(python_dir, keys)

def read_aws_credentials():
    with open(keys_file_path, "r") as file:
        lines = file.readlines()
        access_key_id = lines[0].strip()
        secret_access_key = lines[1].strip()
        session_token = lines[2].strip()
    return access_key_id, secret_access_key, session_token

# Read AWS credentials from keys.txt file
access_key, secret_key, session_token = read_aws_credentials()

# Local files paths
input_dir = "/app/input/"

movies_file = "movies.csv"
movies_file_local_path = os.path.join(input_dir, movies_file)

series_file = "series.csv"
series_file_local_path = os.path.join(input_dir, series_file)

# Get current date for partitioning
current_date = datetime.now()
year = current_date.strftime("%Y")
month = current_date.strftime("%m")
day = current_date.strftime("%d")

# S3 bucket and keys
bucket_name = "projeto2-bucket"
s3_movies_key = f"raw/local/CSV/Movies/{year}/{month}/{day}/movies.csv"
s3_series_key = f"raw/local/CSV/Series/{year}/{month}/{day}/series.csv"


# AWS S3 client
s3_client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token)

# Upload local files to S3
try:
    s3_client.upload_file(movies_file_local_path, bucket_name, s3_movies_key)
    print(f"File uploaded successfully to s3://{bucket_name}/{s3_movies_key}")

    s3_client.upload_file(series_file_local_path, bucket_name, s3_series_key)
    print(f"File uploaded successfully to s3://{bucket_name}/{s3_series_key}")

except Exception as e:
    print(f"Error: {e}")
