import os
import json
import pandas
import boto3
import requests
from datetime import datetime

def lambda_handler(event, context):
    
    # ==========================================================================
    # Arguments
    
    # Get current date for partitioning
    current_date = datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    
    # --------------------------------------------------------------------------
    # AWS S3 client
    s3_client = boto3.client('s3')
    
    # --------------------------------------------------------------------------
    # S3 bucket and keys
    bucket_name = 'projeto2-bucket'
    movies_csv_key = 'raw/local/CSV/Movies/2023/11/06/movies.csv'
    series_csv_key = 'raw/local/CSV/Series/2023/11/06/series.csv'
    default_api_movies_data_key = f"raw/TMDB/JSON/Movies/{year}/{month}/{day}/api_data_movies_"
    default_api_series_data_key = f"raw/TMDB/JSON/Series/{year}/{month}/{day}/api_data_series_"
    
    # --------------------------------------------------------------------------
    # TMDB keys
    TMDB_API_KEY = os.environ['api_key']
    TMDB_READ_ACCESS_TOKEN = os.environ['api_read_access_token']
    TMDB_API_ENDPOINT = 'https://api.themoviedb.org/3/movie/'
    
    # ==========================================================================
    # Reading CSVs
    
    # Function to download and read CSV files from S3
    def read_csv_from_s3(bucket_name, file_key):
        object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pandas.read_csv(object['Body'], sep='|')
        return df
    
    # --------------------------------------------------------------------------    
    # Read "movies.csv"
    movies_df = read_csv_from_s3(bucket_name, movies_csv_key)
    
    # Filter movies with Eric Roberts
    eric_roberts_movies = movies_df[movies_df['nomeArtista'] == 'Eric Roberts']
    movie_ids = eric_roberts_movies['id'].tolist()
    
    # --------------------------------------------------------------------------
    # Read "series.csv"
    series_df = read_csv_from_s3(bucket_name, series_csv_key)
    
    # Filter series with Eric Roberts
    eric_roberts_series = series_df[series_df['nomeArtista'] == 'Eric Roberts']
    series_ids = eric_roberts_series['id'].tolist()
    
    # ==========================================================================
    # Making API requests
    
    # Function to get movie details from API
    def get_api_details(item_id):
        response = requests.get(f'{TMDB_API_ENDPOINT}{item_id}?api_key={TMDB_API_KEY}')
        if response.status_code == 200:
            item_data = response.json()
            return item_data
        else:
            return None
    
    # --------------------------------------------------------------------------    
    # Fetch detailed movies information and save as JSON in a list
    api_movies_data_list = []
    
    for movie_id in movie_ids:
        api_data = get_api_details(movie_id)
        if api_data:
            api_movies_data_list.append(api_data)
    
    # --------------------------------------------------------------------------
    # Fetch detailed series information and save as JSON in a list
    api_series_data_list = []
    
    for series_id in series_ids:
        api_data = get_api_details(series_id)
        if api_data:
            api_series_data_list.append(api_data)
    
    # ==========================================================================
    # Group data by schema and number of registers
    
    # Function to group data by schema
    def group_records_by_schema(api_data):
        schema_grouped_data = {}
    
        for record in api_data:
            schema_key = tuple(sorted(record.keys()))
            if schema_key not in schema_grouped_data:
                schema_grouped_data[schema_key] = []
            schema_grouped_data[schema_key].append(record)
    
        return schema_grouped_data
    
    # --------------------------------------------------------------------------
    # Group movies data by schema
    schema_grouped_movies_api_data = group_records_by_schema(api_movies_data_list)
    
    # --------------------------------------------------------------------------
    # Group series data by schema
    schema_grouped_series_api_data = group_records_by_schema(api_series_data_list)
    
    # ==========================================================================
    # Saving data in the S3
    
    # Function to upload API data to S3
    def api_data_to_s3(data, bucket_name, file_key, s3_client):
    
        # Group movies data into chunks of 100 (or less) registers
        for schema, records in data.items():
            chunks = [records[i:i + 100] for i in range(0, len(records), 100)]
            
            # Save each chunk to S3
            for index, chunk in enumerate(chunks):
                updated_file_key = f"{file_key}{index+1}.json"
                json_data = json.dumps(chunk)
                s3_client.put_object(Body=json_data, Bucket=bucket_name, Key=updated_file_key)
                
    
    # --------------------------------------------------------------------------
    # Save movies
    api_data_to_s3(schema_grouped_movies_api_data, bucket_name, default_api_movies_data_key, s3_client)
    
    # --------------------------------------------------------------------------
    # Save series
    api_data_to_s3(schema_grouped_series_api_data, bucket_name, default_api_series_data_key, s3_client)
    
    # ==========================================================================
    # Get API data from S3    
    
    # Function to download and read JSON files from S3 and turn into CSV
    def read_json_from_s3(bucket_name, s3_client, file_key):
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            json_content = response['Body'].read().decode('utf-8')
            #api_data = json.loads(json_content)
            #return api_data
            return json_content

        except Exception as e:
            print(f"Error reading JSON from S3: {e}")
            return None
    
    return {
        'statusCode': 200,
        'body': f"Tudo OK."
    }
