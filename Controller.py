import os
import sqlite3
import boto3
import pandas as pd
from Ingestor import ingestor
from pyarrow.parquet import ParquetDataset

# AWS Credentials
boto3.setup_default_session(profile_name='cb2')

def controller(directory, s3_path, database):

    for date_folder in os.listdir(directory):
        # Only consider date folders
        folder_path = os.path.join(directory, date_folder)
        if not os.path.isdir(folder_path):
            continue

        # Retrieve date 
        date = os.path.basename(date_folder)

        # If date has not been ingested
        if date not in ingested_dates:
            # Call Ingestor
            parquet_status, athena_table_status = ingestor(directory = directory + date + '/',
                                                            s3_path = 's3://cb2-yuanjea-development/dataset/empatica/',
                                                            database = 'cb2-yuanjea-development',
                                                            mode="append")
            print(parquet_status) 
            print(athena_table_status)
            # Check if ingestion is successful, add the date to ingested_dates
            if (parquet_status == 'Successfully written parquet files') and athena_table_status == ('Succesfully created/updated Athena table'):
                # Store date into a set
                ingested_dates.add(date)
                # Check if it is a folder
    print(ingested_dates)

if __name__ == "__main__":

    directory = 'empatica/empatica_raw_test/participant_data/'
    s3_path = 's3://cb2-yuanjea-development/dataset/empatica/'
    database = 'cb2-yuanjea-development'
    ingested_dates = set()

    controller(directory, s3_path, database)