import os
import sqlite3
import boto3
import pandas as pd
from Ingestor import ingestor
from pyarrow.parquet import ParquetDataset
import argparse
import time
from datetime import datetime

def controller(directory, s3_path, database, last_sync_date):

    # See if we have a sync date, and if we do then parse it into a datetime format.
    format = '%Y-%m-%d'
    if last_sync_date is not None:
        last_sync_date = datetime.strptime(last_sync_date, format)
    else:  # Set some sensible defaults if the last sync data has not been passed.
        last_sync_date = datetime.strptime('1990-12-01', format)

    for date_folder in os.listdir(directory):
        # Only consider date folders
        folder_path = os.path.join(directory, date_folder)
        if not os.path.isdir(folder_path):
            continue

        # Convert the folder name into date object if we are able to.
        try:
            date_folder_dt = datetime.strptime(date_folder, format)
        except ValueError:
            print(f"Skipping '{date_folder}' as it cannot be converted to date object")
            continue

        # If date has not been ingested (or if the last sync date is just None then we don't check it.)
        if date_folder_dt > last_sync_date:
            print(f"{date_folder} Marked for ingestion.")
            # Call Ingestor
            start = time.time()
            parquet_status, athena_table_status = ingestor(folder_path, s3_path, database, "append")
            end = time.time()
            print(date_folder, end-start, parquet_status, athena_table_status)
            # Check if ingestion is successful, add the date to ingested_dates
            if (parquet_status == 'Successfully written parquet files') and athena_table_status == ('Succesfully created/updated Athena table'):
                # Store date into a set
                ingested_dates.add(date_folder)
                # Check if it is a folder

if __name__ == "__main__":

    cmdparser = argparse.ArgumentParser(description='Controller to ingest empatica files.')
    cmdparser.add_argument('-d', '--directory', type=str, help='Path to the participant_data/ directory', required=True)
    cmdparser.add_argument('-p', '--s3_path', type=str, help='Path to where the parquet files will be uploaded in S3', required=True)
    cmdparser.add_argument('-db', '--database', type=str, help='AWS Glue database name', required=True)
    cmdparser.add_argument('-s', '--last_sync_date', type=str, help='Enter the last date (YYYY-MM-DD) till which the controller has uploaded data to remote S3. If not provided everything is uploaded to AWS If not provided everything is uploaded to AWS.', required=False)
    args = cmdparser.parse_args()

    directory = args.directory
    s3_path = args.s3_path
    database = args.database
    last_sync_date = args.last_sync_date

    """
    The ingestor assumes that there is a local file named "whitelist.txt" that contains
    the names of the tables / metrics that need to be ingested. One table name per line.
    If this file is not there, we will just self-destruct here.
    """
    if not os.path.isfile('whitelist.txt'):
        raise Exception('Cannot find whitelist.txt in the local directory.')

    controller(directory, s3_path, database, last_sync_date)
