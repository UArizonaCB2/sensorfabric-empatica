import os
import sqlite3
import boto3
import pandas as pd
from Ingestor import ingestor
from pyarrow.parquet import ParquetDataset
import argparse
import time

def controller(directory, s3_path, database):

    for date_folder in os.listdir(directory):
        # Only consider date folders
        folder_path = os.path.join(directory, date_folder)
        if not os.path.isdir(folder_path):
            continue

        # If date has not been ingested
        if date_folder not in ingested_dates:
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
    cmdparser.add_argument('directory', type=str, help='Path to the participant_data/ directory')
    cmdparser.add_argument('s3_path', type=str, help='Path to where the parquet files will be uploaded in S3')
    cmdparser.add_argument('database', type=str, help='AWS Glue database name')
    args = cmdparser.parse_args()

    directory = args.directory
    s3_path = args.s3_path
    database = args.database
    ingested_dates = set()

    """
    The ingestor assumes that there is a local file named "whitelist.txt" that contains
    the names of the tables / metrics that need to be ingested. One table name per line.
    If this file is not there, we will just self-destruct here.
    """
    if not os.path.isfile('whitelist.txt'):
        raise Exception('Cannot find whitelist.txt in the local directory.')

    controller(directory, s3_path, database)
