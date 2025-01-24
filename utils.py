"""
Author: Shravan Aras <shravanars@arizona.edu>
Organization: University of Arizona

Description:
Utility function files that are shared across the whole Empatica ingestion head.
"""

import os
import awswrangler as wr
import boto3
import pandas as pd

def createTableWhitelist():
    """
    Looks for a local file called "whitelist.txt" that should contain the table names to ingest.
    Parses it with table name per line, and returns a list with those names.
    """
    if not os.path.isfile('whitelist.txt'):
        raise Exception('File whitelist.txt not found in local directory.')
    f = open('whitelist.txt', 'r')
    tables = []
    for line in f.readlines():
        if len(line) > 0:
            line = line.strip()
            tables.append(line)

    return tables

def uploadToAWS(df, s3_path, database, table_name, partition_key):
    """
    Method which uploads the dataframes to the AWS infrastructure.

    Parameters:
    df(pandas.DataFrame) : Pandas DataFrame to ingest.
    s3_path(str) : S3 URI where raw parquet files will be stored. eg: s3://bucket/object/
    database(str) : The AWS Glue database for this data
    table_name(str) : The AWS Glue table to this data
    partition_key(str) : Key on which to partition the data in Glue

    Returns:
    (bool, error message) : (true, None) on success and (false, err message) on failure.
    """
    # Create a table path that has the modality / table-name attached to it.
    table_path = f"{s3_path}/{table_name}/"
    if s3_path[-1] == '/': # we don't add additional / if s3_path variable had one at the end.
        table_path = f"{s3_path}{table_name}/"

    parquet_success = False
    err = None
    try:
        # Save the DataFrame to S3 in parquet format
        wr.s3.to_parquet(
            df=df,
            path=table_path,
            dataset=True,
            database=database,
            table=table_name,
            partition_cols=[partition_key]
        )
        parquet_success = True

    except Exception as e:
        err = f"ERROR : in writing parquet files: {repr(e)}"

    return parquet_success, err
