import pandas as pd
import awswrangler as wr
import boto3
import os
from utils import createTableWhitelist, uploadToAWS

"""
Pandas to Hadoop datatype mapper.
---------------------------------
The hadoop data type names are different from the pandas datatype names.
This map does a "best-effort" job to provide a conversion for some of them.
Add more to these as we see newer datatypes.
"""
dtype_mapper = {
    'integer': 'bigint',
    'floating': 'double',
    'datetime64': 'timestamp',
}

def ingestor(directory, s3_path, database, mode='append'):
    """
    Function to correctly parse the directory structure, extract the data from the CSV files and then upload them to the correct AWS table.

    Parameters:
    directory(str) - Top level date folder path
    s3_path (str) - S3 path where the parquet files will be stored
    database (str) - AWS Glue/Athena database name
    mode (str) - (Default) "append" to keep any possible existing table or  "overwrite" to recreate any possible existing table
    """
    # Initialize variables to track parquet files writing status
    parquet_success, parquet_status = False , ""
    # Initialize variable to track Athena table creation/update status
    athena_table_success, athena_table_status = False, ""

    # The names of tables to ingest.
    table_whitelist = createTableWhitelist()

    for folder in os.listdir(directory):
        folder_path = os.path.join(directory, folder)
        # Check if it is a folder
        if os.path.isdir(folder_path):
            # Isolate the participant Id and Device Id
            participant_id, device_id = os.path.basename(folder).split('-')

            # Go into digital_biomarkers/aggregated_per_minute
            file_path = os.path.join(folder_path, 'digital_biomarkers/aggregated_per_minute/')
            if not os.path.exists(file_path):
                #TODO: Add this to the logs and move on.
                print(f"Unable to find the path {file_path}")
                continue
            
            for file in os.listdir(file_path):
                # Read CSV file
                df = pd.read_csv(file_path+file)

                # Attach a new column to the data from this file called device_id. Fill this in with the device ID you stored previously.
                # device_id at every row in this column?
                df['device_id'] = device_id
                
                # Check and convert timestamp_iso column to datetime64
                if (df['timestamp_iso'].dtype != 'datetime64'):
                    df['timestamp_iso'] = pd.to_datetime(df['timestamp_iso'])

                # Convert the data type of the timestamp_unix column to bigint
                df['timestamp_unix'] = df['timestamp_unix'].astype('int64')

                # Extract the table name from the filename
                table_name = file.split('_')[-1].split('.')[0]
                # Replace dash with underscore
                table_name = table_name.replace('-', '_')

                # Table whitelist check.
                if not table_name in table_whitelist:
                    continue

                # Upload it all to AWS
                status, err = uploadToAWS(df,
                            s3_path,
                            database,
                            table_name,
                            'participant_full_id')

                if status:
                    print('Success : Written file {} into table {}'
                            .format(os.path.join(file_path, filename), table_name))
                else:
                    print('ERROR : Written file {} into table {}'
                            .format(os.path.join(file_path, filename), table_name))
                    print(err)

    return parquet_status, athena_table_status
