import pandas as pd
import awswrangler as wr
import boto3
import os

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

                # Convert the data type of the timestamp_unix column to bigint #############################
                df['timestamp_unix'] = df['timestamp_unix'].astype('int64')

                # Extract the table name from the filename
                table_name = file.split('_')[-1].split('.')[0]
                # Replace dash with underscore
                table_name = table_name.replace('-', '_')
                try:
                    # Save the DataFrame to S3 in parquet format
                    wr.s3.to_parquet(
                        df=df,
                        path=s3_path,
                        dataset=True,
                        database=database,
                        table=table_name,
                        partition_cols=['participant_full_id']
                    )
                    parquet_success = True
                    parquet_status = "Successfully written parquet files"

                except Exception as e:
                    parquet_status = f"Error in writing parquet files: {repr(e)}"

                # Check if table already exists and parquet files were successfully written
                if not wr.catalog.table(database, table_name).empty and parquet_success:
                    try:
                        # Dynmaically infer column types from csv_file
                        columns_types = {col: pd.api.types.infer_dtype(df[col]) for col in df.columns}
                        
                        # Data from each .csv file in there will be added to its own Glue table
                        # Create a parquet table in AWS Glue Catalog
                        wr.catalog.create_parquet_table(
                            database=database,
                            table=table_name,
                            path=s3_path,
                            columns_types=columns_types,
                            mode=mode
                        )
                        athena_table_success = True
                        athena_table_status = "Succesfully created/updated Athena table"

                    except Exception as e:
                        athena_table_status = f"Error in creating/updating Athena table: {repr(e)}"
                else:
                    athena_table_status = "Athena table not created/updated"

    return parquet_status, athena_table_status
