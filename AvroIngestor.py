"""
Author: Shravan Aras <shravanars@arizona.edu>
Organization: University of Arizona

Description:
This code is written to ingest data from Empatica Avro files into
sensorfabric. These files contain some really good raw low level
data.
"""

import sys
import os
import pandas as pd
import re
from fastavro import reader, parse_schema, json_reader
import numpy as np
from datetime import datetime
import boto3
import awswrangler as wr
from utils import createTableWhitelist, uploadToAWS

def _raw_ibi(record : dict) -> pd.DataFrame:
    """
    Method which will process raw IBI values from the record.

    Parameters:
    record(dict) : An AVRO record that contains data to be parsed.

    Returns:
    frame : Pandas dataframe containing calculated biometrics.
    """
    values = record['rawData']['systolicPeaks']['peaksTimeNanos']
    array = np.array(values)
    timezone = record['timezone']
    # Timestamp in seconds.
    timestamps = array[1:]
    # Convert the absolute timestamps of peaks into relative IBI values.
    dates = [datetime.fromtimestamp(t / (10**9)) for t in timestamps]
    local_dates = [datetime.fromtimestamp((t / (10**9))+timezone) for t in timestamps]
    array = array[1:] - array[:array.size-1]
    # IBI values are usually represented in ms.
    array = array / (10**6)
    bpm = (60 * 1000) / array
    # Create a quick pandas framework using this.
    frame = pd.DataFrame({'timestamp_ns':timestamps,
                        'datetime_utc':dates,
                        'datetime':local_dates,
                        'ibi_ms':array,
                        'hr':bpm})

    return frame

"""
Table names for the various raw biometrics returned by the device.
Dictionary Format :
{
    internal_key : {
        name : table name in database,
        func : biometric specific ingestion function call
    }
}
"""
TABLE_NAMES = dict(
    ibi = dict(
        name = 'raw_ibi',
        func = _raw_ibi
    )
)

def ingestor(directory, s3_path, database, mode='append'):
    """
    Method which parses the directories to extract the AVRO files, upload them to S3
    and then create the appropriate glue DB tables.

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

    avro_pattern = re.compile(r'\.avro$')
    for folder in os.listdir(directory):
        folder_path = os.path.join(directory, folder)
        # Check if it is a folder
        if os.path.isdir(folder_path):
            # Isolate the participant Id and Device Id
            participant_id, device_id = os.path.basename(folder).split('-')

            # Go into raw_data/v6/ to read all the avro files from.
            file_path = os.path.join(folder_path, 'raw_data/v6/')
            if not os.path.exists(file_path):
                #TODO: Add this to the logs and move on.
                print(f"Unable to find the path {file_path}")
                continue

            # There are going to be multiple small files in here and we need to parse each of them.
            success_count = 0
            # We will reduce the network requests by combining all the frames in a directory and pushing them up
            # all at once.
            master_buffer = dict()
            for filename in os.listdir(file_path):
                if not avro_pattern.search(filename):
                    continue

                master = parse_avro(os.path.join(file_path, filename), table_whitelist, device_id)
                # Concat it with the master buffer that we have.
                for tbl_name in master:
                    if not tbl_name in master_buffer:
                        master_buffer[tbl_name] = pd.DataFrame()
                    temp_frame = master_buffer[tbl_name]
                    temp_frame = pd.concat([temp_frame, master[tbl_name]])
                    master_buffer[tbl_name] = temp_frame

            # Upload the complete master frame to AWS.
            for tbl_name in master.keys():
                # Upload it all to AWS
                status, err = uploadToAWS(master[tbl_name],
                            s3_path,
                            database,
                            tbl_name,
                            'participant_full_id')

                if status:
                    print('Success : Written file {} into table {}'
                            .format(os.path.join(file_path, filename), tbl_name))
                else:
                    print('ERROR : Written file {} into table {}'
                            .format(os.path.join(file_path, filename), tbl_name))
                    print(err)

def parse_avro(filepath : str, table_whitelist : list, device_id : str):
    """
    Stores the master list of the tables created in the following format as a dict() object.
    master = {
        'tbl_name1' : pd.DataFrame(),
        'tbl_name2' : pd.DataFrame(),
         ...
    }
    """
    master = dict()
    with open(filepath, 'rb') as f:
        for record in reader(f):
            participant_full_id = gen_full_participant_id(record)

            # Iterate through all the biometrics to see which ones we need to upload.
            for tbl_key in TABLE_NAMES.keys():
                table_name = TABLE_NAMES[tbl_key]['name']
                if table_name in table_whitelist:
                    if not table_name in master:
                        # Initialize an empty frame for this table if it has not already been created.
                        master[table_name] = pd.DataFrame()

                    assert(table_name in master)

                    # We need to extact the timezone offset from the record.
                    timezone = None
                    algoVersion = None
                    if 'timezone' in record:
                        timezone = record['timezone']
                    if 'algoVersion' in record:
                        algoVersion = record['algoVersion']

                    masterFrame = master[table_name]
                    # Use a function pointer to call respective function for this table.
                    frame = TABLE_NAMES[tbl_key]['func'](record)
                    # Append the full participant id
                    frame['participant_full_id'] = participant_full_id
                    # Append the device id to each record as well.
                    frame['device_id'] = device_id
                    # Append the timezone to the frame so we can have that for every entry.
                    frame['timezone'] = timezone
                    # Just storing the JSON string for algoVersion as is right now.
                    frame['algoversion'] = f"{algoVersion}"
                    masterFrame = pd.concat([masterFrame, frame], axis=0, ignore_index=True)
                    master[table_name] = masterFrame

    return master

def gen_full_participant_id(record : dict):
    """
    Method which generates a full participant ID from the enrollment record in the AVRO file.
    """
    participant_info = record['enrollment']
    participant_full_id = "{}-{}-{}-{}".format(participant_info['organizationID'],
                                            participant_info['siteID'],
                                            participant_info['studyID'],
                                            participant_info['participantID'])

    return participant_full_id

if __name__ == '__main__':
    directory_path = sys.argv[1]
    ingestor(directory_path, 's3://togetherya-test/empatica/', 'togetherya-test')
