# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import json
import base64
import random
from decimal import Decimal

# AWS Imports
import boto3
from botocore.exceptions import ClientError

# Project Imports
from functions import *
from constants import *

if TRACK_PERFORMANCE:
    from performance_tracker import EventsCounter, PerformanceTrackerInitializer

# --------------------------------------------------------------------------------------------------
# Initialize Performance Tracker
# --------------------------------------------------------------------------------------------------

# Performance Tracker
if TRACK_PERFORMANCE:
    perf_tracker = PerformanceTrackerInitializer(True, INFLUX_CONNECTION_STRING, KIBANA_INSTANCE_IP)
    event_counter = EventsCounter(['state_lambda_batch_size', 'state_lambda_random_failures'])

# --------------------------------------------------------------------------------------------------
# Lambda Function
# --------------------------------------------------------------------------------------------------

def lambda_handler(event, context):
    
    # Print Status at Start
    records = event['Records']
    print('Invoked StateLambda with ' + str(len(records)) + ' record(s).')

    # Initialize DynamoDB
    ddb_ressource = boto3.resource(DYNAMO_NAME)
    table = ddb_ressource.Table(STATE_TABLE_NAME)
    
    # Loop over records
    for record in records:

        # Load Record
        data = json.loads(base64.b64decode(record[KINESIS_NAME]['data']).decode('utf-8'))

        # Get Entries
        record_id           = data[ID_COLUMN_NAME]
        record_hierarchy    = data[HIERARCHY_COLUMN_NAME]
        record_value        = data[VALUE_COLUMN_NAME]
        record_version      = data[VERSION_COLUMN_NAME]
        record_time         = data[TIMESTAMP_COLUMN_NAME]
        
        # Manually Introduced Random Failure
        if random.uniform(0,100) < FAILURE_STATE_LAMBDA_PCT / len(records):
            
            # Submit measurements
            if TRACK_PERFORMANCE:
                event_counter.increment('state_lambda_random_failures', 1)
                perf_tracker.add_metric_sample(None, event_counter, None, None)
                perf_tracker.submit_measurements()
                
            # Raise exception
            raise Exception('Manually Introduced Random Failure!')

        # Write to DDB
        # --> We use a conditional update item to ensure we always have the most recent version
        try:
            table.update_item(
                Key = {
                    STATE_TABLE_KEY: record_id
                    },
                UpdateExpression = 'SET  #VALUE     = :new_value,' + \
                                        '#VERSION   = :new_version,' + \
                                        '#HIERARCHY = :new_hierarchy,' + \
                                        '#TIMESTAMP = :new_time',
                ConditionExpression = 'attribute_not_exists(' + ID_COLUMN_NAME + ') OR ' + VERSION_COLUMN_NAME + '< :new_version',
                ExpressionAttributeNames={
                    '#VALUE':       VALUE_COLUMN_NAME,
                    '#VERSION':     VERSION_COLUMN_NAME,
                    '#HIERARCHY':   HIERARCHY_COLUMN_NAME,
                    '#TIMESTAMP':   TIMESTAMP_COLUMN_NAME
                    },
                ExpressionAttributeValues={
                    ':new_version':     record_version,
                    ':new_value':       Decimal(str(record_value)),
                    ':new_hierarchy':   json.dumps(record_hierarchy, sort_keys = True),
                    ':new_time':        Decimal(str(record_time))
                    },
                )
        except ClientError as e:
            if e.response['Error']['Code']=='ConditionalCheckFailedException':  
                print('Conditional put failed.' + \
                    ' This is either a duplicate or a more recent version already arrived.')
                print('Id: ',           record_id)
                print('Hierarchy: ',    record_hierarchy)
                print('Value: ',        record_value)
                print('Version: ',      record_version)
                print('Timestamp: ',    record_time)
            else:
                raise Exception(e)
            
    # Submit measurements
    if TRACK_PERFORMANCE:
        event_counter.increment('state_lambda_batch_size', len(records))
        perf_tracker.add_metric_sample(None, event_counter, None, None)
        perf_tracker.submit_measurements()

    # Print Status at End
    print('StateLambda successfully processed ' + str(len(records)) + ' record(s).')

    return {'statusCode': 200}