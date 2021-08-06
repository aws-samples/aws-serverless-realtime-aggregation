# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import json
import base64
import hashlib
import time
import sys
import random
from decimal import Decimal

# AWS Import
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

if TRACK_PERFORMANCE:
    perf_tracker = PerformanceTrackerInitializer(True, INFLUX_CONNECTION_STRING, KIBANA_INSTANCE_IP)
    event_counter = EventsCounter(['stateless_map_lambda_batch_size', 'stateless_map_lambda_random_failures'])

# --------------------------------------------------------------------------------------------------
# Lambda Function
# --------------------------------------------------------------------------------------------------

def lambda_handler(event, context):
    
    # Print Status at Start
    records = event['Records']
    print('Invoked StatelessMapLambda with ' + str(len(records)) + ' record(s).')

    # Aggregate incoming messages (only over the leafs)
    delta = aggregate_over_kinesis_records(records)
    
    # If the batch contains only deletes: Done.
    if not delta:
        print('Skipped batch - no new entries.')
        return {'statusCode': 200}

    # Aggregate along the tree
    delta = aggregate_along_tree(delta)

    # Create Message
    message = json.dumps(delta, sort_keys = True)
    
    # Compute hash over all records
    message_hash = hashlib.sha256(str(records).encode()).hexdigest()

    # Write to DynamoDB
    ddb_ressource = boto3.resource(DYNAMO_NAME)
    table = ddb_ressource.Table(DELTA_TABLE_NAME)

    # We use a conditional put based on the hash of the record list to ensure
    # we're not accidentally writing one batch twice.
    try: 
        table.put_item(
            Item={
                'MessageHash': message_hash,
                'Message': message
                },
            ConditionExpression='attribute_not_exists(MessageHash)'
            )
    except ClientError as e:
        if e.response['Error']['Code']=='ConditionalCheckFailedException':   
            print('Conditional Put failed. Item with MessageHash ' + message_hash + \
                ' already exists.')
            print('Item:', message)
            print('Full Exception: ' + str(e) + '.')
        else:
            raise Exception(e)       
    
    # Manually Introduced Random Failure
    if random.uniform(0,100) < FAILURE_STATELESS_MAP_LAMBDA_PCT:

        if TRACK_PERFORMANCE:
            event_counter.increment('stateless_map_lambda_random_failures', 1)
            perf_tracker.add_metric_sample(None, event_counter, None, None)
            perf_tracker.submit_measurements()

        raise Exception('Manually Introduced Random Failure!')

    print('StatelessMapLambda finished. Aggregated ' + str(delta[MESSAGE_COUNT_NAME]) + \
        ' message(s) and written to DeltaTable. MessageHash: ' + message_hash + '.')
    
    # Performance Tracker
    if TRACK_PERFORMANCE:
        event_counter.increment('stateless_map_lambda_batch_size', len(records))
        perf_tracker.add_metric_sample(None, event_counter, None, None)
        perf_tracker.submit_measurements()

    return {'statusCode': 200}
