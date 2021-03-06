# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import json
import hashlib
import random

# AWS Import
import boto3
from botocore.exceptions import ClientError

# Project Imports
import functions
import constants

if constants.TRACK_PERFORMANCE:
    from performance_tracker import EventsCounter, PerformanceTrackerInitializer

# --------------------------------------------------------------------------------------------------
# Initialize Performance Tracker
# --------------------------------------------------------------------------------------------------

if constants.TRACK_PERFORMANCE:
    perf_tracker = PerformanceTrackerInitializer(
            True, constants.INFLUX_CONNECTION_STRING, constants.GRAFANA_INSTANCE_IP
        )
    event_counter = EventsCounter(
            ['stateless_map_lambda_batch_size', 'stateless_map_lambda_random_failures']
        )

# --------------------------------------------------------------------------------------------------
# Lambda Function
# --------------------------------------------------------------------------------------------------

def lambda_handler(event, context):
    
    # Print Status at Start
    records = event['Records']
    print('Invoked StatelessMapLambda with ' + str(len(records)) + ' record(s).')

    # Aggregate incoming messages (only over the leafs)
    delta = functions.aggregate_over_kinesis_records(records)
    
    # If the batch contains only deletes: Done.
    if not delta:
        print('Skipped batch - no new entries.')
        return {'statusCode': 200}

    # Aggregate along the tree
    delta = functions.aggregate_along_tree(delta)

    # Create Message
    message = json.dumps(delta, sort_keys = True)
    
    # Compute hash over all records
    message_hash = hashlib.sha256(str(records).encode()).hexdigest()

    # Write to DynamoDB
    ddb_ressource = boto3.resource(constants.DYNAMO_NAME)
    table = ddb_ressource.Table(constants.DELTA_TABLE_NAME)

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
    if random.uniform(0,100) < constants.FAILURE_STATELESS_MAP_LAMBDA_PCT:

        if constants.TRACK_PERFORMANCE:
            event_counter.increment('stateless_map_lambda_random_failures', 1)
            perf_tracker.add_metric_sample(None, event_counter, None, None)
            perf_tracker.submit_measurements()

        raise Exception('Manually Introduced Random Failure!')

    print('StatelessMapLambda finished. Aggregated ' + str(delta[constants.MESSAGE_COUNT_NAME]) + \
        ' message(s) and written to DeltaTable. MessageHash: ' + message_hash + '.')
    
    # Performance Tracker
    if constants.TRACK_PERFORMANCE:
        event_counter.increment('stateless_map_lambda_batch_size', len(records))
        perf_tracker.add_metric_sample(None, event_counter, None, None)
        perf_tracker.submit_measurements()

    return {'statusCode': 200}
