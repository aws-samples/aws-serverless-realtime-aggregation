# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import random
import json
import base64

# Project Imports
from constants import *

# --------------------------------------------------------------------------------------------------
# Generic Helper Functions
# --------------------------------------------------------------------------------------------------

# Add a value to an entry in a dictionary, create the entry if necessary
def dict_entry_add(dictionary, key, value):
    if key in dictionary:
        dictionary[key] += value
    else:
        dictionary[key] = value
        
# Add a value to an entry in a dictionary, create the entry if necessary
def dict_entry_min(dictionary, key, value):
    if (key not in dictionary) or (value < dictionary[key]):
        dictionary[key] = value
    
# Print Progress Bar
def print_progress_bar(progress, speed = None):
    progress_bar = int(progress / 2) + 1
    
    if not speed:
        print('\r[' + '#' * progress_bar + ' ' * (50 - progress_bar) +
            '] {:.1f}% completed.'.format(progress), end = '')
    else:
        print('\r[' + '#' * progress_bar + ' ' * (50 - progress_bar) +
            '] {:.1f}% completed. Speed: {:.1f} Messages / Second'.format(
            progress, speed), end = '')

# --------------------------------------------------------------------------------------------------
# AWS Helper Functions
# --------------------------------------------------------------------------------------------------

# Get item from a DynamoDB Table, if it exists, return None otherwise
def get_item_ddb(table, key_name, strong_consistency = False):
    
    # Call to DynamoDB
    if strong_consistency:
        response = table.get_item(Key=key_name,ConsistentRead = True)
    else:
        response = table.get_item(Key=key_name)
    
    # Return item if found
    if 'Item' in response:
        item = response['Item']
    else:
        item = None
   
    return item

# Count number of items in DynamoDB Table
def count_items(table):
    
    item_count = 0
    scan_args = dict()
    done = False
    start_key = None
    i = 0
    
    while not done:
        # Scan
        if start_key:
            scan_args['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_args)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    
        # Get Items
        items = response.get('Items', [])
        item_count += len(items)
    
    return item_count

# --------------------------------------------------------------------------------------------------
# Aggregation & Generator Helper Functions
# --------------------------------------------------------------------------------------------------

# Random Value
def random_value():
    value = random.randint(100 * MIN_VALUE_OF_RISK, 100 * MAX_VALUE_OF_RISK) / 100
    return value

# Random Type Constructor
def random_hierarchy():
    hierarchy = dict()
    for k,v in HIERARCHY_DEFINITION.items():
        hierarchy[k] = random.choice(v)
    return hierarchy

# Convert a Hierarchy Dictionary to a Type String, based on Aggregation Hierarchy
def hierarchy_to_string (hierarchy_dictionary, aggregation_hierarchy):
    type_string = ''
    start = True
    for level in aggregation_hierarchy:
        if start:
            start = False
        else:
            type_string += ":"
            
        type_string += hierarchy_dictionary[level]
    return type_string

# Aggregate along tree
def aggregate_along_tree(data):
    
    # Determine aggregation depth
    aggregation_depth = max([key.count(':') for key in data.keys()])
    
    # Start at max depth and go higher 
    for depth in range(aggregation_depth, 0, -1):
        children = [key for key in data.keys() if key.count(':') == depth]
        for child in children:
            parent = child[:child.rfind(':')]
            dict_entry_add(data, parent, data[child])
                
    return data

# Aggregate over records from a DynamoDB Stream (Stateful Pipeline)
def aggregate_over_dynamo_records(records):

    # Initialize Delta Dict
    delta = dict()

     # Iterate over Messages
    for record in records:

        # If the record doesn't contain new data: Skip
        if 'NewImage' not in record[DYNAMO_NAME]:
            continue

        # Add New Image to Aggregate
        new_data = record[DYNAMO_NAME]['NewImage']

        new_hierarchy       = json.loads(   new_data[HIERARCHY_COLUMN_NAME]['S'] )
        new_value           = float(        new_data[VALUE_COLUMN_NAME]['N']     )
        new_generated_time  = float(        new_data[TIMESTAMP_COLUMN_NAME]['N'] )
        
        # Add to Value for the New Type
        new_type = hierarchy_to_string(new_hierarchy, AGGREGATION_HIERARCHY)
        dict_entry_add(delta, new_type, new_value)
        
        # Times
        dict_entry_add(delta, TIMESTAMP_GENERATOR_MEAN, new_generated_time)
        dict_entry_min(delta, TIMESTAMP_GENERATOR_FIRST, new_generated_time)
            
        # If the record contains old data: Delete from Aggregate
        if 'OldImage' in record[DYNAMO_NAME]:
            old_data = record[DYNAMO_NAME]['OldImage']

            old_hierarchy   = json.loads(   old_data[HIERARCHY_COLUMN_NAME]['S'] )
            old_value       = float(        old_data[VALUE_COLUMN_NAME]['N']     )

            # Subtract from Value for the Old Type
            old_type = hierarchy_to_string(old_hierarchy, AGGREGATION_HIERARCHY)
            dict_entry_add(delta, old_type, - old_value)

        # Increment mesage count
        dict_entry_add(delta, MESSAGE_COUNT_NAME, 1)
        
    # Adjust timestamp mean by number of messages
    if delta:
        delta[TIMESTAMP_GENERATOR_MEAN] /= delta[MESSAGE_COUNT_NAME]

    return delta

# Aggregate over records from a Kinesis Stream (Stateless Pipeline)
def aggregate_over_kinesis_records(records):

    # Initialize Delta Dict
    delta = dict()

     # Iterate over Messages
    for record in records:

        data = json.loads(base64.b64decode(record[KINESIS_NAME]['data']).decode('utf-8'))

        # Get Relevant Data
        record_hierarchy    = data[HIERARCHY_COLUMN_NAME]
        record_value        = data[VALUE_COLUMN_NAME]
        record_time         = data[TIMESTAMP_COLUMN_NAME]
        
        # Add to Value for the New Type
        record_type = hierarchy_to_string(record_hierarchy, AGGREGATION_HIERARCHY)
        dict_entry_add(delta, record_type, record_value)
        
        # Times
        dict_entry_add(delta, TIMESTAMP_GENERATOR_MEAN, record_time)
        dict_entry_min(delta, TIMESTAMP_GENERATOR_FIRST, record_time)
            
        # Increment mesage count
        dict_entry_add(delta, MESSAGE_COUNT_NAME, 1)
        
    # Adjust timestamp mean by number of messages
    if delta:
        delta[TIMESTAMP_GENERATOR_MEAN] /= delta[MESSAGE_COUNT_NAME]

    return delta