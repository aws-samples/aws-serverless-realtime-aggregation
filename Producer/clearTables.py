# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import sys

# AWS Imports
import boto3

# Project Imports
sys.path.append('../Common')
import functions
import constants

# --------------------------------------------------------------------------------------------------
# Clear Table: Delete all items of a DDB Table
# --------------------------------------------------------------------------------------------------

def clear_table(table_name, primary_key_name, secondary_key_name = None):
    
    scan_args = dict()
    done = False
    start_key = None

    dynamodb = boto3.resource(constants.DYNAMO_NAME, region_name = constants.REGION_NAME)
    table = dynamodb.Table(table_name)
    
    # Count number of items in table
    total_item_count = functions.count_items(table)
    count = 0
    
    print('Deleting all ' + str(total_item_count) + ' items from ' + str(table_name) + '...')
    
    while not done:
        
        # Scan
        if start_key:
            scan_args['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_args)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
        
        # Get Items
        items = response.get('Items', [])
        
        # Delete
        batch_key_list = list()
    
        for i in range(len(items)):
            
            # Get ittem
            item = items[i]
            
            # Add to key list
            if secondary_key_name is None:
                key = {
                    primary_key_name: item[primary_key_name]
                }
            else:
                key = {
                        primary_key_name: item[primary_key_name], 
                        secondary_key_name: item[secondary_key_name]
                    }
            batch_key_list.append(key)
            
            # If batch_size is 25 or last item- delete all items:
            if len(batch_key_list) == 25 or i == (len(items) - 1):
                
                request_items = {
                    table.name : [{'DeleteRequest' : {'Key' : key}} for key in batch_key_list]
                }
                
                dynamodb.batch_write_item(RequestItems = request_items)
                
                # Reset list
                batch_key_list = list()
                
                 # Print Progress
                functions.print_progress_bar(int ((count + 1) / total_item_count) * 100)
            
            # Increment Count    
            count += 1
            
    print('')
    return True

# --------------------------------------------------------------------------------------------------
# Clear Aggregate Table
# --------------------------------------------------------------------------------------------------

clear_table(constants.AGGREGATE_TABLE_NAME, constants.AGGREGATE_TABLE_KEY)
#clear_table(STATE_TABLE_NAME, STATE_TABLE_KEY)
#clear_table(DELTA_TABLE_NAME, DELTA_TABLE_KEY)
