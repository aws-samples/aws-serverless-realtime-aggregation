# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import random
import json
import hashlib
import time
import collections
import uuid
import sys

# Multithreading Imports
import threading

# AWS Imports
import boto3

# Project Imports
sys.path.append('../Common')
import functions
import constants

# --------------------------------------------------------------------------------------------------
# Generate Message - This function in invoked by every thread
# --------------------------------------------------------------------------------------------------

def generate_messages(totals, print_to_console):

    thread_state = dict()
    thread_totals = dict()
    
    
    # Only designated thread prints
    if print_to_console:
        print_interval_start_time = time.time()
        print_interval_start_batch = 0
        speed = 0
    
    for i in range(constants.NUMBER_OF_BATCHES_PER_THREAD):
        
        # Only designated thread prints
        if print_to_console:

            # Print Speed
            current_time = time.time()
            time_diff = current_time - print_interval_start_time
            if time_diff > constants.TIME_INTERVAL_SPEED_CALCULATION:
                speed = (i - print_interval_start_batch) * \
                    constants.THREAD_NUM * constants.BATCH_SIZE / time_diff
                print_interval_start_time = current_time
                print_interval_start_batch = i

            # Print Progress
            progress = (i / constants.NUMBER_OF_BATCHES_PER_THREAD) * 100
            functions.print_progress_bar(progress, speed)
            
    
        # Initialize record list for this batch
        records = []
        
        # Calculate number of duplicates that are added at the end
        if constants.DUPLICATES_PER_BATCH < constants.BATCH_SIZE:
            number_of_duplicate_messages = constants.DUPLICATES_PER_BATCH
        else:
            number_of_duplicate_messages = max(0, constants.BATCH_SIZE - 1)
        
        # Create Batch
        for j in range(constants.BATCH_SIZE - number_of_duplicate_messages):
            
            # Initialize Empty Message
            message = {}
            
            # Random decision: Modify or New Entry
            if len(thread_state) == 0 or \
                random.uniform(0,100) < (100 - constants.PERCENTAGE_MODIFY):

                # -> New Entry

                # Generate ID
                message[constants.ID_COLUMN_NAME] = str(uuid.uuid4())
                
                # Add Version
                message[constants.VERSION_COLUMN_NAME] = 0
                
                # Count
                functions.dict_entry_add(thread_totals, 'count:add', 1)
                
            else:

                # -> Modify

                # Pick existing ID
                message[constants.ID_COLUMN_NAME] = random.choice(list(thread_state.keys()))
                
                # Get New Version
                if thread_state[message[constants.ID_COLUMN_NAME]][constants.VERSION_COLUMN_NAME] == 0 or \
                    random.uniform(1,100) < (100 - constants.PERCENTAGE_OUT_OR_ORDER):
                    # Iterate Version
                    message[constants.VERSION_COLUMN_NAME] = \
                        thread_state[message[constants.ID_COLUMN_NAME]][constants.VERSION_COLUMN_NAME] + 1
                    functions.dict_entry_add(thread_totals, 'count:modify:in_order', 1)
                else:
                    # Insert Older Version
                    message[constants.VERSION_COLUMN_NAME] = \
                        thread_state[message[constants.ID_COLUMN_NAME]][constants.VERSION_COLUMN_NAME] - 1
                    functions.dict_entry_add(thread_totals, 'count:modify:out_of_order', 1)
                
            # Add Random Value 
            message[constants.VALUE_COLUMN_NAME] = functions.random_value()
            
            # Add Random Hierarchy
            message[constants.HIERARCHY_COLUMN_NAME] = functions.random_hierarchy()
            
            # Add Timestamp
            message[constants.TIMESTAMP_COLUMN_NAME] = time.time()
            
            # Dump to String
            message_string = json.dumps(message)
            
            # Append to Record List
            record = {'Data' : message_string, 'PartitionKey' : 
                hashlib.sha256(message_string.encode()).hexdigest()}
            records.append(record)
            
            # Append to Internal Storage - if message was sent in order
            if constants.GENERATOR_STORAGE_ACTIVE:
                if (message[constants.ID_COLUMN_NAME] not in thread_state) or \
                    (thread_state[message[constants.ID_COLUMN_NAME]][constants.VERSION_COLUMN_NAME] \
                    < message[constants.VERSION_COLUMN_NAME]):
                    thread_state[message[constants.ID_COLUMN_NAME]] = message
    
        # Add Duplicates
        for k in range(number_of_duplicate_messages):
            duplicate_index = random.randint(0, constants.BATCH_SIZE - number_of_duplicate_messages - 1)
            records.append(records[duplicate_index])
        
        functions.dict_entry_add(thread_totals, 'count:duplicates', number_of_duplicate_messages)

        # Send Batch to Kinesis Stream
        response = kinesis_client.put_records(StreamName=constants.KINESIS_STREAM_NAME,Records=records)

    if constants.GENERATOR_STORAGE_ACTIVE:
        # Aggregate over Final State
        for entry in thread_state.values():
            k = functions.hierarchy_to_string(entry[constants.HIERARCHY_COLUMN_NAME], constants.AGGREGATION_HIERARCHY)
            v = entry[constants.VALUE_COLUMN_NAME]
            functions.dict_entry_add(thread_totals, k, v)

        # Add to Totals
        for k,v in thread_totals.items():
            functions.dict_entry_add(totals, k, v)

# --------------------------------------------------------------------------------------------------
# Main: Invoke Threads and Generate Messages
# --------------------------------------------------------------------------------------------------

# Initialize Kinesis Consumer
kinesis_client = boto3.client(constants.KINESIS_NAME, region_name=constants.REGION_NAME)

# Take start time
start_time = time.time()

# Print general info
print('\nGenerating items and writing to Kinesis...\n')
print("Example message: \n{\n" +
    "   " + constants.ID_COLUMN_NAME          + ": '0d957288-2913-4dbb-b359-5ec5ff732cac',\n" +
    "   " + constants.VERSION_COLUMN_NAME     + ": 0,\n" + 
    "   " + constants.VALUE_COLUMN_NAME       + ": " + str(functions.random_value()) + ",\n" + 
    "   " + constants.TIMESTAMP_COLUMN_NAME   + ": " + str(time.time())  + ",\n" + 
    "   " + constants.HIERARCHY_COLUMN_NAME   + ": " + str(functions.random_hierarchy())+ "\n}\n"
    )

# Invoke Threads
totals = dict()
threads = list()

print('Invoking ' + str(constants.THREAD_NUM) + ' threads...\n')
for index in range(constants.THREAD_NUM):
    x = threading.Thread(target=generate_messages, args=(totals, index == (constants.THREAD_NUM - 1),))
    threads.append(x)
    x.start()

for index, thread in enumerate(threads):
    thread.join()
    
print('\n\nAll threads finished.\n')
    
# Print to Console
end_time = time.time()
total_message_count = \
    constants.BATCH_SIZE * constants.NUMBER_OF_BATCHES_PER_THREAD * constants.THREAD_NUM
ingestion_time = end_time - start_time
print(f'\nSimple Data producer finished!')
print(f'Total number of messages: {total_message_count}.')
print(f'Total ingestion time: {ingestion_time:.1f} seconds.')
print(f'Average ingestion rate: {total_message_count / ingestion_time:.1f} messages / second.')

# --------------------------------------------------------------------------------------------------
# Print Totals to check consistency of pipeline
# --------------------------------------------------------------------------------------------------

if constants.GENERATOR_STORAGE_ACTIVE:

    totals = functions.aggregate_along_tree(totals)
    ordered_totals = collections.OrderedDict(sorted(totals.items()))
    print('\nMessage Counts:\n')
    for k,v in ordered_totals.items():
        if k[:5] == 'count':
            level = k.count(':') 
            print('{:<25}'.format(k) + (' ' * level) + '{:>10}'.format(v))

    print('\nTotals:\n')
    for k,v in ordered_totals.items():
        if k[:5] != 'count':
            level = k.count(':') 
            print('{:<35}'.format(k) + (' ' * level) + '{:10.2f}'.format(v))
    print('\n')