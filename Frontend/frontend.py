# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# --------------------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------------------

# General Imports
import sys
import time
import curses
import collections
from datetime import datetime

# AWS Imports
import boto3

# Project Imports
sys.path.append('../Common')
from functions import *
from constants import *

# --------------------------------------------------------------------------------------------------
# Preparation
# --------------------------------------------------------------------------------------------------

# Connect to DynamoDB
ddb_ressource = boto3.resource(DYNAMO_NAME, region_name=REGION_NAME)
table = ddb_ressource.Table(AGGREGATE_TABLE_NAME)

# Prepare Terminal
stdscr = curses.initscr()
curses.noecho()
curses.cbreak()

speed = None

# --------------------------------------------------------------------------------------------------
# Indefinite Loop - Pull Data and Print it to Console
# --------------------------------------------------------------------------------------------------

try:
    while True:
        
        data = dict()
        message_count = 0

        # Read from DDB
        table_contents = table.scan(ConsistentRead = True)
        
        # Arrange for displaying
        if 'Items' in table_contents:
            for item in table_contents['Items']:
                identifier = item[AGGREGATE_TABLE_KEY]
                data[identifier] = item[VALUE_COLUMN_NAME]
                    
        if data:
            message_count = data[MESSAGE_COUNT_NAME]
            del data[MESSAGE_COUNT_NAME]
            
            ordered_data = collections.OrderedDict(sorted(data.items()))
        
        # Init Speed
        if speed is None:
            speed_measure_start_time = time.time()
            speed_measure_start_count = message_count
            speed = 0
        
        # Update Speed
        time_now = time.time()
        time_diff = time_now - speed_measure_start_time
        if time_now - speed_measure_start_time > 5:
            speed = max(0, int(message_count - speed_measure_start_count) / time_diff)
            speed_measure_start_count = message_count
            speed_measure_start_time = time_now
        
        # Header
        stdscr.addstr(0 ,0, 'Current Time: ' + datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        stdscr.addstr(1, 0, 'Total number of messages received: {}'.format(message_count))
        stdscr.addstr(2, 0, 'Current Message influx: {:.1f} Messages / Second'.format(speed))

        # Data
        if message_count == 0:
            stdscr.addstr(4 ,0, 'No data to be displayed so far...')
        else:
            row = 4
            for k,v in ordered_data.items():
                if k[:10] == "timestamp_":
                    continue
                level = k.count(':') 
                try:
                    stdscr.addstr(row, 0, '{:<35}'.format(k) + (' ' * level) + '{:10.2f}'.format(v))
                    row +=1
                except:
                    pass
                
        stdscr.refresh()
        time.sleep(0.5)
        stdscr.clear()
    
finally:
    # Clean terminal up
    curses.echo()
    curses.nocbreak()
    curses.endwin()
