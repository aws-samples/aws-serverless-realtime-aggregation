# --------------------------------------------------------------------------------------------------
# AWS Settings
# --------------------------------------------------------------------------------------------------

# Region
REGION_NAME                     = INSERT_REGION_TOKEN

# Scenario
SCENARIO                        = INSERT_SCENARIO_TOKEN

# Kinesis
KINESIS_NAME                    = 'kinesis'
KINESIS_STREAM_NAME             = SCENARIO + 'RiskDataStream'

# DynamoDB Table and Column Names
DYNAMO_NAME                     = 'dynamodb'

STATE_TABLE_NAME                = SCENARIO + 'StateTable'
STATE_TABLE_KEY                 = 'id'

DELTA_TABLE_NAME                = SCENARIO + 'ReduceTable'
DELTA_TABLE_KEY                 = 'MessageHash'

AGGREGATE_TABLE_NAME            = SCENARIO + 'AggregateTable'
AGGREGATE_TABLE_KEY             = 'Identifier'

MESSAGE_COUNT_NAME              = 'message_count'

ID_COLUMN_NAME                  = 'TradeID'
VERSION_COLUMN_NAME             = 'Version'
VALUE_COLUMN_NAME               = 'Value'
TIMESTAMP_COLUMN_NAME           = 'Timestamp'
HIERARCHY_COLUMN_NAME           = 'Hierarchy'

HIERARCHY_DEFINITION            =  {
                                    'RiskType'  : ['PV', 'Delta'],
                                    'Region'    : ['EMEA', 'APAC', 'AMER'],
                                    'TradeDesk' : ['FXSpot', 'FXOptions']
                                }

TIMESTAMP_GENERATOR_FIRST       = 'timestamp_generator_first'
TIMESTAMP_GENERATOR_MEAN        = 'timestamp_generator_mean'

# --------------------------------------------------------------------------------------------------
# Generator Settings
# --------------------------------------------------------------------------------------------------

# General
GENERATOR_STORAGE_ACTIVE            = True

# Number of messages per Generator
THREAD_NUM                          = 4
NUMBER_OF_BATCHES_PER_THREAD        = 250
BATCH_SIZE                          = 10

# Risk Values
MIN_VALUE_OF_RISK                   = 0
MAX_VALUE_OF_RISK                   = 100000

# Special Trades
DUPLICATES_PER_BATCH                = 0
PERCENTAGE_MODIFY                   = 0
PERCENTAGE_OUT_OR_ORDER             = 0

SPECIAL_TRADES = False
if SPECIAL_TRADES:
    DUPLICATES_PER_BATCH            = 1
    PERCENTAGE_MODIFY               = 1
    PERCENTAGE_OUT_OR_ORDER         = 100

# Other
TIME_INTERVAL_SPEED_CALCULATION     = 3
    
# --------------------------------------------------------------------------------------------------
# Aggregation Settings
# --------------------------------------------------------------------------------------------------

# Definition of the Hierarchy
AGGREGATION_HIERARCHY = ['RiskType', 'TradeDesk', 'Region']

# --------------------------------------------------------------------------------------------------
# Lambda Settings
# --------------------------------------------------------------------------------------------------

# Manually Introduced Failure of Lambdas
FAILURE_STATE_LAMBDA_PCT                = 0
FAILURE_MAP_LAMBDA_PCT                  = 0
FAILURE_STATELESS_MAP_LAMBDA_PCT        = 0
FAILURE_REDUCE_LAMBDA_PCT               = 0

FAILURES = False
if FAILURES:
    FAILURE_STATE_LAMBDA_PCT            = 1
    FAILURE_MAP_LAMBDA_PCT              = 2
    FAILURE_STATELESS_MAP_LAMBDA_PCT    = 0.2
    FAILURE_REDUCE_LAMBDA_PCT           = 2

# --------------------------------------------------------------------------------------------------
# Kibana / Performance Tracker Settings
# --------------------------------------------------------------------------------------------------

TRACK_PERFORMANCE                       = False
# INFLUX_CONNECTION_STRING                = '<Enter Connection String>'
# KIBANA_INSTANCE_IP                      = '<Enter Instance IP>'
