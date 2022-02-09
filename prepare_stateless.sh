# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

#!/bin/bash
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
REGION=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -v http://169.254.169.254/latest/meta-data/placement/region) 

# Adjust Region in Common/constants.py
sed -i "s/INSERT_REGION_TOKEN/'$REGION'/g" ./Common/constants.py

# Select Scenario in Common/constants.py
sed -i "s/INSERT_SCENARIO_TOKEN/'Stateless'/g" ./Common/constants.py

# Install Boto3
pip3 install boto3

# Create Deployment Directory
mkdir ./Deployment

# Install all Libraries used by the Lambda Functions


# Push Lambda Functions
chmod +x ./Scripts/updateStatelessLambdas.sh
./Scripts/updateStatelessLambdas.sh