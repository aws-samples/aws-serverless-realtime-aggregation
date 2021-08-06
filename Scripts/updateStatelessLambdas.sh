# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

#!/bin/bash
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
REGION=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -v http://169.254.169.254/latest/meta-data/placement/region)

cd Deployment
cp ./../StatelessMapLambda/lambda_function.py ./
cp ./../Common/*.py ./
zip -r9 ./package.zip .
aws lambda update-function-code --function-name StatelessMapLambda --zip-file fileb://package.zip --region $REGION
cd ..

cd Deployment
cp ./../ReduceLambda/lambda_function.py ./
cp ./../Common/*.py ./
zip -r9 ./package.zip .
aws lambda update-function-code --function-name StatelessReduceLambda --zip-file fileb://package.zip --region $REGION
cd ..