## Build a near-real-time, fault-tolerant data aggregation pipeline using a serverless, event-driven architecture

The collection, aggregation, and reporting of large volumes of data in near-real time is a challenge faced by customers from many different industries, like manufacturing, retail, gaming, utilities, and financial services. In this series of two blog posts, we review different architectural patterns for building near-real-time, scalable, serverless data aggregation pipelines in the AWS Cloud with Amazon DynamoDB, AWS Lambda, and Amazon Kinesis. 

## Deployment

Please follow the guidelines in our corresponding blog series to deploy the architecture in your own AWS account.

## Performance Tracking

The performance graphs (total throughput, pipeline latency, etc.) in our blog series were produced using Grafana in conjunction with InfluxDB. Our source code contains a flag in the file Common/constants.py that you can set to true, in order to start sending data to InfluxDB, enabling the performance visualization with Grafana. If you want to do this, you also need to set up a Grafana instance with InfluxDB, for example using Amazon Managed Service for Grafana and provide the IP of the instance, as well as the connection string for InfluxDB in the file Common/constants.py.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

