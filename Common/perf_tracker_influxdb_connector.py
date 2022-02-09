# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
from influxdb import InfluxDBClient

class PerfTrackerInfluxDBConnector:

    def __init__(self, connector_string, influxdb_ip):

        """ TBD """

        tokens = connector_string.split(" ")
        self.influxdb_ip = influxdb_ip
        self.influxdb_port = tokens[0]
        self.database = tokens[1]
        self.measurement = tokens[2]


        self.influxdb_client = InfluxDBClient(host=self.influxdb_ip, port=self.influxdb_port)

        self.influxdb_client.create_database(self.database)
        self.influxdb_client.switch_database(self.database)

        self.samples_buffer = []


    def add_sample(self, json_data_sample):
        fields = {}

        for k,v in json_data_sample.items():
            fields[k] = v

        sample =   {
            "measurement": self.measurement,
            "time": json_data_sample["EVENT_TIME"],
            "fields": fields
        }


        self.samples_buffer.append(sample)


    def submit_measurements(self):

        res = self.influxdb_client.write_points(self.samples_buffer)
        self.samples_buffer = []

        return res
