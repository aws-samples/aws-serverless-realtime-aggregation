# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import datetime
import time

from perf_tracker_influxdb_connector import PerfTrackerInfluxDBConnector

def get_time_now_ms():
    return int(round(time.time() * 1000))

def PerformanceTrackerInitializer(metrics_are_enabled, connection_string, influxdb_ip):
    metrics_are_enabled = bool(int(metrics_are_enabled))
    if metrics_are_enabled:
        tokens = connection_string.split(" ", 1) # Pick up first word in the string
        connector_type = tokens[0]
        if connector_type == "influxdb":
            influxdb_connector = PerfTrackerInfluxDBConnector(connector_string=tokens[1], influxdb_ip=influxdb_ip)
            perf_tracker = PerformanceTracker(influxdb_connector)
            return perf_tracker
        else:
            print("ERROR Undefined metrics connector type, no metrics will be collected: {} [{}]".format(connector_type))
            return __CreateEmptyPerformanceTracker
    else:
        return __CreateEmptyPerformanceTracker()

def __CreateEmptyPerformanceTracker():
    return PerformanceTracker(None)

class EventsCounter:
    def __init__(self, expected_events = []):
        self.expected_events = expected_events

        self.reset()

    def increment(self, event_name, value=1):
        if event_name in self.evcounter:
            self.evcounter[event_name] += value
        else:
            self.evcounter[event_name] = 1

    def set(self, event_name, value):
        self.evcounter[event_name] = value

    def get_counter(self, name):
        if name in self.evcounter:
            return self.evcounter[name]
        elif name.startswith("str"):
            return ""
        else:
            return 0

    def reset(self):

        """ When resetting counter we need to prebuild keys for expected counters,
        because elastic search can not change index mapping when new counters are added """

        self.evcounter = {}

        for e in self.expected_events:
            if e.startswith("str"):
                self.evcounter[e] = ""
            else:
                self.evcounter[e] = 0


class PerformanceTracker():

    def __init__(self, buffered_storage_connector):
        self.buffered_storage_connector = buffered_storage_connector
        self.stats_batch = []
        self.last_batch_submission_delay_ms = 0
        self.last_batch_submission_timestamp_ms = 0
        self.max_batching_delay_ms = 5*1000

    def add_metric_sample(self, stats_dic, event_counter, from_event, to_event, event_time=None):

        if not event_time:
            event_time = datetime.datetime.now().isoformat()

        data = {}
        data["EVENT_TIME"] = event_time
     
        if stats_dic is not None:
            sorted_keys = sorted(stats_dic.keys())
            start_index = sorted_keys.index(from_event)
            end_index = sorted_keys.index(to_event)

            for i in range(start_index, end_index):
                key = stats_dic[sorted_keys[i+1]]["label"]
                value = stats_dic[sorted_keys[i+1]]["tstmp"] - stats_dic[sorted_keys[i]]["tstmp"]
                data[key] = value


        if event_counter != None:
            for key, value in sorted(event_counter.evcounter.items()):
                data[key] = value

            event_counter.reset()

        # Self profiling on the batch submission delays
        data["last_batch_submission_delay_ms"] = self.last_batch_submission_delay_ms

        if self.buffered_storage_connector:
            self.buffered_storage_connector.add_sample(data)


    def submit_measurements(self):

        if self.buffered_storage_connector:
            if self.max_batching_delay_ms < get_time_now_ms() - self.last_batch_submission_timestamp_ms:

                time_start_ms = get_time_now_ms()
                self.buffered_storage_connector.submit_measurements()
                self.last_batch_submission_delay_ms = get_time_now_ms() - time_start_ms
                self.last_batch_submission_timestamp_ms = get_time_now_ms()

