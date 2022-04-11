import requests
import requests.adapters
import hashlib
import hmac
import base64
import logging
import threading
import collections
import time
import json
import datetime
from prtg.sensor.result import CustomSensorResult


class PRTGInterface:

    def __init__(self, config=None):
        """
        Interface to send logs to an Azure Log Analytics Workspace.
        """
        self.monitor_thread = None
        self.queue = collections.deque()
        self.successfully_sent = 0
        self.unsuccessfully_sent = 0
        self.config = config
        self.results = collections.defaultdict(collections.deque)

    def start(self):

        self.monitor_thread = threading.Thread(target=self.monitor_queue, daemon=True)
        self.monitor_thread.start()

    def stop(self, gracefully=True):

        if gracefully:
            self.queue.append(('stop monitor thread', ''))
        else:
            self.queue.appendleft(('stop monitor thread', ''))
        if self.monitor_thread.is_alive():
            self.monitor_thread.join()

    def monitor_queue(self):

        while 1:
            if self.queue:
                msg, content_type = self.queue.popleft()
                if msg == 'stop monitor thread':
                    return
                else:
                    self.filter_result(message=msg, content_type=content_type)

    def send_messages_to_prtg(self, *messages, content_type):

        for message in messages:
            self.queue.append((message, content_type))

    def filter_result(self, message, content_type):

        for channel in self.config['channels']:
            if content_type not in channel['filters']:
                continue
            self._filter_result(message=message, content_type=content_type, channel=channel)

    def _filter_result(self, message, content_type, channel):

        for filter_rule in channel['filters'][content_type]:
            for filter_key, filter_value in filter_rule.items():
                if filter_key not in message or filter_value.lower() != message[filter_key].lower():
                    return
        self.results[channel['name']].append(message)

    def output(self):
        try:
            csr = CustomSensorResult()
            for channel_name, messages in self.results.items():
                csr.add_channel(
                    name=channel_name, value=len(messages), unit='Count')
            print(csr.json_result)
        except Exception as e:
            csr = CustomSensorResult(text="Python Script execution error")
            csr.error = "Python Script execution error: %s" % str(e)
            print(csr.json_result)

