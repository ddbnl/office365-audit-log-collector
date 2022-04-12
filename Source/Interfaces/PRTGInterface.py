from . import _Interface
import threading
import collections
from prtg.sensor.result import CustomSensorResult


class PRTGInterface(_Interface.Interface):

    def __init__(self, config=None, **kwargs):
        """
        Interface to send logs to an Azure Log Analytics Workspace.
        """
        super().__init__(**kwargs)
        self.config = config
        self.results = collections.defaultdict(collections.deque)

    def _send_message(self, message, content_type, **kwargs):

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
            for channel in self.config['channels']:
                if channel['name'] not in self.results:
                    self.results[channel['name']] = collections.deque()
            for channel_name, messages in self.results.items():
                csr.add_channel(
                    name=channel_name, value=len(messages), unit='Count')
            print(csr.json_result)
        except Exception as e:
            csr = CustomSensorResult(text="Python Script execution error")
            csr.error = "Python Script execution error: %s" % str(e)
            print(csr.json_result)

