from . import _Interface
import collections
from prtg.sensor.result import CustomSensorResult


class PRTGInterface(_Interface.Interface):

    def __init__(self, **kwargs):
        """
        Interface to send logs to an Azure Log Analytics Workspace.
        """
        super().__init__(**kwargs)
        self.results = collections.defaultdict(collections.deque)

    @property
    def enabled(self):

        return self.collector.config['output', 'prtg', 'enabled']

    def _send_message(self, msg, content_type, **kwargs):

        for channel in self.collector.config['output', 'prtg', 'channels']:
            if content_type not in channel['filters']:
                continue
            self._filter_result(msg=msg, content_type=content_type, channel=channel)

    def _filter_result(self, msg, content_type, channel):

        for filter_key, filter_value in channel['filters'][content_type].items():
            if filter_key not in msg or filter_value.lower() != msg[filter_key].lower():
                return
        self.results[channel['name']].append(msg)

    def output(self):
        try:
            csr = CustomSensorResult()
            for channel in self.collector.config['output', 'prtg', 'channels']:
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

    def exit_callback(self):

        super().exit_callback()
        self.output()
