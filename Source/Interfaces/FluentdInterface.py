from . import _Interface
import logging
import datetime
from fluent import sender


class FluentdInterface(_Interface.Interface):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self._logger = None

    @property
    def enabled(self):

        return self.collector.config['output', 'fluentd', 'enabled']

    @property
    def address(self):

        return self.collector.config['output', 'fluentd', 'address']

    @property
    def port(self):

        return self.collector.config['output', 'fluentd', 'port']

    @property
    def tenant_name(self):

        return self.collector.config['output', 'fluentd', 'tenantName']

    @property
    def logger(self):

        if not self._logger:
            self._logger = sender.FluentSender('o365', host=self.address, port=int(self.port))
        return self._logger

    def _send_message(self, msg, content_type, **kwargs):

        creation_time = datetime.datetime.strptime(msg['CreationTime'], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
        unixtime = int(creation_time.timestamp())

        try:
            msg['tenant'] = self.tenant_name
            self.logger.emit_with_time(content_type, unixtime, msg)
            
            self.successfully_sent += 1
        except Exception as e:
            logging.error("Error outputting to Fluentd: {}".format(e))
            self.unsuccessfully_sent += 1
