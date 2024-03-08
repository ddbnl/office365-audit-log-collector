from . import _Interface
import logging


class FluentdInterface(_Interface.Interface):

    interface_name = 'fluentd'

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self._logger = None

    @property
    def enabled(self):

        return self.collector.config['output', self.interface_name, 'enabled']

    @property
    def address(self):

        return self.collector.config['output', self.interface_name, 'address']

    @property
    def port(self):

        return self.collector.config['output', self.interface_name, 'port']

    @property
    def tenant_name(self):

        return self.collector.config['output', self.interface_name, 'tenantName']

    @property
    def logger(self):

        if not self._logger:
            from fluent import sender
            self._logger = sender.FluentSender('o365', host=self.address, port=int(self.port))
        return self._logger

    def _send_message(self, msg, content_type, **kwargs):

        try:
            msg['tenant'] = self.tenant_name
            self.logger.emit(content_type, msg)
            self.successfully_sent += 1
        except Exception as e:
            logging.error("Error outputting to Fluentd: {}".format(e))
            self.unsuccessfully_sent += 1
