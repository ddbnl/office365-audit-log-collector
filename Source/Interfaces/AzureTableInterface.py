import collections
import logging
import threading
import azure.core.exceptions
from . import _Interface
from azure.data.tables import TableServiceClient


class AzureTableInterface(_Interface.Interface):

    def __init__(self, table_connection_string=None, **kwargs):
        """
        Interface to send logs to CSV file(s). Not every audit log has every possible column, so columns in the CSV
        file might change over time. Therefore, the CSV file is recreated each time the cache_size is hit to insure
        integrity, taking the performance hit.
        """
        super().__init__(**kwargs)
        self.connection_string = table_connection_string
        self._table_service = None
        self._table_client = None
        self._threads = collections.deque()

    @property
    def enabled(self):

        return self.collector.config['output', 'azureTable', 'enabled']

    @property
    def table_service(self):

        if not self._table_service:
            if not self.connection_string:
                raise RuntimeError("Azure table output needs a connection string. Use --table-string to pass one.")
            self._table_service = TableServiceClient.from_connection_string(conn_str=self.connection_string)
        return self._table_service

    @property
    def table_client(self):

        if not self._table_client:
            self._table_client = self.table_service.create_table_if_not_exists(
                table_name=self.collector.config['output', 'azureTable', 'tableName'] or 'AuditLogs')
        return self._table_client

    @staticmethod
    def _validate_fields(msg):

        for k, v in msg.copy().items():
            if (isinstance(v, int) and v > 2147483647) or isinstance(v, list) or isinstance(v, dict):
                msg[k] = str(v)
        return msg

    def monitor_queue(self):
        """
        Overloaded for multithreading.
        """
        while 1:
            self._threads = [running_thread for running_thread in self._threads if running_thread.is_alive()]
            if self.queue and len(self._threads) < (self.collector.config['output', 'azureTable', 'maxThreads'] or 10):
                msg, content_type = self.queue.popleft()
                if msg == 'stop monitor thread':
                    [running_thread.join() for running_thread in self._threads]
                    return
                else:
                    new_thread = threading.Thread(target=self._send_message,
                                                  kwargs={"msg": msg, "content_type": content_type}, daemon=True)
                    new_thread.start()
                    self._threads.append(new_thread)

    def _send_message(self, msg, content_type, **kwargs):
        try:
            msg = self._validate_fields(msg=msg)
            entity = {
                'PartitionKey': content_type,
                'RowKey': msg['Id'],
            }
            entity.update(msg)
            self.table_client.create_entity(entity)
        except azure.core.exceptions.ResourceExistsError:
            self.successfully_sent += 1
            return
        except Exception as e:
            self.unsuccessfully_sent += 1
            logging.error("Error sending log to Azure Table. Log: {}. Error: {}.".format(msg, e))
        else:
            self.successfully_sent += 1

    def exit_callback(self):

        return [thread.join() for thread in self._threads]
