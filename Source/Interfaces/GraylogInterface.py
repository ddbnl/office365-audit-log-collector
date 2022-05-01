from . import _Interface
import datetime
import logging
import socket
import json
import time


class GraylogInterface(_Interface.Interface):

    @property
    def enabled(self):

        return self.collector.config['output', 'graylog', 'enabled']

    def _send_message(self, msg, retries=3, **kwargs):
        """
        Send a single message to a graylog input; the socket must be closed after each individual message,
        otherwise Graylog will interpret it as a single large message.
        :param msg: dict
        """
        msg = self._add_timestamp_field(msg)
        msg_string = json.dumps(msg)
        if not msg_string:
            return
        while True:
            try:
                sock = self._connect_to_graylog_input()
            except OSError as e:  # For issue: OSError: [Errno 99] Cannot assign requested address #6
                if retries:
                    logging.error("Error connecting to graylog: {}. Retrying {} more times".format(e, retries))
                    retries -= 1
                    time.sleep(30)
                else:
                    logging.error("Error connecting to graylog: {}. Giving up for this message: {}".format(
                        e, msg_string))
                    self.unsuccessfully_sent += 1
                    return
            else:
                break
        try:
            sock.sendall(msg_string.encode())
        except Exception as e:
            self.unsuccessfully_sent += 1
            logging.error("Error sending message to graylog: {}.".format(e))
        sock.close()
        self.successfully_sent += 1

    @staticmethod
    def _add_timestamp_field(msg):
        """
        Microsoft uses the CreationTime field for the datetime of the creation of the log. Graylog uses Timestamp for
        this by default. Add a Timestamp log to a msg with a Graylog compatible datetime for convencience.
        :param msg: JSON dict
        :return: JSON dict
        """
        creation_time = datetime.datetime.strptime(msg['CreationTime'], "%Y-%m-%dT%H:%M:%S")
        timestamp = creation_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        msg['timestamp'] = timestamp
        return msg

    def _connect_to_graylog_input(self):
        """
        Return a socket connected to the Graylog input.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.collector.config['output', 'graylog', 'address'],
                   int(self.collector.config['output', 'graylog', 'port'])))
        return s
