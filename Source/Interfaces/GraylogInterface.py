from . import _Interface
from collections import deque
import logging
import threading
import socket
import json
import time


class GraylogInterface(_Interface.Interface):

    def __init__(self, graylog_address=None, graylog_port=None, **kwargs):

        super().__init__(**kwargs)
        self.gl_address = graylog_address
        self.gl_port = graylog_port

    def _send_message(self, msg, retries=3, **kwargs):
        """
        Send a single message to a graylog input; the socket must be closed after each individual message,
        otherwise Graylog will interpret it as a single large message.
        :param msg: dict
        """
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

    def _connect_to_graylog_input(self):
        """
        Return a socket connected to the Graylog input.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.gl_address, int(self.gl_port)))
        return s
