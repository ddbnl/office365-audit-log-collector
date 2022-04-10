# Standard libs
from collections import deque
import logging
import threading
import socket
import json
import time


class GraylogInterface(object):

    def __init__(self, graylog_address, graylog_port):

        self.gl_address = graylog_address
        self.gl_port = graylog_port
        self.monitor_thread = None
        self.queue = deque()
        self.successfully_sent = 0
        self.unsuccessfully_sent = 0

    def start(self):

        self.monitor_thread = threading.Thread(target=self.monitor_queue, daemon=True)
        self.monitor_thread.start()

    def stop(self, gracefully=True):

        if gracefully:
            self.queue.append('stop monitor thread')
        else:
            self.queue.appendleft('stop monitor thread')
        if self.monitor_thread.is_alive():
            self.monitor_thread.join()

    def monitor_queue(self):

        while 1:
            if self.queue:
                msg = self.queue.popleft()
                if msg == 'stop monitor thread':
                    return
                else:
                    self._send_message_to_graylog(msg=msg)

    def send_messages_to_graylog(self, *messages):

        for message in messages:
            self.queue.append(message)

    def _connect_to_graylog_input(self):
        """
        Return a socket connected to the Graylog input.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.gl_address, int(self.gl_port)))
        return s

    def _send_message_to_graylog(self, msg, retries=3):
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
