from collections import deque
import threading


class Interface(object):

    def __init__(self, **kwargs):

        self.monitor_thread = None
        self.queue = deque()
        self.successfully_sent = 0
        self.unsuccessfully_sent = 0

    def start(self):
        """
        Start monitoring for messages to dispatch.
        """
        self.monitor_thread = threading.Thread(target=self.monitor_queue, daemon=True)
        self.monitor_thread.start()

    def stop(self, gracefully=True):
        """
        Stop the interface gracefully or forcefully.
        :param gracefully: wait for all messages to be dispatched (Bool)
        """
        if gracefully:
            self.queue.append(('stop monitor thread', ''))
        else:
            self.queue.appendleft(('stop monitor thread', ''))
        if self.monitor_thread.is_alive():
            self.monitor_thread.join()

    def monitor_queue(self):
        """
        Check the message queue and dispatch them when found.
        """
        while 1:
            if self.queue:
                msg, content_type = self.queue.popleft()
                if msg == 'stop monitor thread':
                    return
                else:
                    self._send_message(msg=msg, content_type=content_type)

    def send_messages(self, *messages, content_type):
        """
        Send message(s) to this interface. They will be handled asynchronously.
        :param messages: list of dict
        :param content_type: str
        """
        for message in messages:
            self.queue.append((message, content_type))

    def _send_message(self, msg, content_type, **kwargs):
        """
        Overload and implement actual sending of the message to the interface.
        :param msg: dict
        :param content_type: str
        """
        pass
