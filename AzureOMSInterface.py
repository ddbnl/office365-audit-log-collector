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


class AzureOMSInterface:

    def __init__(self, workspace_id, shared_key, max_threads=50):
        """
        Interface to send logs to an Azure Log Analytics Workspace.
        :param workspace_id: Found under "Agent Configuration" blade (str)
        :param shared_key: Found under "Agent Configuration" blade (str)
        """
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.max_threads = max_threads
        self.threads = collections.deque()
        self.monitor_thread = None
        self.queue = collections.deque()
        self.successfully_sent = 0
        self.unsuccessfully_sent = 0
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=self.max_threads, pool_maxsize=self.max_threads)
        self.session.mount('https://', adapter)

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
            self.threads = [running_thread for running_thread in self.threads if running_thread.is_alive()]
            if self.queue and len(self.threads) < self.max_threads:
                msg, content_type = self.queue.popleft()
                if msg == 'stop monitor thread':
                    [running_thread.join() for running_thread in self.threads]
                    return
                else:
                    new_thread = threading.Thread(target=self._send_message_to_oms,
                                                  kwargs={"msg": msg, "content_type": content_type}, daemon=True)
                    new_thread.start()
                    self.threads.append(new_thread)

    def send_messages_to_oms(self, *messages, content_type):

        for message in messages:
            self.queue.append((message, content_type))

    def _send_message_to_oms(self, msg, content_type, retries=3):
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
                self.post_data(body=msg_string, log_type=content_type.replace('.', ''))
            except Exception as e:
                logging.error("Error sending to OMS: {}. Retries left: {}".format(e, retries))
                if retries:
                    retries -= 1
                    time.sleep(10)
                    continue
                else:
                    self.unsuccessfully_sent += 1
                    break
            else:
                self.successfully_sent += 1
                break

    def build_signature(self, date, content_length, method, content_type, resource):
        """Returns authorization header which will be used when sending data into Azure Log Analytics"""

        x_headers = 'x-ms-date:' + date
        string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
        bytes_to_hash = bytes(string_to_hash, 'UTF-8')
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode(
            'utf-8')
        authorization = "SharedKey {}:{}".format(self.workspace_id, encoded_hash)
        return authorization

    def post_data(self, body, log_type):
        """Sends payload to Azure Log Analytics Workspace

        Keyword arguments:
        customer_id -- Workspace ID obtained from Advanced Settings
        shared_key -- Authorization header, created using build_signature
        body -- payload to send to Azure Log Analytics
        log_type -- Azure Log Analytics table name
        """
        method = 'POST'
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body)
        signature = self.build_signature(rfc1123date, content_length, method, content_type, resource)

        uri = 'https://' + self.workspace_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': log_type,
            'x-ms-date': rfc1123date
        }
        response = self.session.post(uri, data=body, headers=headers)
        status_code, json_output = response.status_code, response.json
        response.close()
        if 200 <= status_code <= 299:
            logging.info('Accepted payload:' + body)
        else:
            raise RuntimeError("Unable to send to OMS with {}: {} ".format(status_code, json_output))