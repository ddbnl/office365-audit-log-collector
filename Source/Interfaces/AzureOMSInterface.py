from . import _Interface
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


class AzureOMSInterface(_Interface.Interface):

    def __init__(self, **kwargs):
        """
        Interface to send logs to an Azure Log Analytics Workspace.
        :param workspace_id: Found under "Agent Configuration" blade (str)
        :param shared_key: Found under "Agent Configuration" blade (str)
        """
        super().__init__(**kwargs)
        self.threads = collections.deque()
        self.session = requests.Session()
        max_threads = self.collector.config['output', 'azureLogAnalytics', 'maxThreads'] or 50
        adapter = requests.adapters.HTTPAdapter(pool_connections=max_threads, pool_maxsize=max_threads)
        self.session.mount('https://', adapter)

    @property
    def enabled(self):

        return self.collector.config['output', 'azureLogAnalytics', 'enabled']

    def monitor_queue(self):
        """
        Overloaded for multithreading.
        """
        while 1:
            self.threads = [running_thread for running_thread in self.threads if running_thread.is_alive()]
            if self.queue and len(self.threads) < (self.collector.config['output', 'azureLogAnalytics', 'maxThreads']
                                                   or 50):
                msg, content_type = self.queue.popleft()
                if msg == 'stop monitor thread':
                    [running_thread.join() for running_thread in self.threads]
                    return
                else:
                    new_thread = threading.Thread(target=self._send_message,
                                                  kwargs={"msg": msg, "content_type": content_type}, daemon=True)
                    new_thread.start()
                    self.threads.append(new_thread)

    def _send_message(self, msg, content_type, retries=3):
        """
        Send a single message to a graylog input; the socket must be closed after each individual message,
        otherwise Graylog will interpret it as a single large message.
        :param msg: dict
        """
        time_generated = msg['CreationTime']
        msg_string = json.dumps(msg)
        if not msg_string:
            return
        while True:
            try:
                self._post_data(body=msg_string, log_type=content_type.replace('.', ''), time_generated=time_generated)
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

    def _build_signature(self, date, content_length, method, content_type, resource):
        """
        Returns authorization header which will be used when sending data into Azure Log Analytics.
        """

        x_headers = 'x-ms-date:' + date
        string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
        bytes_to_hash = bytes(string_to_hash, 'UTF-8')
        decoded_key = base64.b64decode(self.collector.config['output', 'azureLogAnalytics', 'sharedKey'])
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode(
            'utf-8')
        authorization = "SharedKey {}:{}".format(self.collector.config['output', 'azureLogAnalytics', 'workspaceId'],
                                                 encoded_hash)
        return authorization

    def _post_data(self, body, log_type, time_generated):
        """
        Sends payload to Azure Log Analytics Workspace.
        :param body: payload to send to Azure Log Analytics (json.dumps str)
        :param log_type: Azure Log Analytics table name (str)
        :param time_generated: date time of the original audit log msg (ISO 8601 str)
        """
        method = 'POST'
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body)
        signature = self._build_signature(rfc1123date, content_length, method, content_type, resource)

        uri = 'https://' + self.collector.config['output', 'azureLogAnalytics', 'workspaceId'] + \
              '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': log_type,
            'x-ms-date': rfc1123date,
            'time-generated-field': time_generated
        }
        response = self.session.post(uri, data=body, headers=headers)
        status_code = response.status_code
        try:
            json_output = response.json()
        except:
            json_output = ''

        response.close()
        if 200 <= status_code <= 299:
            logging.debug('Accepted payload:' + body)
        else:
            raise RuntimeError("Unable to send to OMS with {}: {} ".format(status_code, json_output))