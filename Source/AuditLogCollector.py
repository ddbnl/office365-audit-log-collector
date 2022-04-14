from Interfaces import AzureOMSInterface, GraylogInterface, PRTGInterface, FileInterface
import AuditLogSubscriber
import ApiConnection
import os
import sys
import yaml
import time
import json
import logging
import datetime
import argparse
import collections
import threading


class AuditLogCollector(ApiConnection.ApiConnection):

    def __init__(self, content_types=None, resume=True, fallback_time=None, skip_known_logs=True,
                 log_path='collector.log', debug=False, auto_subscribe=False, max_threads=20, retries=3,
                 retry_cooldown=3, file_output=False, output_path=None, graylog_output=False, azure_oms_output=False,
                 prtg_output=False, **kwargs):
        """
        Object that can retrieve all available content blobs for a list of content types and then retrieve those
        blobs and output them to a file or Graylog input (i.e. send over a socket).
        :param content_types: list of content types to retrieve (e.g. 'Audit.Exchange', 'Audit.Sharepoint')
        :param resume: Resume from last known run time for each content type (Bool)
        :param fallback_time: if no last run times are found to resume from, run from this start time (Datetime)
        :param retries: Times to retry retrieving a content blob if it fails (int)
        :param retry_cooldown: Seconds to wait before retrying retrieving a content blob (int)
        :param skip_known_logs: record retrieved content blobs and log ids, skip them next time (Bool)
        :param file_output: path of file to output audit logs to (str)
        :param log_path: path of file to log to (str)
        :param debug: enable debug logging (Bool)
        :param auto_subscribe: automatically subscribe to audit log feeds for which content is retrieved (Bool)
        :param output_path: path to output retrieved logs to (None=no file output) (string)
        :param graylog_output: Enable graylog Interface (Bool)
        :param azure_oms_output: Enable Azure workspace analytics OMS Interface (Bool)
        :param prtg_output: Enable PRTG output (Bool)
                """
        super().__init__(**kwargs)
        self.content_types = content_types or collections.deque()
        self.resume = resume
        self._fallback_time = fallback_time or datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=23)
        self.retries = retries
        self.retry_cooldown = retry_cooldown
        self.skip_known_logs = skip_known_logs
        self.log_path = log_path
        self.debug = debug
        self.auto_subscribe = auto_subscribe
        self.filters = {}

        self.file_output = file_output
        self.file_interface = FileInterface.FileInterface(**kwargs)
        self.azure_oms_output = azure_oms_output
        self.azure_oms_interface = AzureOMSInterface.AzureOMSInterface(**kwargs)
        self.graylog_output = graylog_output
        self.graylog_interface = GraylogInterface.GraylogInterface(**kwargs)
        self.prtg_output = prtg_output
        self.prtg_interface = PRTGInterface.PRTGInterface(**kwargs)
        self.max_threads = max_threads

        self._last_run_times = {}
        self._known_content = {}
        self._known_logs = {}

        self.blobs_to_collect = collections.defaultdict(collections.deque)
        self.monitor_thread = threading.Thread()
        self.retrieve_available_content_threads = collections.deque()
        self.retrieve_content_threads = collections.deque()
        self.run_started = None
        self.logs_retrieved = 0
        self.errors_retrieving = 0

    @property
    def all_content_types(self):
        """
        :return: list of str
        """
        return ['Audit.General', 'Audit.AzureActiveDirectory', 'Audit.Exchange', 'Audit.SharePoint', 'DLP.All']

    def load_config(self, path):
        """
        Load a YML config containing settings for this collector and its' interfaces.
        :param path: str
        """
        with open(path, 'r') as ofile:
            config = yaml.safe_load(ofile)
        self._load_log_config(config=config)
        self._load_collect_config(config=config)
        self._load_filter_config(config=config)
        self._load_output_config(config=config)

    def _load_log_config(self, config):
        """
        :param config: str
        """
        if 'log' in config['collect']:
            if 'path' in config['collect']['log']:
                self.log_path = config['collect']['log']['path']
            if 'debug' in config['collect']['log']:
                self.debug = config['collect']['log']['path']

    def _load_collect_config(self, config):
        """
        :param config: str
        """
        if 'collect' in config:
            if 'contentTypes' in config['collect']:
                self.content_types = [
                    x for x in self.all_content_types if x in config['collect']['contentTypes'] and
                    config['collect']['contentTypes'][x] is True]
            if 'maxThreads' in config['collect']:
                self.max_threads = config['collect']['maxThreads']
            if 'retries' in config['collect']:
                self.retries = config['collect']['retries']
            if 'retryCooldown' in config['collect']:
                self.retry_cooldown = config['collect']['retryCooldown']
            if 'autoSubscribe' in config['collect']:
                self.auto_subscribe = config['collect']['autoSubscribe']
            if 'skipKnownLogs' in config['collect']:
                self.skip_known_logs = config['collect']['skipKnownLogs']
            if 'resume' in config['collect']:
                self.resume = config['collect']['resume']
            if 'hoursToCollect' in config['collect']:
                self._fallback_time = datetime.datetime.now(datetime.timezone.utc) -\
                    datetime.timedelta(hours=config['collect']['hoursToCollect'])

    def _load_filter_config(self, config):
        """
        :param config: str
        """
        if 'filter' in config and config['filter']:
            self.filters = config['filter']

    def _load_output_config(self, config):
        """
        :param config: str
        """
        if 'output' in config:
            self._load_file_output_config(config=config)
            self._load_azure_log_analytics_output_config(config=config)
            self._load_graylog_output_config(config=config)
            self._load_prtg_output_config(config=config)

    def _load_file_output_config(self, config):
        """
        :param config: str
        """
        if 'file' in config['output']:
            if 'enabled' in config['output']['file']:
                self.file_output = config['output']['file']['enabled']
            if 'path' in config['output']['file']:
                self.file_interface.output_path = config['output']['file']['path']
            if 'separateByContentType' in config['output']['file']:
                self.file_interface.separate_by_content_type = config['output']['file']['separateByContentType']
            if 'separator' in config['output']['file']:
                self.file_interface.separator = config['output']['file']['separator']

    def _load_azure_log_analytics_output_config(self, config):
        """
        :param config: str
        """
        if 'azureLogAnalytics' in config['output']:
            if 'enabled' in config['output']['azureLogAnalytics']:
                self.azure_oms_output = config['output']['azureLogAnalytics']['enabled']
            if 'workspaceId' in config['output']['azureLogAnalytics']:
                self.azure_oms_interface.workspace_id = config['output']['azureLogAnalytics']['workspaceId']
            if 'sharedKey' in config['output']['azureLogAnalytics']:
                self.azure_oms_interface.shared_key = config['output']['azureLogAnalytics']['sharedKey']

    def _load_graylog_output_config(self, config):
        """
        :param config: str
        """
        if 'graylog' in config['output']:
            if 'enabled' in config['output']['graylog']:
                self.graylog_output = config['output']['graylog']['enabled']
            if 'address' in config['output']['graylog']:
                self.graylog_interface.gl_address = config['output']['graylog']['address']
            if 'port' in config['output']['graylog']:
                self.graylog_interface.gl_port = config['output']['graylog']['port']

    def _load_prtg_output_config(self, config):
        """
        :param config: str
        """
        if 'prtg' in config['output']:
            if 'enabled' in config['output']['prtg']:
                self.prtg_output = config['output']['prtg']['enabled']
            self.prtg_interface.config = config['output']['prtg']

    def init_logging(self):
        """
        Start logging to file and console. If PRTG output is enabled do not log to console, as this will interfere with
        the sensor result.
        """
        logger = logging.getLogger()
        file_handler = logging.FileHandler(self.log_path, mode='w')
        if not self.prtg_output:
            stream_handler = logging.StreamHandler(sys.stdout)
            logger.addHandler(stream_handler)
        logger.addHandler(file_handler)
        logger.setLevel(logging.INFO if not self.debug else logging.DEBUG)

    def _prepare_to_run(self):
        """
        Make sure that self.run_once can be called multiple times by resetting to an initial state.
        """
        if self.auto_subscribe:
            self._auto_subscribe()
        if self.resume:
            self._get_last_run_times()
        if self.skip_known_logs:
            self._known_content.clear()
            self._known_logs.clear()
            self._clean_known_content()
            self._clean_known_logs()
        self.logs_retrieved = 0
        self.graylog_interface.successfully_sent = 0
        self.graylog_interface.unsuccessfully_sent = 0
        self.azure_oms_interface.successfully_sent = 0
        self.azure_oms_interface.unsuccessfully_sent = 0
        self.run_started = datetime.datetime.now()

    def run_once(self, start_time=None):
        """
        Check available content and retrieve it, then exit.
        """
        logging.log(level=logging.INFO, msg='Starting run @ {}. Content: {}.'.format(
            datetime.datetime.now(), self.content_types))
        self._prepare_to_run()
        self._start_monitoring()
        self._get_all_available_content(start_time=start_time)
        self.monitor_thread.join()
        self._finish_run()

    def _finish_run(self):
        """
        Save relevant information and output PRTG result if the interface is enabled. The other interfaces output
        while collecting.
        """
        if self.skip_known_logs:
            self._add_known_log()
            self._add_known_content()
        if self.resume and self._last_run_times:
            with open('last_run_times', 'w') as ofile:
                json.dump(fp=ofile, obj=self._last_run_times)
        if self.prtg_output:
            self.prtg_interface.output()
        self._log_statistics()

    def _log_statistics(self):
        """
        Write run statistics to log file / console.
        """
        logging.info("Finished. Total logs retrieved: {}. Total logs with errors: {}. Run time: {}.".format(
            self.logs_retrieved, self.errors_retrieving, datetime.datetime.now() - self.run_started))
        if self.azure_oms_output:
            logging.info("Azure OMS output report: {} successfully sent, {} errors".format(
                self.azure_oms_interface.successfully_sent, self.azure_oms_interface.unsuccessfully_sent))
        if self.graylog_output:
            logging.info("Graylog output report: {} successfully sent, {} errors".format(
                self.graylog_interface.successfully_sent, self.graylog_interface.unsuccessfully_sent))

    def _get_last_run_times(self):
        """
        Load last_run_times file and interpret the datetime for each content type.
        """
        if os.path.exists('last_run_times'):
            try:
                with open('last_run_times', 'r') as ofile:
                    self._last_run_times = json.load(ofile)
            except Exception as e:
                logging.error("Could not read last run times file: {}.".format(e))
            for content_type, last_run_time in self._last_run_times.items():
                try:
                    self._last_run_times[content_type] = datetime.datetime.strptime(last_run_time, "%Y-%m-%dT%H:%M:%SZ")
                except Exception as e:
                    logging.error("Could not read last run time for content type {}: {}.".format(content_type, e))
                    del self._last_run_times[content_type]

    @property
    def done_retrieving_content(self):
        """
        Returns True if there are no more content blobs to be collected. Used to determine when to exit the script.
        :return: Bool
        """
        for content_type in self.blobs_to_collect:
            if self.blobs_to_collect[content_type]:
                return False
        return True

    @property
    def done_collecting_available_content(self):
        """
        Once a call is made to retrieve content for a particular type, and there is no 'NextPageUri' in the response,
        the type is removed from 'self.content_types' to signal that all available content has been retrieved for that
        type.
        """
        return not bool(self.content_types)

    def _start_monitoring(self):
        """
        Start a thread monitoring the list containing blobs that need collecting.
        """
        self.monitor_thread = threading.Thread(target=self._monitor_blobs_to_collect, daemon=True)
        self.monitor_thread.start()

    def _auto_subscribe(self):
        """
        Subscribe to all content types that are set to be retrieved.
        """
        subscriber = AuditLogSubscriber.AuditLogSubscriber(tenant_id=self.tenant_id, client_key=self.client_key,
                                                           secret_key=self.secret_key)
        status = subscriber.get_sub_status()
        if status == '':
            raise RuntimeError("Auto subscribe enabled but could not get subscription status")
        unsubscribed_content_types = self.content_types.copy()
        for s in status:
            if s['contentType'] in self.content_types and s['status'].lower() == 'enabled':
                unsubscribed_content_types.remove(s['contentType'])
        for content_type in unsubscribed_content_types:
            logging.info("Auto subscribing to: {}".format(content_type))
            subscriber.set_sub_status(content_type=content_type, action='start')

    def _get_all_available_content(self, start_time=None):
        """
        Start a thread to retrieve available content blobs for each content type to be collected.
        :param start_time: DateTime
        """
        for content_type in self.content_types.copy():
            if not start_time:
                if self.resume and content_type in self._last_run_times.keys():
                    start_time = self._last_run_times[content_type]
                else:
                    start_time = self._fallback_time
            self.retrieve_available_content_threads.append(threading.Thread(
                target=self._get_available_content, daemon=True,
                kwargs={'content_type': content_type, 'start_time': start_time}))
            self.retrieve_available_content_threads[-1].start()

    def _get_available_content(self, content_type, start_time):
        """
        Retrieve available content blobs for a content type. If the response contains a
        'NextPageUri' there is more content to be retrieved; rerun until all has been retrieved.
        """
        try:
            logging.log(level=logging.DEBUG, msg='Getting available content for type: "{}"'.format(content_type))
            current_time = datetime.datetime.now(datetime.timezone.utc)
            formatted_end_time = str(current_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
            formatted_start_time = str(start_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
            logging.info("Retrieving {}. Start time: {}. End time: {}.".format(
                content_type, formatted_start_time, formatted_end_time))
            response = self.make_api_request(url='subscriptions/content?contentType={0}&startTime={1}&endTime={2}'.format(
                content_type, formatted_start_time, formatted_end_time))
            self.blobs_to_collect[content_type] += response.json()
            while 'NextPageUri' in response.headers.keys() and response.headers['NextPageUri']:
                logging.log(level=logging.DEBUG, msg='Getting next page of content for type: "{0}"'.format(content_type))
                self.blobs_to_collect[content_type] += response.json()
                response = self.make_api_request(url=response.headers['NextPageUri'], append_url=False)
            logging.log(level=logging.DEBUG, msg='Got {0} content blobs of type: "{1}"'.format(
                len(self.blobs_to_collect[content_type]), content_type))
        except Exception as e:
            logging.log(level=logging.DEBUG, msg="Error while getting available content: {}: {}".format(
                content_type, e))
            self.content_types.remove(content_type)
        else:
            self.content_types.remove(content_type)
            self._last_run_times[content_type] = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _start_interfaces(self):

        if self.file_output:
            self.file_interface.start()
        if self.azure_oms_output:
            self.azure_oms_interface.start()
        if self.prtg_output:
            self.prtg_interface.start()
        if self.graylog_output:
            self.graylog_interface.start()

    def _stop_interfaces(self):

        if self.file_output:
            self.file_interface.stop()
        if self.azure_oms_output:
            self.azure_oms_interface.stop()
        if self.prtg_output:
            self.prtg_interface.stop()
        if self.graylog_output:
            self.graylog_interface.stop()

    def _monitor_blobs_to_collect(self):
        """
        Wait for the 'retrieve_available_content' function to retrieve content URI's. Once they become available
        start retrieving in a background thread.
        """
        self._start_interfaces()
        threads = collections.deque()
        while True:
            threads = [thread for thread in threads if thread.is_alive()]
            if self.done_collecting_available_content and self.done_retrieving_content and not threads:
                break
            if not self.blobs_to_collect:
                continue
            for content_type, blobs_to_collect in self.blobs_to_collect.copy().items():
                if len(threads) >= self.max_threads:
                    break
                if self.blobs_to_collect[content_type]:
                    blob_json = self.blobs_to_collect[content_type].popleft()
                    self._collect_blob(blob_json=blob_json, content_type=content_type, threads=threads)
        self._stop_interfaces()

    def _collect_blob(self, blob_json, content_type, threads):
        """
        Collect a single content blob in a thread.
        :param blob_json: JSON
        :param content_type: str
        :param threads: list
        """
        if blob_json and 'contentUri' in blob_json:
            logging.log(level=logging.DEBUG, msg='Retrieving content blob: "{0}"'.format(blob_json))
            threads.append(threading.Thread(
                target=self._retrieve_content, daemon=True,
                kwargs={'content_json': blob_json, 'content_type': content_type, 'retries': self.retries}))
            threads[-1].start()

    def _retrieve_content(self, content_json, content_type, retries):
        """
        Get an available content blob. If it exists in the list of known content blobs it is skipped to ensure
        idempotence.
        :param content_json: JSON dict of the content blob as retrieved from the API (dict)
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :param retries: Times to retry retrieving a content blob if it fails (int)
        """
        if self.skip_known_logs and self.known_content and content_json['contentId'] in self.known_content:
            return
        try:
            results = self.make_api_request(url=content_json['contentUri'], append_url=False).json()
            if not results:
                return
        except Exception as e:
            if retries:
                time.sleep(self.retry_cooldown)
                return self._retrieve_content(content_json=content_json, content_type=content_type, retries=retries - 1)
            else:
                self.errors_retrieving += 1
                logging.error("Error retrieving content: {}".format(e))
                return
        else:
            self._handle_retrieved_content(content_json=content_json, content_type=content_type, results=results)

    def _handle_retrieved_content(self, content_json, content_type, results):
        """
        Check known logs, filter results and output what remains.
        :param content_json: JSON dict of the content blob as retrieved from the API (dict)
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :param results: list of JSON
        """

        if self.skip_known_logs:
            self._known_content[content_json['contentId']] = content_json['contentExpiration']
        for log in results.copy():
            if self.skip_known_logs:
                if log['Id'] in self.known_logs:
                    results.remove(log)
                    continue
                self.known_logs[log['Id']] = log['CreationTime']
            if self.filters and not self._check_filters(log=log, content_type=content_type):
                results.remove(log)
        self.logs_retrieved += len(results)
        self._output_results(results=results, content_type=content_type)

    def _output_results(self, results, content_type):
        """
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :param results: list of JSON
        """
        if self.file_output:
            self.file_interface.send_messages(*results, content_type=content_type)
        if self.prtg_output:
            self.prtg_interface.send_messages(*results, content_type=content_type)
        if self.graylog_output:
            self.graylog_interface.send_messages(*results, content_type=content_type)
        if self.azure_oms_output:
            self.azure_oms_interface.send_messages(*results, content_type=content_type)

    def _check_filters(self, log, content_type):
        """
        :param log: JSON
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :return: True if log matches filter, False if not (Bool)
        """
        if content_type in self.filters and self.filters[content_type]:
            for log_filter_key, log_filter_value in self.filters[content_type].items():
                if log_filter_key not in log or log[log_filter_key].lower() != log_filter_value.lower():
                    return False
        return True

    def _add_known_log(self):
        """
        Add a content ID to the known content file to avoid saving messages more than once.
        :return:
        """
        with open('known_logs', 'w') as ofile:
            for log_id, creation_time in self.known_logs.items():
                ofile.write('{},{}\n'.format(log_id, creation_time))

    def _add_known_content(self):
        """
        Add a content ID to the known content file to avoid saving messages more than once.
        :return:
        """
        with open('known_content', 'w') as ofile:
            for content_id, content_expiration in self.known_content.items():
                ofile.write('{0},{1}\n'.format(content_id, content_expiration))

    def _clean_known_logs(self):
        """
        Remove any known content ID's that have expired. Can't download a duplicate if it is not available for
        download.
        """
        known_logs = self.known_logs
        if os.path.exists('known_logs'):
            os.remove('known_logs')
            for log_id, creation_time in known_logs.copy().items():
                try:
                    date = datetime.datetime.strptime(creation_time.strip()+'Z', "%Y-%m-%dT%H:%M:%S%z")
                    expire_date = date + datetime.timedelta(days=7)
                    if not datetime.datetime.now(datetime.timezone.utc) < expire_date:
                        del self.known_logs[log_id]
                except Exception as e:
                    logging.debug("Could not parse known logs: {}".format(e))
                    del self.known_logs[log_id]
        if not known_logs:
            return
        with open('known_logs', mode='w') as ofile:
            for log_id, creation_time in known_logs.items():
                ofile.write("{},{}\n".format(log_id, creation_time.strip()))

    def _clean_known_content(self):
        """
        Remove any known content ID's that have expired. Can't download a duplicate if it is not available for
        download.
        """
        known_content = self.known_content
        if os.path.exists('known_content'):
            os.remove('known_content')
            for content_id, expire_date in known_content.copy().items():
                try:
                    date = datetime.datetime.strptime(expire_date, "%Y-%m-%dT%H:%M:%S.%f%z")
                    if not datetime.datetime.now(datetime.timezone.utc) < date:
                        del known_content[content_id]
                except Exception as e:
                    logging.debug("Could not parse known content: {}".format(e))
                    del known_content[content_id]
        if not known_content:
            return
        with open('known_logs', 'w') as ofile:
            for content_id, expire_date in known_content.items():
                ofile.write("{},{}\n".format(content_id, expire_date))

    @property
    def known_logs(self):
        """
        Parse and return known content file.
        :return: {content_id: content_expiration_date} dict
        """
        if not self._known_logs and os.path.exists('known_logs'):
            with open('known_logs', 'r') as ofile:
                for line in ofile.readlines():
                    if not line.strip():
                        continue
                    try:
                        self._known_logs[line.split(',')[0].strip()] = line.split(',')[1]
                    except:
                        continue
        return self._known_logs

    @property
    def known_content(self):
        """
        Parse and return known content file.
        :return: {content_id: content_expiration_date} dict
        """
        if not self._known_content and os.path.exists('known_content'):
            with open('known_content', 'r') as ofile:
                for line in ofile.readlines():
                    if not line.strip():
                        continue
                    try:
                        self._known_content[line.split(',')[0].strip()] = line.split(',')[1]
                    except:
                        continue
        return self._known_content


if __name__ == "__main__":

    description = \
    """
    Retrieve audit log contents from Office 365 API and save to file or Graylog.
    Example: Retrieve all available content and send it to Graylog (using mock ID's and keys):
    "AuditLogCollector.py 123 456 789 --general --exchange --azure_ad --sharepoint --dlp -g -gA 10.10.10.1 -gP 5000
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('tenant_id', type=str, help='Tenant ID of Azure AD', action='store')
    parser.add_argument('client_key', type=str, help='Client key of Azure application', action='store')
    parser.add_argument('secret_key', type=str, help='Secret key generated by Azure application', action='store')
    parser.add_argument('--config', metavar='config', type=str, help='Path to YAML config file',
                        action='store', dest='config')
    parser.add_argument('--interactive-subscriber', action='store_true',
                        help='Manually (un)subscribe to audit log feeds', dest='interactive_subscriber')
    parser.add_argument('--general', action='store_true', help='Retrieve General content', dest='general')
    parser.add_argument('--exchange', action='store_true', help='Retrieve Exchange content', dest='exchange')
    parser.add_argument('--azure_ad', action='store_true', help='Retrieve Azure AD content', dest='azure_ad')
    parser.add_argument('--sharepoint', action='store_true', help='Retrieve SharePoint content', dest='sharepoint')
    parser.add_argument('--dlp', action='store_true', help='Retrieve DLP content', dest='dlp')
    parser.add_argument('-p', metavar='publisher_id', type=str, help='Publisher GUID to avoid API throttling',
                        action='store', dest='publisher_id',
                        default=os.path.join(os.path.dirname(__file__), 'AuditLogCollector.log'))
    parser.add_argument('-r',
                        help='Look for last run time and resume looking for content from there (takes precedence over '
                             '-tH and -tD)', action='store_true', dest='resume')
    parser.add_argument('-tH', metavar='time_hours', type=int, help='Amount of hours to go back and look for content',
                        action='store', dest='time_hours')
    parser.add_argument('-s',
                        help='Keep track of each retrieved log ID and skip it in the future to prevent duplicates',
                        action='store_true', dest='skip_known_logs')
    parser.add_argument('-l', metavar='log_path', type=str, help='Path of log file', action='store', dest='log_path',
                        default=os.path.join(os.path.dirname(__file__), 'AuditLogCollector.log'))
    parser.add_argument('-d', action='store_true', dest='debug_logging',
                        help='Enable debug logging (generates large log files and decreases performance).')
    parser.add_argument('-f', help='Output to file.', action='store_true', dest='file')
    parser.add_argument('-fP', metavar='file_output_path', type=str, help='Path of directory of output files',
                        default=os.path.join(os.path.dirname(__file__), 'output'), action='store',
                        dest='output_path')
    parser.add_argument('-P', help='Output to PRTG with PrtgConfig.yaml.', action='store_true', dest='prtg')
    parser.add_argument('-a', help='Output to Azure Log Analytics workspace.', action='store_true', dest='azure')
    parser.add_argument('-aC', metavar='azure_workspace', type=str, help='ID of log analytics workspace.',
                        action='store', dest='azure_workspace')
    parser.add_argument('-aS', metavar='azure_key', type=str, help='Shared key of log analytics workspace.',
                        action='store', dest='azure_key')
    parser.add_argument('-g', help='Output to graylog.', action='store_true', dest='graylog')
    parser.add_argument('-gA', metavar='graylog_address', type=str, help='Address of graylog server.', action='store',
                        dest='graylog_addr')
    parser.add_argument('-gP', metavar='graylog_port', type=str, help='Port of graylog server.', action='store',
                        dest='graylog_port')
    args = parser.parse_args()
    argsdict = vars(args)

    if argsdict['interactive_subscriber']:
        subscriber = AuditLogSubscriber.AuditLogSubscriber(
            tenant_id=argsdict['tenant_id'], secret_key=argsdict['secret_key'], client_key=argsdict['client_key'])
        subscriber.interactive()
        quit(0)

    content_types = []
    if argsdict['general']:
        content_types.append('Audit.General')
    if argsdict['exchange']:
        content_types.append('Audit.Exchange')
    if argsdict['sharepoint']:
        content_types.append('Audit.Sharepoint')
    if argsdict['azure_ad']:
        content_types.append('Audit.AzureActiveDirectory')
    if argsdict['dlp']:
        content_types.append('DLP.All')

    fallback_time = None
    if argsdict['time_hours']:
        fallback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=argsdict['time_hours'])

    collector = AuditLogCollector(
        tenant_id=argsdict['tenant_id'], secret_key=argsdict['secret_key'], client_key=argsdict['client_key'],
        content_types=content_types, publisher_id=argsdict['publisher_id'], resume=argsdict['resume'],
        fallback_time=fallback_time, skip_known_logs=argsdict['skip_known_logs'], log_path=argsdict['log_path'],
        file_output=argsdict['file'], path=argsdict['output_path'], debug=argsdict['debug_logging'],
        prtg_output=argsdict['prtg'],
        azure_oms_output=argsdict['azure'], workspace_id=argsdict['azure_workspace'],
        shared_key=argsdict['azure_key'],
        gl_address=argsdict['graylog_addr'], gl_port=argsdict['graylog_port'],
        graylog_output=argsdict['graylog'])
    if argsdict['config']:
        collector.load_config(path=argsdict['config'])
    collector.init_logging()
    collector.run_once()


