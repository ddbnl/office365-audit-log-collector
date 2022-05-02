from Interfaces import AzureOMSInterface, SqlInterface, GraylogInterface, PRTGInterface, FileInterface, \
    AzureTableInterface, AzureBlobInterface, FluentdInterface
import alc  # Rust based log collector Engine
import AuditLogSubscriber
import ApiConnection
import os
import sys
import yaml
import time
import json
import signal
import logging
import datetime
import argparse
import collections
import threading

# Azure logger is very noisy on INFO
az_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
az_logger.setLevel(logging.WARNING)


class AuditLogCollector(ApiConnection.ApiConnection):

    def __init__(self, config_path, **kwargs):
        """
        Object that can retrieve all available content blobs for a list of content types and then retrieve those logs
        and send them to a variety of outputs.
        """
        super().__init__(**kwargs)
        self.config = Config(path=config_path)
        self.interfaces = {}
        self._register_interfaces(**kwargs)
        self._init_logging()

        self._last_run_times = {}
        self._known_content = {}
        self._known_logs = {}
        self._force_stop = False

        self._remaining_content_types = collections.deque()
        self.blobs_to_collect = collections.defaultdict(collections.deque)
        self.monitor_thread = threading.Thread()
        self.retrieve_available_content_threads = collections.deque()
        self.retrieve_content_threads = collections.deque()
        self.run_started = None
        self.logs_retrieved = 0
        self.errors_retrieving = 0
        self.retries = 0

    def force_stop(self, *args):

        self._force_stop = True
        logging.info("Got a SIGINT, stopping..")
        self.monitor_thread.join(timeout=10)
        sys.exit(0)

    def run(self):
        if not self.config['collect', 'schedule']:
            self.run_once()
        else:
            self.run_scheduled()

    def run_once(self):
        """
        Check available content and retrieve it, then exit.
        """
        self._prepare_to_run()
        logging.log(level=logging.INFO, msg='Starting run @ {}. Content: {}.'.format(
            datetime.datetime.now(), self.config['collect', 'contentTypes']))
        if not self.config['collect', 'rustEngine'] is False:
            self._start_interfaces()
            self.receive_results_from_rust_engine()
            self._stop_interfaces(force=False)
        else:
            self._start_monitoring()
            self._get_all_available_content()
            while self.monitor_thread.is_alive():
                self.monitor_thread.join(1)
        self._finish_run()

    def receive_results_from_rust_engine(self):

        runs = self._get_needed_runs(content_types=self.config['collect', 'contentTypes'].copy())
        engine = alc.RustEngine(self.tenant_id, self.client_key, self.secret_key, self.publisher_id or self.tenant_id,
                                self.config['collect', 'contentTypes'], runs,
                                self.config['collect', 'maxThreads'] or 50,
                                self.config['collect', 'retries'] or 3)
        engine.run_once()
        last_received = datetime.datetime.now()
        while True:
            try:
                result = engine.get_result()
            except ValueError:  # RustEngine throws this error when no logs are in the results recv queue
                now = datetime.datetime.now()
                if now - last_received > datetime.timedelta(seconds=60):
                    logging.error("Timed out waiting for results from engine")
                    break
                last_received = now
            except EOFError:  # RustEngine throws this error when all content has been retrieved
                logging.info("Rust engine finished receiving all content")
                break
            else:
                content_json, content_id, content_expiration, content_type = result
                self._handle_retrieved_content(content_id=content_id, content_expiration=content_expiration,
                                               content_type=content_type, results=json.loads(content_json))
                self.logs_retrieved += 1
        _, _, self.retries, self.errors_retrieving = engine.stop()

    def run_scheduled(self):
        """
        Run according to the schedule set in the config file. Collector will not exit unless manually stopped.
        """
        if not self.run_started:  # Run immediately initially
            target_time = datetime.datetime.now()
        else:
            days, hours, minutes = self.config['collect', 'schedule']
            target_time = self.run_started + datetime.timedelta(days=days, hours=hours, minutes=minutes)
            if datetime.datetime.now() > target_time:
                logging.warning("Warning: last run took longer than the scheduled interval.")
        logging.info("Next run is scheduled for: {}.".format(target_time))
        while True:
            if datetime.datetime.now() > target_time:
                self.run_once()
                self.run_scheduled()
            else:
                time.sleep(1)

    def _register_interfaces(self, **kwargs):

        for interface in [FileInterface.FileInterface, AzureTableInterface.AzureTableInterface,
                          AzureBlobInterface.AzureBlobInterface, AzureOMSInterface.AzureOMSInterface,
                          SqlInterface.SqlInterface, GraylogInterface.GraylogInterface, PRTGInterface.PRTGInterface,
                          FluentdInterface.FluentdInterface]:
            self.interfaces[interface] = interface(collector=self, **kwargs)

    @property
    def _all_enabled_interfaces(self):

        return [interface for interface in self.interfaces.values() if interface.enabled]

    def _init_logging(self):
        """
        Start logging to file and console. If PRTG output is enabled do not log to console, as this will interfere with
        the sensor result.
        """
        logger = logging.getLogger()
        file_handler = logging.FileHandler(self.config['log', 'path'].strip("'") if self.config['log', 'path']
                                           else 'collector.log', mode='w')
        if not self.interfaces[PRTGInterface.PRTGInterface].enabled:
            stream_handler = logging.StreamHandler(sys.stdout)
            logger.addHandler(stream_handler)
        logger.addHandler(file_handler)
        logger.setLevel(logging.INFO if not self.config['log', 'debug'] else logging.DEBUG)

    def _prepare_to_run(self):
        """
        Make sure that self.run_once can be called multiple times by resetting to an initial state.
        """
        self.config.load_config()
        self._remaining_content_types = self.config['collect', 'contentTypes'] or collections.deque()
        if self.config['collect', 'autoSubscribe']:
            self._auto_subscribe()
        if self.config['collect', 'resume']:
            self._get_last_run_times()
        if self.config['collect', 'skipKnownLogs']:
            self._known_content.clear()
            self._known_logs.clear()
            self._clean_known_content()
            self._clean_known_logs()
        self.logs_retrieved = 0
        for interface in self._all_enabled_interfaces:
            interface.reset()
        self.run_started = datetime.datetime.now()

    def _finish_run(self):
        """
        Save relevant information and output PRTG result if the interface is enabled. The other interfaces output
        while collecting.
        """
        if self.config['collect', 'skipKnownLogs']:
            self._add_known_log()
            self._add_known_content()
        if self.config['collect', 'resume'] and self._last_run_times:
            with open('last_run_times', 'w') as ofile:
                json.dump(fp=ofile, obj=self._last_run_times)
        self._log_statistics()

    def _log_statistics(self):
        """
        Write run statistics to log file / console.
        """
        logging.info("Finished. Total logs retrieved: {}. Total retries: {}. Total logs with errors: {}. Run time: {}."
            .format(self.logs_retrieved, self.retries, self.errors_retrieving, datetime.datetime.now() - self.run_started))
        for interface in self._all_enabled_interfaces:
            logging.info("{} reports: {} successfully sent, {} errors".format(
                interface.__class__.__name__, interface.successfully_sent, interface.unsuccessfully_sent))

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
                    self._last_run_times[content_type] = datetime.datetime.strptime(last_run_time, "%Y-%m-%dT%H:%M:%S%z")
                except Exception as e:
                    logging.error("Could not read last run time for content type {}: {}.".format(content_type, e))
                    del self._last_run_times[content_type]

    @property
    def _done_retrieving_content(self):
        """
        Returns True if there are no more content blobs to be collected. Used to determine when to exit the script.
        :return: Bool
        """
        for content_type in self.blobs_to_collect:
            if self.blobs_to_collect[content_type]:
                return False
        return True

    @property
    def _done_collecting_available_content(self):
        """
        Once a call is made to retrieve content for a particular type, and there is no 'NextPageUri' in the response,
        the type is removed from 'self.content_types' to signal that all available content has been retrieved for that
        type.
        """
        return not bool(self._remaining_content_types)

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
        unsubscribed_content_types = self._remaining_content_types.copy()
        for s in status:
            if isinstance(s, str):  # For issue #18
                raise RuntimeError("Auto subscribe enabled but could not get subscription status")
            if s['contentType'] in self._remaining_content_types and s['status'].lower() == 'enabled':
                unsubscribed_content_types.remove(s['contentType'])
        for content_type in unsubscribed_content_types:
            logging.info("Auto subscribing to: {}".format(content_type))
            subscriber.set_sub_status(content_type=content_type, action='start')

    def _get_needed_runs(self, content_types):
        """
        Return the start- and end times needed to retrieve content for each content type. If the timespan to retrieve
        logs for exceeds 24 hours, we need to split it up into 24 hour runs (limit by Office API).
        """
        runs = {}
        end_time = datetime.datetime.now(datetime.timezone.utc)
        for content_type in content_types:
            runs[content_type] = []
            if self.config['collect', 'resume'] and content_type in self._last_run_times.keys():
                start_time = self._last_run_times[content_type]
                logging.info("{} - resuming from: {}".format(content_type, start_time))
            else:
                hours_to_collect = self.config['collect', 'hoursToCollect'] or 24
                start_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours_to_collect)

            if end_time - start_time > datetime.timedelta(hours=168):
                logging.warning("Hours to collect cannot be more than 168 due to Office API limits, defaulting to 168")
                end_time = start_time + datetime.timedelta(hours=168)
            while True:
                if end_time - start_time > datetime.timedelta(hours=24):
                    split_start_time = start_time
                    split_end_time = start_time + datetime.timedelta(hours=24)
                    formatted_start_time = str(split_start_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
                    formatted_end_time = str(split_end_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
                    runs[content_type].append((formatted_start_time, formatted_end_time))
                    start_time = split_end_time
                    self._remaining_content_types.append(content_type)
                else:
                    formatted_start_time = str(start_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
                    formatted_end_time = str(end_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
                    runs[content_type].append((formatted_start_time, formatted_end_time))
                    break
            self._last_run_times[content_type] = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return runs

    def _get_all_available_content(self):
        """
        Start a thread to retrieve available content blobs for each content type to be collected.
        """
        runs = self._get_needed_runs(content_types=self._remaining_content_types.copy())
        for content_type, run_dates in runs.items():
            for run_date in run_dates:
                start_time, end_time = run_date
                self._start_get_available_content_thread(
                    content_type=content_type, start_time=start_time, end_time=end_time)

    def _start_get_available_content_thread(self, content_type, start_time, end_time):

        self.retrieve_available_content_threads.append(threading.Thread(
            target=self._get_available_content, daemon=True,
            kwargs={'content_type': content_type, 'start_time': start_time, 'end_time': end_time}))
        self.retrieve_available_content_threads[-1].start()

    def _get_available_content(self, content_type, start_time, end_time):
        """
        Retrieve available content blobs for a content type. If the response contains a
        'NextPageUri' there is more content to be retrieved; rerun until all has been retrieved.
        """
        try:
            logging.log(level=logging.DEBUG, msg='Getting available content for type: "{}"'.format(content_type))
            logging.info("Retrieving {}. Start time: {}. End time: {}.".format(
                content_type, start_time, end_time))
            response = self.make_api_request(url='subscriptions/content?contentType={0}&startTime={1}&endTime={2}'.
                                                 format(content_type, start_time, end_time))
            self.blobs_to_collect[content_type] += response.json()
            while 'NextPageUri' in response.headers.keys() and response.headers['NextPageUri']:
                logging.log(level=logging.DEBUG, msg='Getting next page of content for type: "{0}"'.
                            format(content_type))
                self.blobs_to_collect[content_type] += response.json()
                response = self.make_api_request(url=response.headers['NextPageUri'], append_url=False)
            logging.log(level=logging.DEBUG, msg='Got {0} content blobs of type: "{1}"'.format(
                len(self.blobs_to_collect[content_type]), content_type))
        except Exception as e:
            logging.log(level=logging.WARN, msg="Error while getting available content: {}: {}".format(
                content_type, e))
            self._remaining_content_types.remove(content_type)
        else:
            self._remaining_content_types.remove(content_type)

    def _start_interfaces(self):

        for interface in self._all_enabled_interfaces:
            interface.start()

    def _stop_interfaces(self, force):

        for interface in self._all_enabled_interfaces:
            interface.stop(gracefully=not force)

    def _monitor_blobs_to_collect(self):
        """
        Wait for the 'retrieve_available_content' function to retrieve content URI's. Once they become available
        start retrieving in a background thread.
        """
        self._start_interfaces()
        threads = collections.deque()
        while True:
            threads = [thread for thread in threads if thread.is_alive()]
            if self._force_stop or \
                    (self._done_collecting_available_content and self._done_retrieving_content and not threads):
                break
            for content_type, blobs_to_collect in self.blobs_to_collect.copy().items():
                if len(threads) >= (self.config['collect', 'maxThreads'] or 50):
                    break
                if self.blobs_to_collect[content_type]:
                    blob_json = self.blobs_to_collect[content_type].popleft()
                    self._collect_blob(blob_json=blob_json, content_type=content_type, threads=threads)
        self._stop_interfaces(force=self._force_stop)

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
                kwargs={'content_json': blob_json, 'content_type': content_type,
                        'retries': self.config['collect', 'retries'] or 3}))
            threads[-1].start()

    def _retrieve_content(self, content_json, content_type, retries):
        """
        Get an available content blob. If it exists in the list of known content blobs it is skipped to ensure
        idempotence.
        :param content_json: JSON dict of the content blob as retrieved from the API (dict)
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :param retries: Times to retry retrieving a content blob if it fails (int)
        """
        if self.config['collect', 'skipKnownLogs'] and self.known_content and \
                content_json['contentId'] in self.known_content:
            return
        try:
            results = self.make_api_request(url=content_json['contentUri'], append_url=False).json()
            if not results:
                return
        except Exception as e:
            if retries:
                self.retries += 1
                time.sleep(self.config['collect', 'retryCooldown'] or 3)
                return self._retrieve_content(content_json=content_json, content_type=content_type, retries=retries - 1)
            else:
                self.errors_retrieving += 1
                logging.error("Error retrieving content: {}".format(e))
                return
        else:
            self._handle_retrieved_content(
                content_id=content_json['contentId'], content_expiration=content_json['contentExpiration'],
                content_type=content_type, results=results)

    def _handle_retrieved_content(self, content_id, content_expiration, content_type, results):
        """
        Check known logs, filter results and output what remains.
        :param content_id: ID of content blob from API (str)
        :param content_expiration: date string of expiration of content blob from API (str)
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :param results: list of JSON
        """
        if self.config['collect', 'skipKnownLogs']:
            self._known_content[content_id] = content_expiration
        for log in results.copy():
            if self.config['collect', 'skipKnownLogs']:
                if log['Id'] in self.known_logs:
                    results.remove(log)
                    continue
                self.known_logs[log['Id']] = log['CreationTime']
            if self.config['collect', 'filter'] and not self._check_filters(log=log, content_type=content_type):
                results.remove(log)
        self.logs_retrieved += len(results)
        self._output_results(results=results, content_type=content_type)

    def _output_results(self, results, content_type):
        """
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :param results: list of JSON
        """
        for interface in self._all_enabled_interfaces:
            interface.send_messages(*results, content_type=content_type)

    def _check_filters(self, log, content_type):
        """
        :param log: JSON
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :return: True if log matches filter, False if not (Bool)
        """
        filters = self.config['collect', 'filter']
        if content_type in filters and filters[content_type]:
            for log_filter_key, log_filter_value in filters[content_type].items():
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


class Config(object):

    def __init__(self, path=None, config=None):
        """
        Wrapper around the YAML config necessary to run the collector.
        :param path: str
        :param config: loaded YAML
        """
        self._config = config
        self.path = path
        self.requires_parsing = {  # Any settings that require additional parsing go here, with their parsing func
            ('collect', 'contentTypes'): self.parse_content_types,
            ('collect', 'schedule'): self.parse_schedule
        }
        self._cache = {}

    @property
    def config(self):

        if not self._config:
            self.load_config()
        return self._config

    def load_config(self, path=None):
        """
        Load a YML config containing settings for this collector and its' interfaces.
        :param path: str
        """
        self._cache.clear()
        path = path or self.path
        with open(path, 'r') as ofile:
            self._config = yaml.safe_load(ofile)

    def __getitem__(self, item):
        """
        Allow searching config settings by e.g. config['Setting1', 'Subsetting2'].
        :param item:
        :return:
        """
        if item not in self._cache:
            if item in self.requires_parsing:
                self._cache[item] = self.requires_parsing[item]()
            else:
                self._cache[item] = self._find_setting(*item)
        return self._cache[item]

    def _find_setting(self, *setting):
        """
        Try to find a setting in a passed settings path (e.g. 'Main, Setting1, SubSetting1'). Return None if it does not
        exist (allows using config files without all settings defined).
        :param setting: tuple of settings path
        :return: setting in YAML config (int/str/bool)
        """
        root = self.config
        for child in setting:
            if child in root:
                root = root[child]
            else:
                return
        return root

    @property
    def all_content_types(self):
        """
        :return: list of str
        """
        return ['Audit.General', 'Audit.AzureActiveDirectory', 'Audit.Exchange', 'Audit.SharePoint', 'DLP.All']

    def parse_schedule(self):
        """
        :return: tuple of ints (days/hours/minutes)
        """
        schedule = self._find_setting('collect', 'schedule')
        if not schedule:
            return
        try:
            schedule = [int(x) for x in schedule.split(' ')]
            assert len(schedule) == 3
        except Exception as e:
            raise RuntimeError(
                "Could not interpret schedule. Make sure it's in the format '0 0 0' (days/hours/minutes) {}"
                    .format(e))
        else:
            return schedule

    def parse_content_types(self):
        """
        :return: list of str
        """
        content_types = collections.deque()
        for content_type in self.all_content_types:
            if self._find_setting('collect', 'contentTypes', content_type):
                content_types.append(content_type)
        return content_types


if __name__ == "__main__":

    description = \
    """
    Retrieve audit log contents from Office 365 API and save to file or other output.
    Example: Retrieve all available content and send it to an output (using mock ID's and keys):
    "AuditLogCollector.py 123 456 789 --general --exchange --azure_ad --sharepoint --dlp -g -gA 10.10.10.1 -gP 5000
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('tenant_id', type=str, help='Tenant ID of Azure AD', action='store')
    parser.add_argument('client_key', type=str, help='Client key of Azure application', action='store')
    parser.add_argument('secret_key', type=str, help='Secret key generated by Azure application', action='store')
    parser.add_argument('--config', metavar='config', type=str, help='Path to YAML config file',
                        action='store', dest='config', required=True)
    parser.add_argument('--table-string', metavar='table_string', type=str,
                        help='Connection string for Azure Table output interface', action='store', dest='table_string')
    parser.add_argument('--blob-string', metavar='blob_string', type=str,
                        help='Connection string for Azure Blob output interface', action='store', dest='blob_string')
    parser.add_argument('--sql-string', metavar='sql_string', type=str,
                        help='Connection string for SQL output interface', action='store', dest='sql_string')
    parser.add_argument('--interactive-subscriber', action='store_true',
                        help='Manually (un)subscribe to audit log feeds', dest='interactive_subscriber')
    parser.add_argument('-p', metavar='publisher_id', type=str, dest='publisher_id',
                        help='Publisher GUID to avoid API throttling. Defaults to Tenant ID', action='store')
    args = parser.parse_args()
    argsdict = vars(args)

    if argsdict['interactive_subscriber']:
        subscriber = AuditLogSubscriber.AuditLogSubscriber(
            tenant_id=argsdict['tenant_id'], secret_key=argsdict['secret_key'], client_key=argsdict['client_key'])
        subscriber.interactive()
        quit(0)

    collector = AuditLogCollector(
        config_path=argsdict['config'],
        tenant_id=argsdict['tenant_id'], secret_key=argsdict['secret_key'], client_key=argsdict['client_key'],
        publisher_id=argsdict['publisher_id'], sql_connection_string=argsdict['sql_string'],
        table_connection_string=argsdict['table_string'], blob_connection_string=argsdict['blob_string'])

    signal.signal(signal.SIGINT, collector.force_stop)
    collector.run()


