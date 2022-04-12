# Standard libs
import os
import sys
import yaml
import json
import logging
import datetime
import argparse
import dateutil.parser
import collections
import threading
# Internal libs
import AzureOMSInterface, GraylogInterface, PRTGInterface
import ApiConnection


class AuditLogCollector(ApiConnection.ApiConnection):

    def __init__(self, *args, content_types=None, resume=True, fallback_time=None, skip_known_logs=True,
                 file_output=False, output_path=None,
                 graylog_output=False, graylog_address=None, graylog_port=None,
                 azure_oms_output=False, azure_oms_workspace_id=None, azure_oms_shared_key=None,
                 prtg_output=False,
                 **kwargs):
        """
        Object that can retrieve all available content blobs for a list of content types and then retrieve those
        blobs and output them to a file or Graylog input (i.e. send over a socket).
        :param content_types: list of content types to retrieve (e.g. 'Audit.Exchange', 'Audit.Sharepoint')
        :param resume: Resume from last known run time for each content type (Bool)
        :param fallback_time: if no last run times are found to resume from, run from this start time (Datetime)
        :param file_output: path of file to output audit logs to (str)
        :param output_path: path to output retrieved logs to (None=no file output) (string)
        :param graylog_output: Enable graylog Interface (Bool)
        :param graylog_address: IP/Hostname of Graylog server to output audit logs to (str)
        :param graylog_port: port of Graylog server to output audit logs to (int)
        :param azure_oms_output: Enable Azure workspace analytics OMS Interface (Bool)
        :param azure_oms_workspace_id: Found under "Agent Configuration" blade in Portal (str)
        :param azure_oms_shared_key: Found under "Agent Configuration" blade in Portal(str)
        :param prtg_output: Enable PRTG output (Bool)
                """
        super().__init__(*args, **kwargs)
        self.content_types = content_types or collections.deque()
        self.resume = resume
        if resume:
            self.get_last_run_times()
        self._fallback_time = fallback_time or datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=23)
        self.skip_known_logs = skip_known_logs
        self.filters = self.load_filters() or {}
        self.file_output = file_output
        self.output_path = output_path
        self.azure_oms_output = azure_oms_output
        self._azure_oms_interface = AzureOMSInterface.AzureOMSInterface(workspace_id=azure_oms_workspace_id,
                                                                        shared_key=azure_oms_shared_key)
        self.graylog_output = graylog_output
        self._graylog_interface = GraylogInterface.GraylogInterface(graylog_address=graylog_address,
                                                                    graylog_port=graylog_port)
        self.prtg_output = prtg_output
        self._prtg_interface = PRTGInterface.PRTGInterface(config=self.load_prtg_config() if prtg_output else {})

        self._last_run_times = {}
        self._known_content = {}
        self._known_logs = {}

        self.blobs_to_collect = collections.defaultdict(collections.deque)
        self.monitor_thread = threading.Thread()
        self.retrieve_available_content_threads = collections.deque()
        self.retrieve_content_threads = collections.deque()
        self.run_started = None
        self.logs_retrieved = 0

    def run_once(self, start_time=None):
        """
        Check available content and retrieve it, then exit.
        """
        if self.skip_known_logs:
            self._known_content.clear()
            self._known_logs.clear()
            self._clean_known_content()
            self._clean_known_logs()
        self.logs_retrieved = 0
        self._graylog_interface.successfully_sent = 0
        self._graylog_interface.unsuccessfully_sent = 0
        self._azure_oms_interface.successfully_sent = 0
        self._azure_oms_interface.unsuccessfully_sent = 0
        self.run_started = datetime.datetime.now()
        if self.resume:
            self.get_last_run_times()
        self.start_monitoring()
        self.get_all_available_content(start_time=start_time)
        self.monitor_thread.join()
        if self.skip_known_logs:
            self._add_known_log()
            self._add_known_content()
        if self.resume and self._last_run_times:
            with open('last_run_times', 'w') as ofile:
                json.dump(fp=ofile, obj=self._last_run_times)
        if self.prtg_output:
            self._prtg_interface.output()
        logging.info("Finished. Total logs retrieved: {}. Run time: {}.".format(
            self.logs_retrieved, datetime.datetime.now() - self.run_started))
        if self.azure_oms_output:
            logging.info("Azure OMS output report: {} successfully sent, {} errors".format(
                self._azure_oms_interface.successfully_sent, self._azure_oms_interface.unsuccessfully_sent))
        if self.graylog_output:
            logging.info("Graylog output report: {} successfully sent, {} errors".format(
                self._graylog_interface.successfully_sent, self._graylog_interface.unsuccessfully_sent))

    def load_filters(self):

        if os.path.exists('filters.yaml'):
            with open('filters.yaml', 'r') as ofile:
                return yaml.safe_load(ofile)

    def load_prtg_config(self):

        if os.path.exists('PrtgConfig.yaml'):
            with open('PrtgConfig.yaml', 'r') as ofile:
                return yaml.safe_load(ofile)
        else:
            raise RuntimeError("PRTG output needs 'PrtgConfig.yaml")

    def get_last_run_times(self):

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

    def start_monitoring(self):

        self.monitor_thread = threading.Thread(target=self.monitor_blobs_to_collect, daemon=True)
        self.monitor_thread.start()

    def stop_monitoring(self):

        self.monitor_thread.join()

    def get_all_available_content(self, start_time=None):
        """
        Make a call to retrieve avaialble content blobs for a content type in a thread.
        """
        for content_type in self.content_types.copy():
            if not start_time:
                if self.resume and content_type in self._last_run_times.keys():
                    start_time = self._last_run_times[content_type]
                else:
                    start_time = self._fallback_time
            self.retrieve_available_content_threads.append(threading.Thread(
                target=self.get_available_content, daemon=True,
                kwargs={'content_type': content_type, 'start_time': start_time}))
            self.retrieve_available_content_threads[-1].start()

    def get_available_content(self, content_type, start_time):
        """
        Make a call to retrieve avaialble content blobs for a content type in a thread. If the response contains a
        'NextPageUri' there is more content to be retrieved; rerun until all has been retrieved.
        """
        try:
            logging.log(level=logging.DEBUG, msg='Getting available content for type: "{0}"'.format(content_type))
            current_time = datetime.datetime.now(datetime.timezone.utc)
            formatted_end_time = str(current_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
            formatted_start_time = str(start_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
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

    def monitor_blobs_to_collect(self):
        """
        Wait for the 'retrieve_available_content' function to retrieve content URI's. Once they become available
        start retrieving in a background thread.
        """
        if self.azure_oms_output:
            self._azure_oms_interface.start()
        if self.prtg_output:
            self._prtg_interface.start()
        if self.graylog_output:
            self._graylog_interface.start()
        threads = collections.deque()
        while True:
            threads = [thread for thread in threads if thread.is_alive()]
            if self.done_collecting_available_content and self.done_retrieving_content and not threads:
                break
            if not self.blobs_to_collect:
                continue
            for content_type, blobs_to_collect in self.blobs_to_collect.copy().items():
                if self.blobs_to_collect[content_type]:
                    blob_json = self.blobs_to_collect[content_type].popleft()
                    if blob_json and 'contentUri' in blob_json:
                        logging.log(level=logging.DEBUG, msg='Retrieving content blob: "{0}"'.format(blob_json))
                        threads.append(threading.Thread(
                            target=self.retrieve_content, daemon=True,
                            kwargs={'content_json': blob_json, 'content_type': content_type}))
                        threads[-1].start()
        if self.azure_oms_output:
            self._azure_oms_interface.stop()
        if self.prtg_output:
            self._prtg_interface.stop()
        if self.graylog_output:
            self._graylog_interface.stop()

    def retrieve_content(self, content_json, content_type):
        """
        Get an available content blob. If it exists in the list of known content blobs it is skipped to ensure
        idempotence.
        :param content_json: JSON dict of the content blob as retrieved from the API (dict)
        :param content_type: Type of API being retrieved for, e.g. 'Audit.Exchange' (str)
        :return:
        """
        if self.skip_known_logs and self.known_content and content_json['contentId'] in self.known_content:
            return
        try:
            result = self.make_api_request(url=content_json['contentUri'], append_url=False).json()
            if not result:
                return
        except Exception as e:
            logging.error("Error retrieving content: {}".format(e))
            return
        else:
            if self.skip_known_logs:
                self._known_content[content_json['contentId']] = content_json['contentExpiration']
            for log in result.copy():
                if self.filters and not self.check_filters(log=log, content_type=content_type):
                    result.remove(log)
                elif self.skip_known_logs:
                    if log['Id'] in self.known_logs:
                        return
                    self.known_logs[log['Id']] = log['CreationTime']
            self.logs_retrieved += len(result)
            if self.file_output:
                self.output_results_to_file(results=result)
            if self.prtg_output:
                self._prtg_interface.send_messages_to_prtg(*result, content_type=content_type)
            if self.graylog_output:
                self._graylog_interface.send_messages_to_graylog(*result, content_type=content_type)
            if self.azure_oms_output:
                self._azure_oms_interface.send_messages_to_oms(*result, content_type=content_type)

    def check_filters(self, log, content_type):

        if content_type in self.filters and self.filters[content_type]:
            for log_filter_key, log_filter_value in self.filters[content_type].items():
                if log_filter_key not in log or log[log_filter_key].lower() != log_filter_value.lower():
                    return False
        return True

    def output_results_to_file(self, results):
        """
        Dump received JSON messages to a file.
        :param results: retrieved JSON (dict)
        """
        with open(self.output_path, 'a') as ofile:
            ofile.write("{}\n".format(json.dump(obj=results, fp=ofile)))

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
        if not self._known_logs:
            if os.path.exists('known_logs'):
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
        if not self._known_content and not os.path.exists('known_content'):
            return {}
        if not self._known_content:
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
    parser.add_argument('-tD', metavar='time_days', type=int, help='Amount of days to go back and look for content',
                        action='store', dest='time_days')
    parser.add_argument('-s',
                        help='Keep track of each retrieved log ID and skip it in the future to prevent duplicates',
                        action='store_true', dest='skip_known_logs')
    parser.add_argument('-l', metavar='log_path', type=str, help='Path of log file', action='store', dest='log_path',
                        default=os.path.join(os.path.dirname(__file__), 'AuditLogCollector.log'))
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
    parser.add_argument('-d', action='store_true', dest='debug_logging',
                        help='Enable debug logging (generates large log files and decreases performance).')
    args = parser.parse_args()
    argsdict = vars(args)

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
    if argsdict['time_days']:
        fallback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=argsdict['time_days'])
    elif argsdict['time_hours']:
        fallback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=argsdict['time_hours'])

    logger = logging.getLogger()
    fileHandler = logging.FileHandler(argsdict['log_path'], mode='w')
    streamHandler = logging.StreamHandler(sys.stdout)
    if not argsdict['prtg']:
        logger.addHandler(streamHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(logging.INFO if not argsdict['debug_logging'] else logging.DEBUG)
    logging.log(level=logging.INFO, msg='Starting run @ {0}'.format(datetime.datetime.now()))

    collector = AuditLogCollector(
        tenant_id=argsdict['tenant_id'], secret_key=argsdict['secret_key'], client_key=argsdict['client_key'],
        content_types=content_types, publisher_id=argsdict['publisher_id'], resume=argsdict['resume'],
        fallback_time=fallback_time, skip_known_logs=argsdict['skip_known_logs'],
        file_output=argsdict['file'], output_path=argsdict['output_path'],
        prtg_output=argsdict['prtg'],
        azure_oms_output=argsdict['azure'], azure_oms_workspace_id=argsdict['azure_workspace'],
        azure_oms_shared_key=argsdict['azure_key'],
        graylog_address=argsdict['graylog_addr'], graylog_port=argsdict['graylog_port'],
        graylog_output=argsdict['graylog'])
    collector.run_once()


