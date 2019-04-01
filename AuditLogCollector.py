#!/usr/bin/python3

# Standard libs
import os
import json
import logging
import datetime
import argparse
import dateutil.parser
from collections import deque
import threading
# Internal libs
import GraylogInterface
import ApiConnection


class AuditLogCollector(ApiConnection.ApiConnection):

    def __init__(self, output_path, content_types, *args, graylog_address=None, graylog_port=None, graylog_output=False,
                 file_output=False, **kwargs):
        """
        Object that can retrieve all available content blobs for a list of content types and then retrieve those
        blobs and output them to a file or Graylog input (i.e. send over a socket).
        :param output_path: path to output retrieved logs to (None=no file output) (string)
        :param content_types: list of content types to retrieve (e.g. 'Audit.Exchange', 'Audit.Sharepoint')
        :param graylog_address: IP/Hostname of Graylog server to output audit logs to (str)
        :param graylog_port: port of Graylog server to output audit logs to (int)
        :param file_output: path of file to output audit logs to (str)
        """
        super().__init__(*args, **kwargs)
        self.file_output = file_output
        self.graylog_output = graylog_output
        self.output_path = output_path
        self.content_types = content_types
        self._known_content = {}
        self._graylog_interface = GraylogInterface.GraylogInterface(graylog_address=graylog_address,
                                                                    graylog_port=graylog_port)

        self.blobs_to_collect = deque()
        self.monitor_thread = threading.Thread()
        self.retrieve_available_content_threads = deque()
        self.retrieve_content_threads = deque()

    def run_once(self):
        """
        Check available content and retrieve it, then exit.
        """
        self._clean_known_content()
        self.start_monitoring()
        self.get_all_available_content()
        self.monitor_thread.join()

    @property
    def done_retrieving_content(self):

        return not bool(self.blobs_to_collect)

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

    def get_all_available_content(self):
        """
        Make a call to retrieve avaialble content blobs for a content type in a thread.
        """
        for content_type in self.content_types.copy():
            self.retrieve_available_content_threads.append(threading.Thread(
                target=self.get_available_content, daemon=True, kwargs={'content_type': content_type}))
            self.retrieve_available_content_threads[-1].start()

    def get_available_content(self, content_type):
        """
        Make a call to retrieve avaialble content blobs for a content type in a thread. If the response contains a
        'NextPageUri' there is more content to be retrieved; rerun until all has been retrieved.
        """
        logging.log(level=logging.DEBUG, msg='Getting available content for type: "{0}"'.format(content_type))
        current_time = datetime.datetime.now(datetime.timezone.utc)
        end_time = str(current_time).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
        start_time = str(current_time - datetime.timedelta(hours=1)).replace(' ', 'T').rsplit('.', maxsplit=1)[0]
        response = self.make_api_request(url='subscriptions/content?contentType={0}&startTime={1}&endTime={2}'.format(
            content_type, start_time, end_time))
        self.blobs_to_collect += response.json()
        while 'NextPageUri' in response.headers.keys() and response.headers['NextPageUri']:
            logging.log(level=logging.DEBUG, msg='Getting next page of content for type: "{0}"'.format(content_type))
            self.blobs_to_collect += response.json()
            response = self.make_api_request(url=response.headers['NextPageUri'], append_url=False)
        self.content_types.remove(content_type)
        logging.log(level=logging.DEBUG, msg='Got {0} content blobs of type: "{1}"'.format(
            len(self.blobs_to_collect), content_type))

    def monitor_blobs_to_collect(self):
        """
        Wait for the 'retrieve_available_content' function to retrieve content URI's. Once they become available
        start retrieving in a background thread.
        """
        self._graylog_interface.start()
        threads = deque()
        while not (self.done_collecting_available_content and self.done_retrieving_content):
            if not self.blobs_to_collect:
                continue
            blob_json = self.blobs_to_collect.popleft()
            if blob_json and 'contentUri' in blob_json:
                logging.log(level=logging.DEBUG, msg='Retrieving content blob: "{0}"'.format(blob_json))
                threads.append(threading.Thread(target=self.retrieve_content, daemon=True,
                    kwargs={'content_json': blob_json, 'save_as_file': self.file_output}))
                threads[-1].start()
        self._graylog_interface.stop()

    def retrieve_content(self, content_json, send_to_graylog=True, save_as_file=False):
        """
        Get an available content blob. If it exists in the list of known content blobs it is skipped to ensure
        idempotence.
        :param content_json: JSON dict of the content blob as retrieved from the API (dict)
        :param send_to_graylog: send the messages to a Graylog input after receiving (Bool)
        :param save_as_file: save the messages to a file after receiving (Bool)
        :return:
        """
        if self.known_content and content_json['contentId'] in self.known_content:
            return
        try:
            result = self.make_api_request(url=content_json['contentUri'], append_url=False).json()
            if not result:
                return
        except:
            return
        else:
            self._add_known_content(content_id=content_json['contentId'],
                                    content_expiration=content_json['contentExpiration'])
            if save_as_file:
                self.output_results_to_file(results=result, content_id=content_json['contentId'])
            if send_to_graylog:
                self._graylog_interface.send_messages_to_graylog(*result)

    def output_results_to_file(self, results, content_id):
        """
        Dump received JSON messages to a file.
        :param results: retrieved JSON (dict)
        :param content_id: ID of the content blob to avoid duplicates (string)
        """
        if not os.path.exists(self.output_path):
            os.mkdir(self.output_path)
        with open(os.path.join(self.output_path, str(content_id)), 'w') as ofile:
            json.dump(obj=results, fp=ofile)

    def _add_known_content(self, content_id, content_expiration):
        """
        Add a content ID to the known content file to avoid saving messages more than once.
        :param content_id: string
        :param content_expiration: date string
        :return:
        """
        with open('known_content', 'a') as ofile:
            ofile.write('\n{0},{1}'.format(content_id, content_expiration))

    def _clean_known_content(self):
        """
        Remove any known content ID's that have expired. Can't download a duplicate if it is not available for
        download.
        """
        if os.path.exists('known_content'):
            known_contents = self.known_content
            os.remove('known_content')
            for id, expire_date in known_contents.items():
                date = dateutil.parser.parse(expire_date)
                if datetime.datetime.now(datetime.timezone.utc) < date:
                    self._add_known_content(content_id=id, content_expiration=expire_date)

    @property
    def known_content(self):
        """
        Parse and return known content file.
        :return: {content_id: content_expiration_date} dict
        """
        if not os.path.exists('known_content'):
            return
        if not self._known_content:
            with open('known_content', 'r') as ofile:
                for line in ofile.readlines():
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
    parser.add_argument('-l', metavar='log_path', type=str, help='Path of log file', action='store', dest='log_path',
                        default=os.path.join(os.path.dirname(__file__), 'AuditLogCollector.log'))
    parser.add_argument('-f', help='Output to file.', action='store_true', dest='file')
    parser.add_argument('-fP', metavar='file_output_path', type=str, help='Path of directory of output files',
                        default=os.path.join(os.path.dirname(__file__), 'output'), action='store',
                        dest='output_path')
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

    logging.basicConfig(filemode='w', filename=argsdict['log_path'],
                        level=logging.INFO if not argsdict['debug_logging'] else logging.DEBUG)
    logging.log(level=logging.INFO, msg='Starting run @ {0}'.format(datetime.datetime.now()))

    collector = AuditLogCollector(output_path=argsdict['output_path'], tenant_id=argsdict['tenant_id'],
                                  secret_key=argsdict['secret_key'], client_key=argsdict['client_key'],
                                  content_types=content_types, graylog_address=argsdict['graylog_addr'],
                                  graylog_port=argsdict['graylog_port'], graylog_output=argsdict['graylog'],
                                  file_output=argsdict['file'], publisher_id=argsdict['publisher_id'])
    collector.run_once()


