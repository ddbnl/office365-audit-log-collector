import logging
import os
from . import _Interface
import collections
import pandas


class FileInterface(_Interface.Interface):

    def __init__(self, **kwargs):
        """
        Interface to send logs to CSV file(s). Not every audit log has every possible column, so columns in the CSV
        file might change over time. Therefore, the CSV file is recreated each time the cache_size is hit to insure
        integrity, taking the performance hit.
        """
        super().__init__(**kwargs)
        self.paths = {}
        self.results_cache = collections.defaultdict(collections.deque)

    @property
    def enabled(self):

        return self.collector.config['output', 'file', 'enabled']

    @property
    def total_cache_length(self):

        return sum([len(self.results_cache[k]) for k in self.results_cache.keys()])

    @property
    def separate_by_content(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: bool
        """
        return self.collector.config['output', 'file', 'separateByContentType']

    @property
    def separator(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: str
        """
        return self.collector.config['output', 'file', 'separator']

    @property
    def path(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: str
        """
        return self.collector.config['output', 'file', 'path']

    @property
    def cache_size(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: int
        """
        return self.collector.config['output', 'file', 'cacheSize']

    def _path_for(self, content_type):

        if content_type not in self.paths:
            if not self.separate_by_content:
                self.paths[content_type] = self.path or 'output.csv'
            else:
                path, file_name = os.path.split(self.path or 'output.csv')
                file_name = file_name.strip('.csv')
                file_name = "{}_{}.csv".format(file_name, content_type.replace('.', ''))
                self.paths[content_type] = os.path.join(path, file_name)
        return self.paths[content_type]

    def _send_message(self, msg, content_type, **kwargs):

        self.results_cache[content_type].append(msg)
        if self.total_cache_length >= (self.cache_size or 500000):
            self._process_caches()

    def exit_callback(self):

        self._process_caches()

    def _process_caches(self):

        for content_type in self.results_cache.keys():
            self._process_cache(content_type=content_type)

    def _process_cache(self, content_type):

        amount = len(self.results_cache[content_type])
        try:
            df = pandas.DataFrame(self.results_cache[content_type])
            self.results_cache[content_type].clear()
            if os.path.exists(self._path_for(content_type=content_type)):
                existing_df = pandas.read_csv(
                    self._path_for(content_type=content_type),
                    sep=self.separator or ';')
                df = pandas.concat([existing_df, df])
            logging.info("Writing {} logs of type {} to {}".format(amount, content_type, self._path_for(content_type)))
            df.to_csv(self._path_for(content_type=content_type), index=False, sep=self.separator or ';', mode='w',
                      header=not os.path.exists(self.paths[content_type]))
        except Exception as e:
            self.unsuccessfully_sent += amount
            raise e
        else:
            self.successfully_sent += amount
