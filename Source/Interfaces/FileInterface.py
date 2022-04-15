import logging
import os
from . import _Interface
import collections
import pandas
import threading
import time


class FileInterface(_Interface.Interface):

    def __init__(self, path='output', separate_by_content_type=True, separator=';', cache_size=500000, **kwargs):
        """
        Interface to send logs to CSV file(s). Not every audit log has every possible column, so columns in the CSV
        file might change over time. Therefore, the CSV file is recreated each time the cache_size is hit to insure
        integrity, taking the performance hit.
        """
        super().__init__(**kwargs)
        self.path = path
        self.paths = {}
        self.separate_by_content_type = separate_by_content_type
        self.separator = separator
        self.cache_size = cache_size
        self.results_cache = collections.defaultdict(collections.deque)

    @property
    def total_cache_length(self):

        return sum([len(self.results_cache[k]) for k in self.results_cache.keys()])

    def _path_for(self, content_type):

        if content_type not in self.paths:
            self.paths[content_type] = "{}_{}.csv".format(self.path, content_type.replace('.', '')) \
                if self.separate_by_content_type else self.path
        return self.paths[content_type]

    def _send_message(self, msg, content_type, **kwargs):

        self.results_cache[content_type].append(msg)
        if self.total_cache_length >= self.cache_size:
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
                existing_df = pandas.read_csv(self._path_for(content_type=content_type), sep=self.separator)
                df = pandas.concat([existing_df, df])
            logging.info("Writing {} logs of type {} to {}".format(amount, content_type, self._path_for(content_type)))
            df.to_csv(self._path_for(content_type=content_type), index=False, sep=self.separator, mode='w',
                      header=not os.path.exists(self.paths[content_type]))
        except Exception as e:
            self.unsuccessfully_sent += amount
            raise e
        else:
            self.successfully_sent += amount