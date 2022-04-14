import logging
import os
from . import _Interface
import collections
import pandas


class FileInterface(_Interface.Interface):

    def __init__(self, path='output', separate_by_content_type=True, separator=';', **kwargs):
        """
        Interface to send logs to an Azure Log Analytics Workspace.
        """
        super().__init__(**kwargs)
        self.path = path
        self.paths = {}
        self.separate_by_content_type = separate_by_content_type
        self.separator = separator

    def _send_message(self, msg, content_type, **kwargs):

        if content_type not in self.paths:
            self.paths[content_type] = "{}_{}.csv".format(self.path, content_type.replace('.', '')) \
                if self.separate_by_content_type else self.path
        df = pandas.json_normalize(msg)
        df.to_csv(self.paths[content_type], index=False, sep=self.separator, mode='a',
                  header=not os.path.exists(self.paths[content_type]))


