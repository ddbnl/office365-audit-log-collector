from . import _Interface
import collections
import logging
import threading
import socket
import json
import time
from fluent import sender


class FluentdInterface(_Interface.Interface):

    def __init__(self, fl_address=None, fl_port=None, cache_size=500000, tenant_name=None, **kwargs):

        super().__init__(**kwargs)
        self.fl_address = fl_address
        self.fl_port = fl_port
        self.cache_size = cache_size
        self.results_cache = collections.defaultdict(collections.deque)
        self.tenant_name = tenant_name

    @property
    def total_cache_length(self):

        return sum([len(self.results_cache[k]) for k in self.results_cache.keys()])
    
    def _send_message(self, msg, content_type, retries=3, **kwargs):
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
            logger = sender.FluentSender('o365', host=self.fl_address, port=int(self.fl_port))
            for msg in self.results_cache[content_type]:
                msg['tenant'] = self.tenant_name
                logger.emit(content_type,msg)
        except Exception as e:
            self.unsuccessfully_sent += amount
            raise e
        else:
            self.successfully_sent += amount
