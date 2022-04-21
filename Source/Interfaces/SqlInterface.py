from . import _Interface
from sqlalchemy import create_engine, inspect
import pyodbc
import time
import urllib
import logging
import threading
import collections
import pandas


class SqlInterface(_Interface.Interface):

    def __init__(self, sql_connection_string, **kwargs):
        """
        Interface to send logs to an SQL database. Caches logs in memory until the cache size is hit, then writes them
        to database. When the cache is too small too many SQL writes are made taking ages to complete. Too
        large and the collector will eat too much memory.
        """
        super().__init__(**kwargs)
        self._connection_string = sql_connection_string
        self.results_cache = collections.defaultdict(collections.deque)
        self._existing_columns = {}
        self._threads = collections.deque()
        self._engine = None

    @property
    def enabled(self):

        return self.collector.config['output', 'sql', 'enabled']

    @property
    def engine(self):
        """
        DB Engine for use in main thread. A separate one is creation for each sub thread.
        :return: sqlalchemy.Engine
        """
        if not self._engine:
            self._engine = create_engine(self.connection_string)
        return self._engine

    @staticmethod
    def _table_name_for(content_type):
        """
        Create a table name for a content type (remove periods).
        :param content_type: str
        :return: str
        """
        return content_type.replace('.', '')

    def _existing_columns_for(self, content_type, engine):
        """
        Cache columns currently existing for a table. Used to check if new incoming logs have columns that currently
        don't exist in the database.
        :param content_type: str
        :return: list of str
        """
        if content_type not in self._existing_columns.keys():
            self._existing_columns[content_type] = \
                pandas.read_sql_query(f"SELECT TOP (1) * FROM {self._table_name_for(content_type)};",
                                      con=engine).columns.tolist()
        return self._existing_columns[content_type]

    @property
    def total_cache_length(self):

        return sum([len(self.results_cache[k]) for k in self.results_cache.keys()])

    @property
    def connection_string(self):

        params = urllib.parse.quote_plus(self._connection_string)
        return 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)

    @staticmethod
    def _validate_column_names(df):
        """
        Audit logs tend to have periods (.) in their column names. Take those out. If a log column has the same name
        as an existing column in the database, but the capitalization doesn't match, rename the column to the existing
        one. Otherwise SQL will throw an error for duplicate column names.
        :param df: pandas.DataFrame.
        :return: pandas.DataFrame.
        """
        to_rename = {}
        for column in df:
            if '.' in column:
                to_rename[column] = column.replace('.', '')
        return df.rename(columns=to_rename)

    @staticmethod
    def _validate_column_value(df):
        """
        Flatten columns that a list as value. E.g. column "ColumnA: [1,2,3]" becomes:
        "ColumnA_0: 1, ColumnA_1: 2, ColumnA_2: 3".
        :param df: pandas.DataFrame.
        :return: pandas.DataFrame
        """
        for column in df.columns.tolist():
            for i, value in enumerate(df[column]):
                if type(df[column][i]) in [list, dict]:
                    df[column][i] = str(df[column][i])
        return df

    def _validate_existing_columns(self, df, content_type, engine):
        """
        Not all audit logs have all available columns. There columns in the database might change as logs come in.
        Check whether all columns in a log already exist in the current table.
        :return: Bool
        """
        if inspect(engine).has_table(self._table_name_for(content_type=content_type)):
            new_cols = df.columns.tolist()
            missing_cols = set(new_cols) - set(self._existing_columns_for(content_type, engine=engine))
            return not missing_cols
        return True

    @staticmethod
    def _deduplicate_columns(df):
        """
        Different logs sometimes have identical columns names but with different capitalization (for some reason);
        merge these columns.
        :param df:
        :return:
        """
        to_check = df.columns.tolist()
        leading_columns = []
        to_merge = collections.defaultdict(collections.deque)
        for column in to_check:
            for leading_column in leading_columns:
                if column.lower() == leading_column.lower() and column != leading_column:
                    to_merge[leading_column].append(column)
                    break
            else:
                leading_columns.append(column)
        for leading_column, columns_to_merge in to_merge.items():
            new_column = df[leading_column]
            for column_to_merge in columns_to_merge:
                new_column = new_column.combine_first(df[column_to_merge])
                del df[column_to_merge]
            del df[leading_column]
            df[leading_column] = new_column
        return df

    def _remake_table(self, new_data, content_type, engine):
        """
        If a new log is coming in that has columns that don't exist in the current table, replace it instead of
        appending.
        :param new_data: pandas.DataFrame
        :param content_type: str
        """
        table_name = self._table_name_for(content_type=content_type)
        existing_table = pandas.read_sql_table(con=engine, table_name=table_name)
        df = pandas.concat([new_data, existing_table])
        self._existing_columns[content_type] = df.columns.tolist()
        logging.info("Recommitting {} records of type {} to table {}".format(
            len(df), content_type, table_name))
        df = df.loc[:, ~df.columns.duplicated()]  # Remove any duplicate columns
        df = self._deduplicate_columns(df=df)
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace',
                  chunksize=int((self.collector.config['output', 'sql', 'chunkSize'] or 2000) / len(df.columns)),
                  method='multi')

    def _send_message(self, msg, content_type, **kwargs):
        """
        Write logs to cache. Process cache if cache size is hit.
        :param msg: JSON
        :param content_type: str
        """
        self.results_cache[content_type].append(msg)
        if self.total_cache_length >= (self.collector.config['output', 'sql', 'cacheSize'] or 500000):
            self._wait_threads()
            self._threads.clear()
            self._process_caches()

    def _process_caches(self):
        """
        Write all cached logs to database.
        """
        for content_type in self.results_cache.copy().keys():
            if not self.results_cache[content_type]:
                continue
            thread = threading.Thread(target=self._process_cache, kwargs={'content_type': content_type}, daemon=True)
            thread.start()
            self._threads.append(thread)

    def _wait_threads(self, timeout=600):

        while True in [thread.is_alive() for thread in self._threads]:
            if not timeout:
                raise RuntimeError("Timeout while committing to database")
            timeout -= 1
            time.sleep(1)

    def _process_cache(self, content_type):
        """
        Write cached logs to database for a content type.
        :param content_type: str
        """
        df = pandas.DataFrame(self.results_cache[content_type])
        df = self._validate_column_names(df=df)
        df = self._validate_column_value(df=df)

        table_name = self._table_name_for(content_type=content_type)
        engine = create_engine(self.connection_string)
        with engine.connect():
            try:
                if not self._validate_existing_columns(df=df, content_type=content_type, engine=engine):
                    self._remake_table(new_data=df, content_type=content_type, engine=engine)
                else:
                    logging.info("Committing {} records of type {} to table {}".format(
                        len(df), content_type, table_name))
                    df = df.loc[:, ~df.columns.duplicated()]  # Remove any duplicate columns
                    df = self._deduplicate_columns(df=df)
                    df.to_sql(
                        name=table_name, con=engine, index=False, if_exists='append',
                        chunksize=int((self.collector.config['output', 'sql', 'chunkSize'] or 2000) / len(df.columns)),
                        method='multi')
            except Exception as e:
                self.unsuccessfully_sent += len(df)
                raise e
            else:
                self.successfully_sent += len(df)

    def exit_callback(self):

        super().exit_callback()
        self._process_caches()
        self._wait_threads()
