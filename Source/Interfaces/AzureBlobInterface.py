from . import FileInterface
from azure.storage.blob import BlobServiceClient
import os


class AzureBlobInterface(FileInterface.FileInterface):

    def __init__(self, blob_connection_string=None, **kwargs):
        """
        Interface to send logs to CSV file(s). Not every audit log has every possible column, so columns in the CSV
        file might change over time. Therefore, the CSV file is recreated each time the cache_size is hit to insure
        integrity, taking the performance hit.
        """
        super().__init__(**kwargs)
        self.connection_string = blob_connection_string
        self._blob_service = None
        self._container_client = None

    @property
    def enabled(self):

        return self.collector.config['output', 'azureBlob', 'enabled']

    @property
    def separate_by_content(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: bool
        """
        return self.collector.config['output', 'azureBlob', 'separateByContentType']

    @property
    def separator(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: str
        """
        return self.collector.config['output', 'azureBlob', 'separator']

    @property
    def path(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: str
        """
        return self.collector.config['output', 'azureBlob', 'tempPath']

    @property
    def cache_size(self):
        """
        Needed because AzureBlobInterface inherits from this interface and has its own config.
        :return: int
        """
        return self.collector.config['output', 'azureBlob', 'cacheSize']

    @property
    def blob_service(self):

        if not self._blob_service:
            self._blob_service = BlobServiceClient.from_connection_string(conn_str=self.connection_string)
        return self._blob_service

    @property
    def container_client(self):

        if not self._container_client:
            container_name = self.collector.config['output', 'azureBlob', 'containerName'] or 'audit-logs'
            if container_name not in [container['name'] for container in self.blob_service.list_containers()]:
                self._container_client = self._blob_service.create_container(name=container_name)
            else:
                self._container_client = self._blob_service.get_container_client(container=container_name)
        return self._container_client

    def write_blob(self, blob_name, file_path):

        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(file_path, 'rb') as ofile:
            blob_client.upload_blob(ofile, overwrite=True)

    def exit_callback(self):

        super().exit_callback()
        if not self.separate_by_content:
            self.write_blob(blob_name=self.collector.config['output', 'azureBlob', 'blobName'] or 'AuditLog',
                            file_path=self.path)
        for content_type in self.paths.keys():
            temp_file_path = self.paths[content_type]
            blob_name = os.path.split(self._path_for(content_type=content_type))[-1]
            self.write_blob(blob_name=blob_name, file_path=temp_file_path)
