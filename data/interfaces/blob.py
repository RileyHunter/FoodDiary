from azure.storage.filedatalake import DataLakeServiceClient
import os
from utils import is_prod
import logging

_storage_account = "fooddiarystorage"
_storage_key = os.environ["FOODDIARY_STORAGE_ACCOUNT_KEY"]
_service_client = None
_file_system_client = None

def get_service_client():
    global _storage_account, _storage_key, _service_client
    if _service_client is None:
        account_url = f"https://{_storage_account}.dfs.core.windows.net"
        _service_client = DataLakeServiceClient(account_url=account_url, credential=_storage_key)
    return _service_client

def get_file_system_client():
    global _file_system_client
    if _file_system_client is None:
        container_name = "prod" if is_prod() else "test"
        service_client = get_service_client()
        _file_system_client = service_client.get_file_system_client(file_system=container_name)
        logging.info(f"File system client created for container: {container_name}")
    return _file_system_client