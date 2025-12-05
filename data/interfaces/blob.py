from azure.storage.filedatalake import DataLakeServiceClient
import os
from utils import is_prod
import logging

_storage_account = "fooddiarystorage"
_storage_key = os.environ.get("FOODDIARY_STORAGE_ACCOUNT_KEY", default=None)
_service_client = None
_file_system_client = None
_file_clients = {}

def get_container_name() -> str:
    return "prod" if is_prod() else "test"

def get_storage_options():
    global _storage_account, _storage_key
    if _storage_key is None:
        raise ValueError("Storage account key is not set in environment variables.")
    return {
        "account_name": _storage_account,
        "account_key": _storage_key
    }

def get_adlfs_path() -> str:
    return f"az://{get_container_name()}/"

def get_service_client():
    global _storage_account, _storage_key, _service_client
    if _storage_key is None:
        raise ValueError("Storage account key is not set in environment variables.")
    if _service_client is None:
        account_url = f"https://{_storage_account}.dfs.core.windows.net"
        _service_client = DataLakeServiceClient(account_url=account_url, credential=_storage_key)
    return _service_client

def get_file_system_client():
    global _file_system_client
    if _file_system_client is None:
        container_name = get_container_name()
        service_client = get_service_client()
        _file_system_client = service_client.get_file_system_client(file_system=container_name)
        logging.info(f"File system client created for container: {container_name}")
    return _file_system_client

def get_file_client(file_path):
    if file_path not in _file_clients:
        file_system_client = get_file_system_client()
        _file_clients[file_path] = file_system_client.get_file_client(file_path)
    return _file_clients[file_path]

def check_exists(file_path) -> bool:
    logging.info(f"Checking existence of file: {file_path}")
    file_client = get_file_client(file_path)
    result = file_client.exists()
    logging.info(f"File exists: {result}")
    return result

def create_path_to(file_path):
    logging.info(f"Creating path to file: {file_path}")
    file_system_client = get_file_system_client()
    directories = os.path.dirname(file_path)
    if directories:
        dir_client = file_system_client.create_directory(directories)