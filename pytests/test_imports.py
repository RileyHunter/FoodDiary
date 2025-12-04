from data.interfaces.blob import get_file_system_client

def test_get_file_system_client():
    assert get_file_system_client() is not None
    