import azure.functions as func
import logging
from data.interfaces.blob import get_file_system_client

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name(name="debug_endpoint")
@app.route(route="debug")
def main(req: func.HttpRequest) -> str:
    msg = "The debug function endpoint executed successfully"
    return msg

@app.function_name(name="debug_write_endpoint")
@app.route(route="debug_write")
def main(req: func.HttpRequest) -> str:
    file_name = "testfile.txt"
    if "msg" not in req.params:
        return "Please pass a 'msg' parameter in the query string"
    msg = req.params.get("msg")
    file_client = get_file_system_client().get_file_client(file_name)
    file_client.upload_data(data=msg, overwrite=True)
    return f"Successfully wrote:\n{msg}\nto {file_name}"
