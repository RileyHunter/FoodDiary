import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name(name="debug_endpoint")
@app.route(route="debug")
def main(req: func.HttpRequest) -> str:
    msg = "The debug function endpoint executed successfully"
    return msg