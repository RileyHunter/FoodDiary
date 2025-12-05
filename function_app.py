import azure.functions as func
import logging
from data.interfaces.blob import get_adlfs_path, get_file_client
from data.entities.diary_entry import DiaryEntries
from datetime import datetime, timezone

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name(name="debug_endpoint")
@app.route(route="debug")
def main(req: func.HttpRequest) -> str:
    msg = "The debug function endpoint executed successfully; "
    diary = DiaryEntries()
    instance_id = diary.create({
        "UserId": "user-123",
        "FoodId": "food-456",
        "ConsumedAt": datetime.now(timezone.utc),
        "Notes": "Breakfast"
    })

    return msg + '\n' + str(diary.load_all().collect())

@app.function_name(name="debug_write_endpoint")
@app.route(route="debug_write")
def main(req: func.HttpRequest) -> str:
    file_name = "testfile.txt"
    if "msg" not in req.params:
        return "Please pass a 'msg' parameter in the query string"
    msg = req.params.get("msg")
    file_client = get_file_client(file_name)
    file_client.upload_data(data=msg, overwrite=True)
    return f"Successfully wrote:\n{msg}\nto {file_name}"
