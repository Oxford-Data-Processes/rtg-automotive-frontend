from fastapi import FastAPI
import json
from fastapi.responses import JSONResponse
import os
from typing import Optional

app = FastAPI()


@app.get("/items/")
async def read_items(
    table_name: str,
    pickup_datetime: Optional[str] = None,
    dropoff_datetime: Optional[str] = None,
    limit: int = 5,
):
    current_directory = os.getcwd()
    data_directory = os.path.join(current_directory, "app/api/data")
    file_path = os.path.join(data_directory, f"{table_name}_processed_limit_5.json")
    with open(file_path) as f:
        data = json.load(f)
    filtered_data = data[:limit]
    return JSONResponse(content=filtered_data)
