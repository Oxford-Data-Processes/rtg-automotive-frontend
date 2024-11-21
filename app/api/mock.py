from fastapi import FastAPI
import json
from fastapi.responses import JSONResponse
import os
from typing import Optional, List, Literal

app = FastAPI()


@app.get("/items/")
async def read_items(
    table_name: str,
    filters: Optional[List[str]] = None,
    limit: int = 5,
):
    current_directory = os.getcwd()
    data_directory = os.path.join(current_directory, "app/api/data")
    file_path = os.path.join(data_directory, f"{table_name}.json")
    with open(file_path) as f:
        data = json.load(f)
    filtered_data = data[:limit]
    return JSONResponse(content=filtered_data)


@app.post("/items/")
async def edit_items(
    table_name: str,
    type: Literal["update", "delete", "append"],
    limit: int = 5,
):
    current_directory = os.getcwd()
    data_directory = os.path.join(current_directory, "app/api/data")
    file_path = os.path.join(data_directory, f"{table_name}.json")
    with open(file_path) as f:
        data = json.load(f)
    filtered_data = data[:limit]
    return JSONResponse(content=filtered_data)
