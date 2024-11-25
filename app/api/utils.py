import requests
import urllib.parse

# BASE_URL = ""

# BASE_URL = ""


API_ID = "tsybspea31"
STAGE = "dev"
REGION = "eu-west-2"
BASE_URL = f"https://{API_ID}.execute-api.{REGION}.amazonaws.com/{STAGE}/"


BASE_URL = "http://localhost:8000/"


def get_request(endpoint, params=None):
    request_url = f"{BASE_URL}{endpoint}/"
    print("\n")
    print("REQUEST URL:")
    print(request_url)
    print("PARAMS:")
    print(params)
    response = requests.get(request_url, params=params)
    return response.json()


def post_request(endpoint, params=None):
    response = requests.post(
        f"{BASE_URL}{endpoint}/?table_name={params['table_name']}&type={params['type']}",
        headers={"Content-Type": "application/json"},
        json=params["payload"],
    )
    return response.json()
