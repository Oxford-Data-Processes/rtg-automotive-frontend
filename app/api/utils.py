import requests
import urllib.parse

# BASE_URL = ""

# BASE_URL = ""


API_ID = "tsybspea31"
STAGE = "dev"
REGION = "eu-west-2"
BASE_URL = f"https://{API_ID}.execute-api.{REGION}.amazonaws.com/{STAGE}/"


# curl -X GET "https://tsybspea31.execute-api.eu-west-2.amazonaws.com/dev/items/?table_name=ebay&limit=5"

# BASE_URL = "http://localhost:8000/"


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
    response = requests.post(f"{BASE_URL}{endpoint}", params=params)
    return response.json()
