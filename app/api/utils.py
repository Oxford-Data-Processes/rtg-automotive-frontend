import requests

# BASE_URL = ""

# BASE_URL = ""

BASE_URL = "http://localhost:8000/"


def get_request(endpoint, params=None):
    response = requests.get(f"{BASE_URL}{endpoint}", params=params)
    return response.json()


def post_request(endpoint, params=None):
    response = requests.post(f"{BASE_URL}{endpoint}", params=params)
    return response.json()
