import requests

# BASE_URL = ""

# BASE_URL = ""

BASE_URL = "http://localhost:8000/"


def get_request(endpoint, params=None):
    response = requests.get(f"{BASE_URL}{endpoint}", params=params)
    print("RESPONSE")
    print(response)
    return response.json()