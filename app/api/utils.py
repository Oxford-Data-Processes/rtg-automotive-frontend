import requests
from aws_utils import api_gateway, iam
import streamlit as st
import os
from typing import List, Dict, Any

iam.get_aws_credentials(st.secrets["aws_credentials"])

STAGE = st.secrets["aws_credentials"]["STAGE"]

api_gateway_handler = api_gateway.APIGatewayHandler()
api_id = api_gateway_handler.search_api_by_name("rtg-automotive-api")

BASE_URL = f"https://{api_id}.execute-api.{os.environ['AWS_REGION']}.amazonaws.com/{STAGE.lower()}/"

# BASE_URL = "http://localhost:8000/"


def get_request(endpoint, params=None) -> List[Dict[str, Any]]:
    print(f"GET REQUEST - Params: {params}")
    request_url = f"{BASE_URL}{endpoint}/"
    response = requests.get(request_url, params=params)
    return response.json()


def post_request(endpoint, params=None):
    print(f"POST REQUEST - Params: {params}")
    request_url = (
        f"{BASE_URL}{endpoint}/?table_name={params['table_name']}&type={params['type']}"
    )
    response = requests.post(
        request_url,
        headers={"Content-Type": "application/json"},
        json=params["payload"],
    )
    return response.json()
