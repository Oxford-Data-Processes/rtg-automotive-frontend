import boto3
import os
import time
import re
import pandas as pd
from io import BytesIO
import streamlit as st
from datetime import datetime
import json
import pytz


def log_action(bucket_name, action, user):
    s3_client = boto3.client("s3")

    timestamp = datetime.now().isoformat()
    log_entry = {"timestamp": timestamp, "action": action, "user": user}

    london_tz = pytz.timezone("Europe/London")
    log_file_name = f"logs/{datetime.now(london_tz).strftime('%Y-%m-%dT%H:%M:%S')}.json"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=log_file_name,
        Body=json.dumps([log_entry]) + "\n",
        ContentType="application/json",
    )


# def trigger_lambda_function(function_name: str, aws_account_id: str) -> bool:
#     lambda_client = boto3.client(
#         "lambda",
#         region_name="eu-west-2",
#         aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
#         aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
#         aws_session_token=os.environ["AWS_SESSION_TOKEN"],
#     )
#     try:
#         response = lambda_client.invoke(
#             FunctionName=function_name,
#             InvocationType="RequestResponse",
#         )
#         time.sleep(2)
#         return True
#     except Exception as e:
#         st.error(f"Error: {str(e)}")
#         return False


def get_s3_client():
    return boto3.client(
        "s3",
        region_name="eu-west-2",
        aws_access_key_id=st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=st.secrets["aws_credentials"].get("AWS_SESSION_TOKEN"),
    )
