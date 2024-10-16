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


def get_last_csv_from_s3(bucket_name, prefix, s3_client):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    csv_files = [
        obj for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")
    ]
    csv_files.sort(key=lambda x: x["LastModified"], reverse=True)
    return csv_files[0]["Key"] if csv_files else None


# def load_csv_from_s3(bucket_name, csv_key, s3_client):
#     csv_object = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
#     csv_data = csv_object["Body"].read()
#     df = pd.read_csv(BytesIO(csv_data))
#     return df


def run_athena_query(query):
    # Initialize a session using Boto3
    session = boto3.Session()
    athena_client = session.client("athena", region_name="eu-west-2")

    # Define the parameters for the query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "rtg_automotive"},
        WorkGroup="rtg-automotive-workgroup",
    )

    query_execution_id = response["QueryExecutionId"]
    # Wait for the query to complete
    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = query_status["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    if status == "SUCCEEDED":
        return athena_client.get_query_results(QueryExecutionId=query_execution_id)
    else:
        st.error(f"Query failed with status: {status}")
        return []


def trigger_lambda_function(function_name: str, aws_account_id: str) -> bool:
    lambda_client = boto3.client(
        "lambda",
        region_name="eu-west-2",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"],
    )
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
        )
        time.sleep(2)
        return True
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return False


def get_s3_client():
    return boto3.client(
        "s3",
        region_name="eu-west-2",
        aws_access_key_id=st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=st.secrets["aws_credentials"].get("AWS_SESSION_TOKEN"),
    )
