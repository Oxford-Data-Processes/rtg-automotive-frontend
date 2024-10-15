import streamlit as st
import boto3
import json
import os
import pandas as pd


def app_log_viewer(aws_account_id):
    st.title("Log Viewer")
    bucket_name = f"rtg-automotive-bucket-{aws_account_id}"
    s3_client = boto3.client("s3")
    log_prefix = "logs/"

    # List objects in the specified S3 bucket with the log prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=log_prefix)
    logs = response.get("Contents", [])

    if not logs:
        st.write("No logs found.")
        return

    log_data = []

    for log in logs:
        log_key = log["Key"]
        # Fetch and display log content
        log_object = s3_client.get_object(Bucket=bucket_name, Key=log_key)
        log_content = json.loads(log_object["Body"].read().decode("utf-8"))[0]

        log_data.append(log_content)

    # Convert log_data to a DataFrame and display it, sorting by timestamp to show most recent logs first
    log_df = pd.DataFrame(log_data)
    log_df = log_df.sort_values(
        by="timestamp", ascending=False
    )  # Assuming 'timestamp' is a key in log_content
    st.dataframe(log_df)
