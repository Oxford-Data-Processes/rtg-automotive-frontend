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
