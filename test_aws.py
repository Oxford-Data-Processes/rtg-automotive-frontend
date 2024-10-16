from aws_utils import sqs, iam
import streamlit as st
import os


aws_access_key_id = st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"]
aws_account_id = st.secrets["aws_credentials"]["AWS_ACCOUNT_ID"]

aws_credentials = iam.AWSCredentials(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    stage="dev",
)

aws_credentials.get_aws_credentials()

sqs_handler = sqs.SQSHandler()

sqs_queue_url = (
    f"https://sqs.eu-west-2.amazonaws.com/{aws_account_id}/rtg-automotive-sqs-queue"
)

messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)

print(messages)
