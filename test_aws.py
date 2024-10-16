from aws_utils import iam
import streamlit as st
import os

aws_access_key_id = st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"]

aws_credentials = iam.AWSCredentials(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    stage="dev",
)

aws_credentials.get_aws_credentials()

print(os.environ["AWS_ACCESS_KEY_ID"])
print(os.environ["AWS_SECRET_ACCESS_KEY"])
print(os.environ["AWS_SESSION_TOKEN"])
