from aws_utils import s3, iam
import streamlit as st
import os
import pandas as pd

aws_access_key_id = st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"]
aws_account_id = st.secrets["aws_credentials"]["AWS_ACCOUNT_ID"]

aws_credentials = iam.AWSCredentials(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    stage="dev",
)

aws_credentials.get_aws_credentials()


last_csv_key = "athena-results/0b091758-c380-42f4-a0ba-a99b25652114.csv"
project_bucket_name = "rtg-automotive-bucket-654654324108"
s3_client = s3.S3Client()
data = s3_client.load_csv_from_s3(project_bucket_name, last_csv_key)
df = pd.DataFrame(data[1:], columns=data[0])
print(df.head())
