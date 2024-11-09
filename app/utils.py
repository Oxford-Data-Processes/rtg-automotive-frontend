from aws_utils import iam
import streamlit as st
import os

iam.get_aws_credentials(st.secrets["aws_credentials"])

PROJECT_BUCKET_NAME = f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}"
