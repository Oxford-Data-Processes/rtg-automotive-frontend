import os

import streamlit as st
from aws_utils import iam

iam.get_aws_credentials(st.secrets["aws_credentials"])

PROJECT_BUCKET_NAME = f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}"
