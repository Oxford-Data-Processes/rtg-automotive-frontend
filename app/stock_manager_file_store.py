import os
from typing import Any

import pandas as pd
import streamlit as st
from aws_utils import iam, s3


iam.get_aws_credentials(st.secrets["aws_credentials"])


def main() -> None:
    project: str = "rtg-automotive"
    bucket_name: str = f"{project}-bucket-{os.environ['AWS_ACCOUNT_ID']}"
    st.title("Stock Manager File Store")

    s3_handler = s3.S3Handler()
    objects = s3_handler.list_objects(bucket_name, "ebay/zip_folders/")
    st.write(objects)
