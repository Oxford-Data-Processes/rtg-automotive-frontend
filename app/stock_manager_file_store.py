import os
from typing import Any
import io
import zipfile

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

    # Sort objects by timestamp (the second last part of the key)
    objects.sort(key=lambda obj: obj["Key"].split("/")[-2], reverse=True)

    # Limit to the latest 20 items
    for object in objects[:20]:
        file_name = object["Key"].split("/")[-1]
        timestamp = object["Key"].split("/")[-2]
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            s3_client = s3_handler.s3_client
            response = s3_client.get_object(Bucket=bucket_name, Key=object["Key"])
            zip_file.writestr(file_name, response["Body"].read())

        zip_buffer.seek(0)
        st.download_button(
            label=f"Download: {timestamp}",
            data=zip_buffer,
            file_name=file_name,
            mime="application/zip",
        )
