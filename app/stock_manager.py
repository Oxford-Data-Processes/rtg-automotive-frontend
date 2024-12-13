import io
import time
import zipfile
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import streamlit as st
from aws_utils import iam, s3, sqs
from utils import PROJECT_BUCKET_NAME


def zip_dataframes(dataframes: List[Tuple[pd.DataFrame, str]]) -> io.BytesIO:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for df, name in dataframes:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            zip_file.writestr(f"{name}.csv", csv_buffer.getvalue())
    return zip_buffer


def create_ebay_dataframe(ebay_df: pd.DataFrame) -> pd.DataFrame:
    ebay_df = ebay_df[ebay_df["quantity_delta"] != 0]
    ebay_df = ebay_df.dropna(subset=["item_id"])

    ebay_df = ebay_df.rename(
        columns={
            "custom_label": "CustomLabel",
            "item_id": "ItemID",
            "ebay_store": "Store",
            "quantity": "Quantity",
        }
    )
    ebay_df["Action"] = "Revise"
    ebay_df["SiteID"] = "UK"
    ebay_df["Currency"] = "GBP"
    ebay_df = ebay_df[
        [
            "Action",
            "ItemID",
            "SiteID",
            "Currency",
            "Quantity",
            "Store",
        ]
    ]
    ebay_df["Quantity"] = ebay_df["Quantity"].astype(int)
    ebay_df["ItemID"] = ebay_df["ItemID"].apply(lambda x: int(x) if x != "" else None)
    return ebay_df


def upload_file_to_s3(
    file: io.BytesIO, bucket_name: str, date: str, s3_handler: s3.S3Handler
) -> None:
    year = date.split("-")[0]
    month = date.split("-")[1]
    day = date.split("-")[2]
    file_path = (
        f"stock_feed/year={year}/month={month}/day={day}/{file.name.replace(' ','_')}"
    )
    try:
        s3_handler.upload_excel_to_s3(bucket_name, file_path, file.getvalue())
    except Exception as e:
        st.error(f"Error uploading file: {str(e)}")


def handle_file_uploads(
    uploaded_files: List[io.BytesIO],
    bucket_name: str,
    date: str,
    s3_handler: s3.S3Handler,
    sqs_queue_url: str,
) -> None:
    if uploaded_files:
        sqs_handler = sqs.SQSHandler()
        sqs_handler.delete_all_sqs_messages(sqs_queue_url)
        for uploaded_file in uploaded_files:
            upload_file_to_s3(uploaded_file, bucket_name, date, s3_handler)
        st.success("Files uploaded successfully")
        with st.spinner("Waiting for files to be processed, about 1 minute per file."):
            start_time = time.time()
            st.write(f"Processing {len(uploaded_files)} files...")
            st.write(
                f"Start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            time.sleep(len(uploaded_files) * 60)
            messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)[
                -len(uploaded_files) :
            ]
            st.write("--------------------------------------------------")
            for message in messages:
                st.write(message["Body"])
    else:
        st.warning("Please upload at least one file first.")


def main() -> None:
    st.title("Stock Manager")
    iam.get_aws_credentials(st.secrets["aws_credentials"])

    s3_handler = s3.S3Handler()

    sqs_queue_url = "rtg-automotive-lambda-queue"

    uploaded_files = st.file_uploader(
        "Upload Excel files", type=["xlsx"], accept_multiple_files=True
    )
    date = st.date_input("Select a date", value=pd.Timestamp.now().date())
    date = str(date.strftime("%Y-%m-%d"))
    if st.button("Upload Files") and date is not None:
        if uploaded_files:
            handle_file_uploads(
                uploaded_files,
                PROJECT_BUCKET_NAME,
                date,
                s3_handler,
                sqs_queue_url,
            )
        else:
            st.warning("Please upload at least one file first.")
