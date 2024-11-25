import io
import time
import uuid
import zipfile
from datetime import datetime
from typing import List, Tuple

import api.utils as api_utils
import pandas as pd
import streamlit as st
from aws_utils import events, iam, s3, sqs
from utils import PROJECT_BUCKET_NAME


def get_last_csv_from_s3(bucket_name, prefix, s3_handler):
    response = s3_handler.list_objects(bucket_name, prefix)
    csv_files = [obj for obj in response if obj["Key"].endswith(".csv")]
    csv_files.sort(key=lambda x: x["LastModified"], reverse=True)
    return csv_files[0]["Key"] if csv_files else None


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


def upload_file_to_s3(file, bucket_name, date, s3_handler):
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


def handle_file_uploads(uploaded_files, bucket_name, date, s3_handler, sqs_queue_url):
    if uploaded_files:
        sqs_handler = sqs.SQSHandler()
        sqs_handler.delete_all_sqs_messages(sqs_queue_url)
        for uploaded_file in uploaded_files:
            upload_file_to_s3(uploaded_file, bucket_name, date, s3_handler)
        st.success("Files uploaded successfully")
        time.sleep(len(uploaded_files) * 5)
        messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)[
            -len(uploaded_files) :
        ]
        st.write("--------------------------------------------------")
        for message in messages:
            st.write(message["Body"])
    else:
        st.warning("Please upload at least one file first.")


def handle_ebay_queue(sqs_queue_url):
    sqs_handler = sqs.SQSHandler()
    sqs_handler.delete_all_sqs_messages(sqs_queue_url)

    events_handler = events.EventsHandler()

    events_handler.publish_event(
        "rtg-automotive-generate-ebay-table-lambda-event-bus",
        "com.oxforddataprocesses",
        "RtgAutomotiveGenerateEbayTable",
        {
            "event_type": "RtgAutomotiveGenerateEbayTable",
            "user": "admin",
            "trigger_id": str(uuid.uuid4()),
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        },
    )

    time.sleep(30)
    messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)
    for message in messages:
        st.write(message["Body"])


def generate_ebay_upload_files():
    sqs_queue_url = "rtg-automotive-lambda-queue"
    handle_ebay_queue(sqs_queue_url)

    s3_handler = s3.S3Handler()
    parquet_data = s3_handler.load_parquet_from_s3(
        PROJECT_BUCKET_NAME, "ebay/data.parquet"
    )

    df = pd.read_parquet(io.BytesIO(parquet_data))

    ebay_df = create_ebay_dataframe(df)
    stores = list(ebay_df["Store"].unique())
    ebay_dfs = [
        (ebay_df[ebay_df["Store"] == store].drop(columns=["Store"]), store)
        for store in stores
    ]

    zip_buffer = zip_dataframes(ebay_dfs)

    st.download_button(
        label="Download eBay Upload Files",
        data=zip_buffer.getvalue(),
        file_name="ebay_upload_files.zip",
        mime="application/zip",
    )


def main():
    st.title("Stock Manager")
    iam.get_aws_credentials(st.secrets["aws_credentials"])

    s3_handler = s3.S3Handler()

    sqs_queue_url = "rtg-automotive-lambda-queue"

    uploaded_files = st.file_uploader(
        "Upload Excel files", type=["xlsx"], accept_multiple_files=True
    )
    date = st.date_input("Select a date", value=pd.Timestamp.now().date()).strftime(
        "%Y-%m-%d"
    )

    if st.button("Upload Files") and date:
        handle_file_uploads(
            uploaded_files, PROJECT_BUCKET_NAME, date, s3_handler, sqs_queue_url
        )

    if st.button("Generate eBay Store Upload Files"):
        generate_ebay_upload_files()
