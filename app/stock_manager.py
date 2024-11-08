import streamlit as st
import os
import time
import pandas as pd
from typing import List, Tuple
import io
import zipfile
from aws_utils import sqs, s3, events, iam
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
        for uploaded_file in uploaded_files:
            upload_file_to_s3(uploaded_file, bucket_name, date, s3_handler)
        st.success("Files uploaded successfully")
        time.sleep(len(uploaded_files) * 4)
        sqs_handler = sqs.SQSHandler()
        messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)[
            -len(uploaded_files) :
        ]
        st.write("--------------------------------------------------")
        st.write(messages)
    else:
        st.warning("Please upload at least one file first.")


def generate_ebay_upload_files(stage, project_bucket_name, s3_handler):
    iam.get_aws_credentials(st.secrets["aws_credentials"])

    events_handler = events.EventsHandler()
    event_detail = {
        "bucket_name": project_bucket_name,
    }
    events_handler.publish_event(
        "rtg-automotive-process-stock-feed-lambda-event-bus",
        "com.oxforddataprocesses",
        "StockFeedProcessed",
        event_detail,
    )

    time.sleep(5)

    last_csv_key = get_last_csv_from_s3(
        project_bucket_name, "athena-results/", s3_handler
    )
    if last_csv_key:

        data = s3_handler.load_csv_from_s3(project_bucket_name, last_csv_key)
        df = pd.DataFrame(data[1:], columns=data[0])
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


def app_stock_manager(stage):

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

    # if st.button("Generate eBay Store Upload Files"):
    #     generate_ebay_upload_files(stage, PROJECT_BUCKET_NAME, s3_handler)
