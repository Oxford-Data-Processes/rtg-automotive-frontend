import io
import time
import uuid
import zipfile
from datetime import datetime
from typing import List, Tuple
import os

from database import run_query

import pandas as pd
import streamlit as st
from aws_utils import events, iam, s3, sqs, logs
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


def create_parquet_file_from_table(table_name: str, columns: str) -> None:
    query = f"SELECT {columns} FROM {table_name};"

    result, columns = run_query(query)
    df = pd.DataFrame(result, columns=columns)

    print(len(df))
    print(df.head())

    current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    s3_handler = s3.S3Handler()
    bucket_name = f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}"

    filename = f"{table_name}/{current_timestamp}/{uuid.uuid4()}.parquet"

    s3_handler.upload_parquet_to_s3(bucket_name, filename, df.to_parquet())


def generate_helper_tables() -> None:

    create_parquet_file_from_table("supplier_stock", "part_number,custom_label")
    create_parquet_file_from_table("store", "custom_label")

    run_query("DROP TABLE IF EXISTS supplier_stock_ranked;")
    supplier_stock_ranked_file_path = os.path.join(
        os.path.dirname(__file__), "sql", "supplier_stock_ranked.sql"
    )
    supplier_stock_ranked_sql = open(supplier_stock_ranked_file_path).read()
    run_query(supplier_stock_ranked_sql)

    run_query("DROP TABLE IF EXISTS store_filtered;")
    store_filtered_file_path = os.path.join(
        os.path.dirname(__file__), "sql", "store_filtered.sql"
    )
    store_filtered_sql = open(store_filtered_file_path).read()
    run_query(store_filtered_sql)


def handle_ebay_queue(sqs_queue_url: str) -> None:
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
    with st.spinner(
        "Generating eBay upload files, this may take approximately 10 minutes..."
    ):
        start_time = time.time()
        st.write(
            f"Start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        time.sleep(10)


def load_ebay_table(s3_handler) -> pd.DataFrame:

    folders = s3_handler.list_objects(PROJECT_BUCKET_NAME, "ebay/table/")

    folder_paths = [
        (folder["Key"].split("/")[-2], folder["Key"])
        for folder in folders
        if folder["Key"].endswith(".parquet")
    ]

    if not folder_paths:
        raise ValueError("No parquet files found in the specified S3 path.")

    latest_timestamp = max([path[0] for path in folder_paths if len(path[0]) == 19])

    folder_paths = [
        (timestamp, key)
        for timestamp, key in folder_paths
        if timestamp == latest_timestamp
    ]

    dfs = []
    for _, folder_path in folder_paths:
        parquet_data = s3_handler.load_parquet_from_s3(PROJECT_BUCKET_NAME, folder_path)
        df = pd.read_parquet(io.BytesIO(parquet_data))
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def generate_ebay_upload_files(logs_handler) -> None:
    sqs_queue_url = "rtg-automotive-lambda-queue"
    handle_ebay_queue(sqs_queue_url)

    s3_handler = s3.S3Handler()

    df = load_ebay_table(s3_handler)

    ebay_df = create_ebay_dataframe(df)
    stores = list(ebay_df["Store"].unique())
    ebay_dfs = [
        (ebay_df[ebay_df["Store"] == store].drop(columns=["Store"]), store)
        for store in stores
    ]

    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    zip_data = zip_dataframes(ebay_dfs).getvalue()
    s3_handler.upload_generic_file_to_s3(
        PROJECT_BUCKET_NAME,
        f"ebay/zip_folders/{timestamp}/ebay_upload_files.zip",
        zip_data,
    )

    logs_handler.log_action(
        f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}",
        "frontend",
        f"EBAY_UPLOAD_FILES_GENERATED",
        "admin",
    )

    st.download_button(
        label="Download eBay Upload Files",
        data=zip_data,
        file_name="ebay_upload_files.zip",
        mime="application/zip",
    )


def main() -> None:
    st.title("Stock Manager")
    iam.get_aws_credentials(st.secrets["aws_credentials"])

    s3_handler = s3.S3Handler()

    sqs_queue_url = "rtg-automotive-lambda-queue"

    if st.button("Build Helper Tables"):
        with st.spinner(
            "Building helper tables, this may take approximately 5 minutes..."
        ):
            start_time = time.time()
        st.write(
            f"Start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        generate_helper_tables()
        time_taken = time.time() - start_time
        minutes, seconds = divmod(time_taken, 60)
        logs_handler = logs.LogsHandler()
        logs_handler.log_action(
            f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}",
            "frontend",
            f"HELPER_TABLES_GENERATED",
            "admin",
        )
        st.success(
            f"Helper tables generated successfully in {int(minutes)} minutes and {seconds:.2f} seconds."
        )

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

    if st.button("Generate eBay Store Upload Files"):
        generate_ebay_upload_files(logs_handler)
