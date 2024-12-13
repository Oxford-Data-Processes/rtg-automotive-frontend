import os
import time
from datetime import datetime
import uuid

import pandas as pd
import streamlit as st

from aws_utils import events, logs, s3, sqs
from utils import PROJECT_BUCKET_NAME


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

        while True:
            messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)
            for message in messages:
                if "Ebay table generated" in message["Body"]:
                    time_taken = time.time() - start_time
                    minutes, seconds = divmod(time_taken, 60)
                    st.success(
                        f"Ebay upload files generated successfully in {int(minutes)} minutes and {seconds:.2f} seconds."
                    )
                    break
            else:
                time.sleep(10)
                continue
            break


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
    st.title("Ebay Upload Generator")

    logs_handler = logs.LogsHandler()

    if st.button("Generate eBay Store Upload Files"):
        generate_ebay_upload_files(logs_handler)
