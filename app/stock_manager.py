import boto3
import streamlit as st
import os
import time
from io import BytesIO
import pandas as pd
from typing import List, Tuple
import io
import zipfile
import re



def get_credentials(aws_account_id, role):
    role_arn = f"arn:aws:iam::{aws_account_id}:role/{role}"
    session_name = f"MySession-{int(time.time())}"

    sts_client = boto3.client(
        "sts",
        aws_access_key_id=st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"],
    )

    # Assume the role
    response = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    # Extract the credentials
    credentials = response["Credentials"]
    access_key_id = credentials["AccessKeyId"]
    secret_access_key = credentials["SecretAccessKey"]
    session_token = credentials["SessionToken"]

    os.environ["AWS_ACCESS_KEY_ID"] = access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_access_key
    os.environ["AWS_SESSION_TOKEN"] = session_token


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
    ebay_df["ItemID"] = ebay_df["ItemID"].astype(int)
    return ebay_df


def get_last_csv_from_s3(bucket_name, prefix, s3_client):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    csv_files = [
        obj for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")
    ]
    csv_files.sort(key=lambda x: x["LastModified"], reverse=True)
    return csv_files[0]["Key"] if csv_files else None


def extract_datetime_from_sns_message(message):
    # Regular expression to find the datetime in the message
    match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", message)
    return match.group(0) if match else None


def get_all_sqs_messages(queue_url):
    sqs_client = boto3.client(
        "sqs",
        region_name="eu-west-2",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"],
    )
    all_messages = []
    while True:
        # Receive messages from the SQS queue
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"],
        )

        # Check if there are any messages
        messages = response.get("Messages", [])
        if not messages:
            break  # Exit the loop if no more messages

        for message in messages:
            timestamp = extract_datetime_from_sns_message(message["Body"])
            message_body = message["Body"]
            all_messages.append({"timestamp": timestamp, "message": message_body})

    all_messages.sort(key=lambda x: x["timestamp"])

    return all_messages


def upload_file_to_s3(file, bucket_name, date, s3_client):
    year = date.split("-")[0]
    month = date.split("-")[1]
    day = date.split("-")[2]
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"stock_feed/year={year}/month={month}/day={day}/{file.name.replace(' ','_')}",
        Body=file.getvalue(),
    )
    st.success(f"File {file.name} uploaded successfully, processing...")


def trigger_generate_ebay_table_lambda(stage, aws_account_id):
    lambda_client = boto3.client(
        "lambda",
        region_name="eu-west-2",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"],
    )
    try:
        response = lambda_client.invoke(
            FunctionName=f"arn:aws:lambda:eu-west-2:{aws_account_id}:function:rtg-automotive-{stage}-generate-ebay-table",
            InvocationType="RequestResponse",
        )
        time.sleep(2)
        return True
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return False


def load_csv_from_s3(bucket_name, csv_key, s3_client):
    csv_object = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
    csv_data = csv_object["Body"].read()
    df = pd.read_csv(BytesIO(csv_data))
    return df


def app_stock_manager(stage, aws_account_id):

    if stage == "prod":
        role = "ProdAdminRole"
    else:
        role = "DevAdminRole"

    st.title("eBay Store Upload Generator")
    get_credentials(aws_account_id, role)

    s3_client = boto3.client(
        "s3",
        region_name="eu-west-2",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"],
    )

    sqs_queue_url = (
        f"https://sqs.eu-west-2.amazonaws.com/{aws_account_id}/rtg-automotive-sqs-queue"
    )
    stock_feed_bucket_name = f"rtg-automotive-stock-feed-bucket-{aws_account_id}"
    project_bucket_name = f"rtg-automotive-bucket-{aws_account_id}"

    uploaded_files = st.file_uploader(
        "Upload Excel files", type=["xlsx"], accept_multiple_files=True
    )

    date = st.date_input("Select a date", value=pd.Timestamp.now().date()).strftime(
        "%Y-%m-%d"
    )

    if st.button("Upload Files") and date:
        get_credentials(aws_account_id, role)
        if uploaded_files:
            for uploaded_file in uploaded_files:
                upload_file_to_s3(
                    uploaded_file, stock_feed_bucket_name, date, s3_client
                )
            time.sleep(len(uploaded_files) * 4)
            messages = get_all_sqs_messages(sqs_queue_url)[-len(uploaded_files) :]
            for message in messages:
                st.write(message["message"])
        else:
            st.warning("Please upload at least one file first.")

    if st.button("Generate eBay Store Upload Files"):
        get_credentials(aws_account_id, role)
        if trigger_generate_ebay_table_lambda(stage, aws_account_id):
            last_csv_key = get_last_csv_from_s3(
                project_bucket_name, "athena-results/", s3_client
            )
            if last_csv_key:
                df = load_csv_from_s3(project_bucket_name, last_csv_key, s3_client)
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

            else:
                st.warning("No CSV file found in the specified S3 path.")