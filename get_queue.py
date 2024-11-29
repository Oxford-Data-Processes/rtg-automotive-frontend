from aws_utils import sqs, iam
import streamlit as st

iam.get_aws_credentials(st.secrets["aws_credentials"])

sqs_handler = sqs.SQSHandler()
sqs_queue_url = "rtg-automotive-lambda-queue"
messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)
for message in messages:
    print(message["Body"])
