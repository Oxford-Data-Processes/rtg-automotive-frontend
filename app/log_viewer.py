import streamlit as st
import pandas as pd
from aws_utils import logs


def app_log_viewer(aws_account_id):
    st.title("Log Viewer")
    bucket_name = f"rtg-automotive-bucket-{aws_account_id}"
    project_name = "frontend"

    logs_handler = logs.LogsHandler()
    log_data = logs_handler.get_logs(bucket_name, project_name)

    # Convert log_data to a DataFrame and display it, sorting by timestamp to show most recent logs first
    log_df = pd.DataFrame(log_data)
    log_df = log_df.sort_values(
        by="timestamp", ascending=False
    )  # Assuming 'timestamp' is a key in log_content
    st.dataframe(log_df)
