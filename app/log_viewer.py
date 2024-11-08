import streamlit as st
import pandas as pd
from aws_utils import logs, iam
import os
from utils import PROJECT_BUCKET_NAME


def app_log_viewer():
    iam.get_aws_credentials(st.secrets["aws_credentials"])
    st.title("Log Viewer")
    project_name = "frontend"

    logs_handler = logs.LogsHandler()
    log_data = logs_handler.get_logs(PROJECT_BUCKET_NAME, project_name)

    log_df = pd.DataFrame(log_data)
    log_df = log_df.sort_values(by="timestamp", ascending=False)
    st.dataframe(log_df)
