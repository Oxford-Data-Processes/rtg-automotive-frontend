import os
from typing import Any

import pandas as pd
import streamlit as st
from aws_utils import iam, logs


def load_logs(logs_handler: logs.LogsHandler, bucket_name: str) -> None:
    iam.get_aws_credentials(st.secrets["aws_credentials"])
    log_messages: Any = logs_handler.get_logs(bucket_name, "frontend")
    log_df: pd.DataFrame = pd.DataFrame(log_messages).sort_values(
        by="timestamp", ascending=False
    )
    st.dataframe(log_df.drop(columns=["log_id"]), use_container_width=True)


def main() -> None:
    project: str = "rtg-automotive"
    bucket_name: str = f"{project}-bucket-{os.environ['AWS_ACCOUNT_ID']}"
    st.title("Logs")
    logs_handler: logs.LogsHandler = logs.LogsHandler()

    load_logs(logs_handler, bucket_name)

    refresh_button: bool = st.button("Refresh Logs")
    if refresh_button:
        load_logs(logs_handler, bucket_name)


if __name__ == "__main__":
    main()
