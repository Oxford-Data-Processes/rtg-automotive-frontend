import json
import os
import streamlit as st
from aws_utils import s3
from utils import PROJECT_BUCKET_NAME


def app_config_viewer():
    st.title("Process Stock Feed Config Viewer")
    json_key = "config/process_stock_feed_config.json"

    try:
        s3_handler = s3.S3Handler()
        config_data = s3_handler.load_json_from_s3(PROJECT_BUCKET_NAME, json_key)

        for item in config_data.values():
            if item.get("process_func") == "process_numerical":
                item["process_func"] = (
                    """lambda x: max(0, min(x, 10)) if isinstance(x, (int, float)) and x > 0 else 0"""
                )

        st.json(config_data)

    except Exception as e:
        st.error(f"Error loading config: {str(e)}")


if __name__ == "__main__":
    app_config_viewer()
