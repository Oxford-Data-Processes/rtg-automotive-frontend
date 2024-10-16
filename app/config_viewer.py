import json
import os
import streamlit as st
from aws_utils import s3


def app_config_viewer(aws_account_id):
    st.title("Process Stock Feed Config Viewer")
    bucket_name = f"rtg-automotive-bucket-{aws_account_id}"
    json_key = "config/process_stock_feed_config.json"

    try:
        s3_handler = s3.S3Handler(
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"],
            os.environ["AWS_SESSION_TOKEN"],
            "eu-west-2",
        )
        config_data = s3_handler.load_json_from_s3(bucket_name, json_key)

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
