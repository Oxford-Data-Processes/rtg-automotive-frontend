import json
import streamlit as st
from aws import get_s3_client


def load_json_from_s3(bucket_name, json_key):
    s3_client = get_s3_client()
    json_object = s3_client.get_object(Bucket=bucket_name, Key=json_key)
    json_data = json_object["Body"].read()
    return json.loads(json_data)


def app_config_viewer(aws_account_id):
    st.title("Process Stock Feed Config Viewer")
    bucket_name = f"rtg-automotive-bucket-{aws_account_id}"
    json_key = "config/process_stock_feed_config.json"

    try:
        config_data = load_json_from_s3(bucket_name, json_key)

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
