import json
import os
import streamlit as st
from aws_utils import s3
from utils import PROJECT_BUCKET_NAME


def main():
    st.title("Configuration")
    json_key = "config/process_stock_feed_config.json"

    try:
        s3_handler = s3.S3Handler()
        config_data = s3_handler.load_json_from_s3(PROJECT_BUCKET_NAME, json_key)

        tab_view, tab_update, tab_functions = st.tabs(
            ["View Config", "Update Config", "Functions"]
        )

        with tab_view:
            st.json(config_data)

        with tab_update:
            st.subheader("Update Config")
            updated_config = st.text_area(
                "Edit Config JSON",
                json.dumps(config_data, indent=4),
                height=400,  # Increased height for larger editor
            )
            if st.button("Save Config"):
                try:
                    updated_data = json.loads(updated_config)
                    st.success("Config updated successfully!")
                except json.JSONDecodeError:
                    st.error("Invalid JSON format.")

        with tab_functions:
            st.subheader("Functions")
            functions_list = [
                "set_value_to_10_if_labelled_yes",
                "set_value_to_0_if_labelled_no",
                "set_value_to_10_if_labelled_yes_and_value_less_than_10",
                "set_value_to_0_if_labelled_no_and_value_greater_than_0",
            ]
            st.write("\n".join(functions_list))

    except Exception as e:
        st.error(f"Error loading config: {str(e)}")


if __name__ == "__main__":
    main()
