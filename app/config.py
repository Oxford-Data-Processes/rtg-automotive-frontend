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
                    s3_handler.upload_json_to_s3(
                        PROJECT_BUCKET_NAME, json_key, updated_data
                    )
                    st.success("Config updated successfully!")
                except json.JSONDecodeError:
                    st.error("Invalid JSON format.")

        with tab_functions:
            st.subheader("Functions")
            functions_dictionary = {
                "set_value_to_10_if_labelled_yes": "Returns 10 if 'yes' is found in the input string, otherwise returns 0.",
                "get_value_if_less_than_10_else_0": "Returns the input value if it's less than or equal to 10, otherwise returns 0.",
                "set_value_to_10_if_labelled_in_stock": "Returns 10 if the input is 'in stock', otherwise returns 0.",
                "set_value_to_10_if_product_in_list": "Always returns 10 regardless of input.",
            }
            for function, description in functions_dictionary.items():
                st.markdown(f"### `{function}`")
                st.markdown(f"> **Description:** {description}")
                st.markdown("---")  # Adds a horizontal line for better separation

    except Exception as e:
        st.error(f"Error loading config: {str(e)}")


if __name__ == "__main__":
    main()
