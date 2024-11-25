import os
import streamlit as st
import pandas as pd
from aws_utils import logs, iam
import api.utils as api_utils


def get_table_columns():
    return {
        "supplier_stock": {
            "columns": [
                {"name": "part_number", "type": "Text"},
                {"name": "quantity", "type": "Integer"},
                {"name": "updated_date", "type": "Text"},
                {"name": "supplier", "type": "Text"},
            ],
            "necessary_columns": ["part_number"],
            "partition_column": "supplier",
        },
        "store": {
            "columns": [
                {"name": "item_id", "type": "Integer"},
                {"name": "custom_label", "type": "Text"},
                {"name": "title", "type": "Text"},
                {"name": "current_price", "type": "Decimal"},
                {"name": "prefix", "type": "Text"},
                {"name": "uk_rtg", "type": "Text"},
                {"name": "fps_wds_dir", "type": "Text"},
                {"name": "payment_profile_name", "type": "Text"},
                {"name": "shipping_profile_name", "type": "Text"},
                {"name": "return_profile_name", "type": "Text"},
                {"name": "supplier", "type": "Text"},
                {"name": "ebay_store", "type": "Text"},
            ],
            "necessary_columns": ["item_id"],
            "partition_column": "ebay_store",
        },
    }


def get_options(table_name, partition_column):

    params = {
        "table_name": table_name,
        "columns": ",".join([partition_column]),
        "limit": 10000,
    }

    results = api_utils.get_request("items", params)
    return list(set([result[partition_column] for result in results]))


def main():
    st.title("Bulk Edits")
    iam.get_aws_credentials(st.secrets["aws_credentials"])

    table_columns = get_table_columns()
    table_name = st.selectbox("Select Table Name", list(table_columns.keys()))

    edit_type = st.selectbox("Select Edit Type", ["append", "update", "delete"])
    columns = [col["name"] for col in table_columns[table_name]["columns"]]
    necessary_column = table_columns[table_name]["necessary_columns"][0]
    necessary_columns = [necessary_column]

    default_columns = [necessary_column] if necessary_column in columns else []
    if edit_type == "update":
        default_columns.append(f"{necessary_column}_old")
        columns.append(f"{necessary_column}_old")
        necessary_columns.append(f"{necessary_column}_old")

    # Ensure necessary columns are displayed first in default columns
    selected_columns = st.multiselect(
        "Select Columns",
        columns,
        default=default_columns
        + [col for col in columns if col not in default_columns],
    )
    st.write(f"Necessary columns: {necessary_columns}")

    data_types = {
        col: next(
            (
                c["type"]
                for c in table_columns[table_name]["columns"]
                if c["name"] == col
            ),
            None,
        )
        for col in selected_columns
    }

    st.write(f"Partition Column: {table_columns[table_name]['partition_column']}")

    options = get_options(table_name, table_columns[table_name]["partition_column"])

    selected_value = st.selectbox(
        f"Select {table_columns[table_name]['partition_column']}",
        options=options,
        index=0,
    )

    st.write("Selected Value:", selected_value)

    st.write("Data Types for Selected Columns:")
    df_display = (
        pd.DataFrame(data_types.items(), columns=["Column", "Data Type"])
        .set_index("Column")
        .transpose()
    )

    st.dataframe(df_display)

    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        if not all(col in df.columns for col in selected_columns):
            st.error("The uploaded CSV must contain the selected columns.")
        else:

            st.write("Edit Type: ", edit_type)
            st.write("Number of rows to edit: ", len(df))

            if st.button("Edit Table"):
                try:
                    with st.spinner("Editing table..."):

                        json_data = df.to_json(orient="records")
                        for item in json_data:
                            params = {
                                "table_name": table_name,
                                "type": edit_type,
                                "payload": item,
                            }
                            api_utils.post_request("items", params)

                        iam.get_aws_credentials(st.secrets["aws_credentials"])
                        logs_handler = logs.LogsHandler()
                        logs_handler.log_action(
                            f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}",
                            "frontend",
                            f"{edit_type.upper()} | table={table_name} | number_of_edits={len(df)}",
                            "admin",
                        )
                        st.success("Changes saved to the database.")
                except Exception as e:
                    st.error(f"Error: {e}")
