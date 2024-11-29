import os
from typing import Dict, List, Any

import api.utils as api_utils
import pandas as pd
import streamlit as st
from aws_utils import iam, logs
import json
from typing import Tuple

from database import run_query


def get_table_columns() -> Dict[str, Dict[str, Any]]:
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


@st.cache_data
def get_suppliers() -> Tuple[List[Tuple[Any]], List[str]]:
    query = "SELECT DISTINCT(supplier) FROM supplier_stock;"
    return run_query(query)


@st.cache_data
def get_ebay_stores() -> Tuple[List[Tuple[Any]], List[str]]:
    query = "SELECT DISTINCT(ebay_store) FROM store;"
    return run_query(query)


def get_options(table_name: str) -> Tuple[List[str], str]:
    if table_name == "supplier_stock":
        results, _ = get_suppliers()
        results = list(set(results))
        results = [result[0] for result in results]
        results.sort()
        return (results, "supplier")
    else:
        results, _ = get_ebay_stores()
        results = list(set(results))
        results = [result[0] for result in results]
        results.sort()
        return (results, "ebay_store")


def display_title() -> None:
    st.title("Bulk Edits")


def select_table_name(table_columns: Dict[str, Dict[str, Any]]) -> str:
    return st.selectbox("Select Table Name", list(table_columns.keys()))


def select_edit_type() -> str:
    return st.selectbox("Select Edit Type", ["append", "update", "delete"])


def get_selected_columns(
    table_name: str, table_columns: Dict[str, Dict[str, Any]], edit_type: str
) -> List[str]:
    columns = [col["name"] for col in table_columns[table_name]["columns"]]
    necessary_column = table_columns[table_name]["necessary_columns"][0]
    necessary_columns = [necessary_column]

    default_columns = [necessary_column] if necessary_column in columns else []
    if edit_type == "update":
        default_columns.append(f"{necessary_column}_old")
        columns.append(f"{necessary_column}_old")
        necessary_columns.append(f"{necessary_column}_old")

    selected_columns = st.multiselect(
        "Select Columns",
        columns,
        default=default_columns
        + [col for col in columns if col not in default_columns],
    )
    st.write(f"Necessary columns: {necessary_columns}")
    return selected_columns


def display_data_types(
    table_name: str,
    table_columns: Dict[str, Dict[str, Any]],
    selected_columns: List[str],
) -> None:
    data_types = {
        col: next(
            (
                c["type"]
                for c in table_columns[table_name]["columns"]
                if c["name"] in col
            ),
            "Text",
        )
        for col in selected_columns
    }

    st.write(f"Partition Column: {table_columns[table_name]['partition_column']}")
    st.write("Data Types for Selected Columns:")
    df_display = (
        pd.DataFrame(data_types.items(), columns=["Column", "Data Type"])
        .set_index("Column")
        .transpose()
    )
    st.dataframe(df_display)


def handle_file_upload(
    selected_columns: List[str], edit_type: str, table_name: str
) -> None:
    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        if not all(col in df.columns for col in selected_columns):
            st.error("The uploaded CSV must contain the selected columns.")
        else:
            st.write("Edit Type: ", edit_type)
            st.write("Number of rows to edit: ", len(df))
            edit_table(df, table_name, edit_type)


def edit_table(df: pd.DataFrame, table_name: str, edit_type: str) -> None:
    if st.button("Edit Table"):
        try:
            with st.spinner("Editing table..."):
                json_data = json.loads(df.to_json(orient="records"))
                for item in json_data:
                    params = {
                        "table_name": table_name,
                        "type": edit_type,
                        "payload": json.dumps(item),
                    }
                    api_utils.post_request("items", params)

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


def main() -> None:
    display_title()
    iam.get_aws_credentials(st.secrets["aws_credentials"])

    table_columns = get_table_columns()
    table_name = select_table_name(table_columns)
    edit_type = select_edit_type()

    selected_columns = get_selected_columns(table_name, table_columns, edit_type)
    display_data_types(table_name, table_columns, selected_columns)

    options, partition_column = get_options(table_name)
    print(options)
    selected_value = st.selectbox(
        f"Select {partition_column}",
        options=options,
        index=None,
    )
    st.write("Selected Value:", selected_value)

    handle_file_upload(selected_columns, edit_type, table_name)
