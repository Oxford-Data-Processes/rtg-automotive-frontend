import json
import zipfile
from io import BytesIO
from typing import Dict, List, Any, Optional

import api.utils as api_utils
import pandas as pd
import streamlit as st
from aws_utils import iam, s3
import os


def get_table_config() -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    return {
        "store": {
            "filter_columns": [
                {"name": "item_id", "type": "integer"},
                {"name": "custom_label", "type": "text"},
                {"name": "supplier", "type": "text"},
                {"name": "ebay_store", "type": "text"},
            ],
        },
        "supplier_stock": {
            "filter_columns": [
                {"name": "part_number", "type": "text"},
                {"name": "custom_label", "type": "text"},
                {"name": "supplier", "type": "text"},
                {"name": "updated_date", "type": "text"},
            ],
        },
    }


def handle_filter_selection(filter_columns: List[Dict[str, str]]) -> None:
    selected_filter_column = select_filter_column(filter_columns)
    if selected_filter_column:
        filter_column_type = get_filter_column_type(
            filter_columns, selected_filter_column
        )
        single_value = st.text_input(
            f"Enter a single value for {selected_filter_column}"
        )
        filter_values = get_filter_values(selected_filter_column, filter_column_type)
        update_filter_values(selected_filter_column, single_value, filter_values)


def select_filter_column(filter_columns: List[Dict[str, str]]) -> str:
    filter_column_names = [col["name"] for col in filter_columns]
    return st.selectbox(
        "Select a filter column",
        options=filter_column_names,
        key=f"filter_column_{len(st.session_state.filters)}",
        index=0,
    )


def get_filter_column_type(
    filter_columns: List[Dict[str, str]], selected_filter_column: str
) -> str:
    return next(
        col["type"] for col in filter_columns if col["name"] == selected_filter_column
    )


def update_filter_values(
    selected_filter_column: str, single_value: str, filter_values: List[Any]
) -> None:
    if single_value:
        filter_values.append(single_value)
    if filter_values:
        if "filters" not in st.session_state:
            st.session_state.filters = {}
        st.session_state.filters[selected_filter_column] = filter_values


def get_filter_values(
    selected_filter_column: str, filter_column_type: str
) -> List[Any]:
    uploaded_file = st.file_uploader(
        f"Upload CSV for {selected_filter_column}", type=["csv"]
    )
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        if selected_filter_column in df.columns:
            return df[selected_filter_column].dropna().unique().tolist()
    return []


def convert_to_excel(data: List[Dict[str, Any]]) -> bytes:
    df = pd.DataFrame(data)

    # If dataframe exceeds Excel's row limit, split it into chunks
    max_rows = 1000000  # Slightly less than Excel's limit for safety
    if len(df) > max_rows:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            # Split dataframe into chunks and write to different sheets
            for i, chunk_start in enumerate(range(0, len(df), max_rows)):
                chunk = df.iloc[chunk_start : chunk_start + max_rows]
                chunk.to_excel(writer, sheet_name=f"Sheet_{i+1}", index=False)
    else:
        output = BytesIO()
        df.to_excel(output, index=False, engine="openpyxl")

    output.seek(0)
    return output.getvalue()


def download_excels_as_zip(data_dictionary: Dict[str, bytes]) -> None:
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for file_name, excel_data in data_dictionary.items():
            zip_file.writestr(file_name, excel_data)

    zip_buffer.seek(0)
    st.download_button(
        label="Download All Excel Files as Zip",
        data=zip_buffer,
        file_name="excel_files.zip",
        mime="application/zip",
    )


def get_table_from_s3(table_name: str) -> List[Dict[str, Any]]:
    s3_handler = s3.S3Handler()
    bucket_name = f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}"

    folders = s3_handler.list_objects(bucket_name, f"{table_name}/")
    folder_paths = [
        (folder["Key"].split("/")[-2], folder["Key"])
        for folder in folders
        if folder["Key"].endswith(".parquet")
    ]
    latest_file = sorted(folder_paths, key=lambda x: x[0], reverse=True)[0][1]
    table_data = s3_handler.load_parquet_from_s3(bucket_name, latest_file)
    df = pd.read_parquet(BytesIO(table_data))
    table_dictionary = [
        {col: row[col] for col in df.columns} for _, row in df.iterrows()
    ]

    return table_dictionary


def run_query(
    params: Dict[str, Any], table_selection: str, split_by_column: str
) -> None:
    if st.button("Run Query"):
        del params["split_by_column"]
        if params["limit"] == 0:
            results = get_table_from_s3(table_selection)
        else:
            results = api_utils.get_request("items", params)
            print(results)
            print(type(results))
        if results:
            display_results(results, table_selection, split_by_column)
        else:
            st.write("No results found")


def display_results(
    results: List[Dict[str, Any]], table_selection: str, split_by_column: str
) -> None:
    st.dataframe(pd.DataFrame(results[:100]))
    with st.spinner("Building the Excel export..."):
        if split_by_column:
            create_split_downloads(results, table_selection, split_by_column)
        else:
            download_single_file(results, table_selection)


def create_split_downloads(
    results: List[Dict[str, Any]], table_selection: str, split_by_column: str
) -> None:
    results_df = pd.DataFrame(results)
    if split_by_column in results_df.columns:
        unique_values = results_df[split_by_column].unique()
        data_dictionary = {}
        for value in unique_values:
            filtered_results = results_df[results_df[split_by_column] == value]
            data_dictionary[f"{table_selection}_{split_by_column}_{value}.xlsx"] = (
                convert_to_excel(filtered_results.to_dict(orient="records"))
            )
            print(data_dictionary.keys())
        download_excels_as_zip(data_dictionary)
    else:
        st.write(f"Column '{split_by_column}' not found in results.")


def download_single_file(results: List[Dict[str, Any]], table_selection: str) -> None:
    data_dictionary = {f"{table_selection}.xlsx": convert_to_excel(results)}
    download_excels_as_zip(data_dictionary)


def select_table(config: Dict[str, Any]) -> str:
    return st.selectbox(
        "Select Table name", options=sorted(list(config.keys())), key="table_selection"
    )


def initialize_filters(table_selection: str) -> None:
    if "filters" not in st.session_state:
        st.session_state.filters = {}

    if (
        "previous_table_selection" not in st.session_state
        or st.session_state.previous_table_selection != table_selection
    ):
        st.session_state.filters = {}
        st.session_state.previous_table_selection = table_selection


def clear_filters_button() -> None:
    if st.button("Clear Filters"):
        st.session_state.filters = {}


def get_result_limit() -> int:
    return st.number_input(
        "Number of results to return (default is 10, set it to 0 for ALL)",
        value=10,
        format="%d",
        min_value=0,
    )


def build_query_params(table_selection: str, result_limit: int) -> Dict[str, Any]:
    print("Filters: ", st.session_state.filters)
    return {
        "table_name": table_selection,
        "filters": json.dumps(st.session_state.filters),
        "limit": result_limit,
    }


def select_split_by_column(filter_columns: List[Dict[str, Any]]) -> str:
    return st.selectbox(
        "Split by column (optional)",
        options=[""] + [item["name"] for item in filter_columns],
        key="split_by_selection",
        index=0,
    )


def main() -> None:
    st.title("Table Viewer")
    iam.get_aws_credentials(st.secrets["aws_credentials"])
    config = get_table_config()

    table_selection = select_table(config)
    initialize_filters(table_selection)

    filter_columns = config[table_selection]["filter_columns"]
    handle_filter_selection(filter_columns)

    clear_filters_button()
    result_limit = get_result_limit()

    params = build_query_params(table_selection, result_limit)

    split_by_column = select_split_by_column(filter_columns)
    params["split_by_column"] = split_by_column

    st.write("Query Parameters:")
    st.write(params)

    run_query(params, table_selection, split_by_column)
