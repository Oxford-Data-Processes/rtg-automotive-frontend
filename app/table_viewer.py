import json
import zipfile
from io import BytesIO

import api.utils as api_utils
import pandas as pd
import streamlit as st
from aws_utils import iam


def get_table_config():
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
                {"name": "supplier", "type": "text"},
                {"name": "updated_date", "type": "text"},
            ],
        },
    }


def handle_filter_selection(filter_columns):
    filter_column_names = [col["name"] for col in filter_columns]
    selected_filter_column = st.selectbox(
        "Select a filter column",
        options=filter_column_names,
        key=f"filter_column_{len(st.session_state.filters)}",
        index=0,
    )

    if selected_filter_column:
        filter_column_type = next(
            col["type"]
            for col in filter_columns
            if col["name"] == selected_filter_column
        )

        # Option to provide a single value
        single_value = st.text_input(
            f"Enter a single value for {selected_filter_column}"
        )

        # Get filter values from CSV upload
        filter_values = get_filter_values(selected_filter_column, filter_column_type)

        # Combine single value and filter values from CSV
        if single_value:
            filter_values.append(single_value)

        if filter_values:
            formatted_filters = format_filters(
                selected_filter_column, filter_values, filter_column_type
            )
            # Change to a dictionary instead of a list
            if "filters" not in st.session_state:
                st.session_state.filters = {}
            st.session_state.filters[selected_filter_column] = filter_values


def get_filter_values(selected_filter_column, filter_column_type):
    uploaded_file = st.file_uploader(
        f"Upload CSV for {selected_filter_column}", type=["csv"]
    )
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        if selected_filter_column in df.columns:
            return df[selected_filter_column].dropna().unique().tolist()
    return []


def format_filters(selected_filter_column, filter_values, filter_column_type):
    if filter_column_type == "text":
        return {selected_filter_column: [f"'{value}'" for value in filter_values]}
    else:
        return {selected_filter_column: [int(value) for value in filter_values]}


def convert_to_excel(data):
    df = pd.DataFrame(data)
    output = BytesIO()
    df.to_excel(output, index=False, engine="openpyxl")
    output.seek(0)
    return output.getvalue()


def download_excels_as_zip(data_dictionary):
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


def run_query(params, table_selection, split_by_column):
    if st.button("Run Query"):
        del params["split_by_column"]
        results = api_utils.get_request("items", params)
        if results:
            st.dataframe(pd.DataFrame(results))
            if split_by_column:  # Check if split_by_column is not empty
                # Ensure results is a DataFrame for proper indexing
                results_df = pd.DataFrame(results)
                if split_by_column in results_df.columns:
                    unique_values = results_df[split_by_column].unique()
                    data_dictionary = {}
                    for value in unique_values:
                        filtered_results = results_df[
                            results_df[split_by_column] == value
                        ]
                        data_dictionary[
                            f"{table_selection}_{value}.xlsx"
                        ] = convert_to_excel(filtered_results.to_dict(orient="records"))
                    download_excels_as_zip(data_dictionary)

                else:
                    st.write(f"Column '{split_by_column}' not found in results.")
            else:
                data_dictionary = {f"{table_selection}.xlsx": convert_to_excel(results)}
                download_excels_as_zip(data_dictionary)
        else:
            st.write("No results found")


def main():
    st.title("Table Viewer")
    iam.get_aws_credentials(st.secrets["aws_credentials"])
    config = get_table_config()

    table_selection = st.selectbox(
        "Select a table", options=list(config.keys()), key="table_selection"
    )

    if "filters" not in st.session_state:
        st.session_state.filters = {}

    if (
        "previous_table_selection" not in st.session_state
        or st.session_state.previous_table_selection != table_selection
    ):
        st.session_state.filters = {}
        st.session_state.previous_table_selection = table_selection

    filter_columns = config[table_selection]["filter_columns"]
    handle_filter_selection(filter_columns)

    if st.button("Clear Filters"):
        st.session_state.filters = {}

    result_limit = st.number_input(
        "Number of results to display (default is 10, set it to 0 for ALL)",
        value=10,
        format="%d",
        min_value=0,
    )

    params = {
        "table_name": table_selection,
        "filters": json.dumps(st.session_state.filters),
        "limit": result_limit,
    }

    split_by_column = st.selectbox(
        "Split by column (optional)",
        options=[""] + [item["name"] for item in filter_columns],
        key="split_by_selection",
        index=0,
    )

    params["split_by_column"] = split_by_column

    st.write("Query Parameters:")
    st.write(params)

    run_query(params, table_selection, split_by_column)
