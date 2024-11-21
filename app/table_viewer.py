import streamlit as st
import pandas as pd
from aws_utils import iam
import api.utils as api_utils
import json


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
    if st.button("View Query"):
        st.write(params)

    if st.button("Run Query"):

        results = api_utils.get_request("items", params)
        if len(results) > 0:
            st.dataframe(pd.DataFrame(results))
        else:
            st.write("No results found")
