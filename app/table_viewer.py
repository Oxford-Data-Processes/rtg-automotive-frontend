import streamlit as st
import pandas as pd
from aws import run_athena_query


def format_query_results(results):
    if "ResultSet" in results and "Rows" in results["ResultSet"]:
        return extract_rows(results["ResultSet"]["Rows"])
    else:
        st.error("No results returned from the query.")
        return []


def extract_rows(rows):
    formatted_results = []
    if len(rows) > 1:  # Ensure there are rows to process
        headers = [col["VarCharValue"] for col in rows[0]["Data"]]
        for row in rows[1:]:
            formatted_row = {
                headers[i]: cell.get("VarCharValue", None)
                for i, cell in enumerate(row["Data"])
            }
            formatted_results.append(formatted_row)
    return formatted_results


def get_table_config():
    return {
        "product": {
            "query": """
            SELECT *
            FROM "rtg_automotive"."product"
            WHERE {filters}
            """,
            "filter_columns": [
                {"name": "custom_label", "type": "text"},
                {"name": "part_number", "type": "text"},
                {"name": "supplier", "type": "text"},
            ],
        },
        "store": {
            "query": """
            SELECT *
            FROM "rtg_automotive"."store"
            WHERE {filters}
            """,
            "filter_columns": [
                {"name": "item_id", "type": "integer"},
                {"name": "custom_label", "type": "text"},
                {"name": "supplier", "type": "text"},
                {"name": "ebay_store", "type": "text"},
            ],
        },
        "supplier_stock": {
            "query": """
            SELECT *
            FROM "rtg_automotive"."supplier_stock"
            WHERE {filters}
            """,
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
        filter_value = get_filter_value(selected_filter_column, filter_column_type)

        if filter_value:
            st.session_state.filters.append(
                format_filter(selected_filter_column, filter_value, filter_column_type)
            )


def get_filter_value(selected_filter_column, filter_column_type):
    if filter_column_type == "text":
        return st.text_input(
            f"Enter value for {selected_filter_column}",
            key=f"filter_value_{len(st.session_state.filters)}",
        )
    elif filter_column_type == "integer":
        return st.number_input(
            f"Enter value for {selected_filter_column}",
            format="%d",
            key=f"filter_value_{len(st.session_state.filters)}",
            value=0.0,
        )


def format_filter(selected_filter_column, filter_value, filter_column_type):
    return (
        f"{selected_filter_column} = '{filter_value}'"
        if filter_column_type == "text"
        else f"{selected_filter_column} = {int(filter_value)}"
    )


def app_table_viewer():
    st.title("Table Viewer")
    config = get_table_config()

    table_selection = st.selectbox(
        "Select a table", options=list(config.keys()), key="table_selection"
    )

    if "filters" not in st.session_state:
        st.session_state.filters = []

    if (
        "previous_table_selection" not in st.session_state
        or st.session_state.previous_table_selection != table_selection
    ):
        st.session_state.filters = []
        st.session_state.previous_table_selection = table_selection

    filter_columns = config[table_selection]["filter_columns"]
    handle_filter_selection(filter_columns)

    if st.button("Clear Filters"):
        st.session_state.filters = []

    result_limit = st.number_input(
        "Number of results to display (default is 10, set it to 0 for ALL)",
        value=10,
        format="%d",
        min_value=0,
    )

    query = build_query(
        config[table_selection]["query"], st.session_state.filters, result_limit
    )

    st.write("SQL Query to be executed:")
    st.code(query)

    if st.button("Run Query"):
        results = run_athena_query(query)
        formatted_results = format_query_results(results)

        df_results = pd.DataFrame(formatted_results)
        st.dataframe(df_results)


def build_query(base_query, filters, result_limit):
    filters_clause = " AND ".join(filters) if filters else "1=1"
    limit_clause = f"LIMIT {result_limit}" if result_limit > 0 else ""
    return base_query.format(filters=filters_clause) + " " + limit_clause
