import streamlit as st
import pandas as pd
import boto3
import time

def run_athena_query(query):
    # Initialize a session using Boto3
    session = boto3.Session()
    athena_client = session.client("athena", region_name="eu-west-2")

    # Define the parameters for the query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "rtg_automotive"},
        WorkGroup="rtg-automotive-workgroup",
    )

    query_execution_id = response["QueryExecutionId"]
    # Wait for the query to complete
    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = query_status["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    if status == "SUCCEEDED":
        return athena_client.get_query_results(QueryExecutionId=query_execution_id)
    else:
        st.error(f"Query failed with status: {status}")
        return []

def format_query_results(results):
    # Check if results are empty
    if 'ResultSet' in results and 'Rows' in results['ResultSet']:
        # Extract the rows and convert them to a more usable format
        rows = results['ResultSet']['Rows']
        formatted_results = []
        if len(rows) > 1:  # Ensure there are rows to process
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            for row in rows[1:]:
                formatted_row = {}
                for i, cell in enumerate(row['Data']):
                    formatted_row[headers[i]] = cell.get('VarCharValue', None)  # Handle missing values
                formatted_results.append(formatted_row)
        return formatted_results
    else:
        st.error("No results returned from the query.")
        return []

def app_table_viewer():
    st.title("Table Viewer")

    # Define queries
    config = {
        "product": {
            "query": """
            SELECT *
            FROM "rtg_automotive"."product"
            WHERE {filter_column} = {filter_value}
            LIMIT 10
            """,
            "filter_columns": [{"name": "custom_label", "type": "text"}, {"name": "part_number", "type": "text"}, {"name": "supplier", "type": "text"}],
        },
        "store": {
            "query": """
            SELECT *
            FROM "rtg_automotive"."store"
            WHERE {filter_column} = {filter_value}
            LIMIT 10
            """,
            "filter_columns": [{"name": "item_id", "type": "integer"}, {"name": "custom_label", "type": "text"}, {"name": "supplier", "type": "text"}, {"name": "ebay_store", "type": "text"}],
        },
        "supplier_stock": {
            "query": """
            SELECT *
            FROM "rtg_automotive"."supplier_stock"
            WHERE {filter_column} = {filter_value}
            LIMIT 10
            """,
            "filter_columns": [{"name": "part_number", "type": "text"}, {"name": "supplier", "type": "text"}, {"name": "updated_date", "type": "text"}],
        },
    }

    # User selects a table
    table_selection = st.selectbox("Select a table", options=list(config.keys()))
    
    # User selects a filter column
    filter_columns = config[table_selection]["filter_columns"]
    filter_column_names = [col["name"] for col in filter_columns]
    selected_filter_column = st.selectbox("Select a filter column", options=filter_column_names)
    
    # User selects a value for the selected filter column
    filter_column_type = next(col["type"] for col in filter_columns if col["name"] == selected_filter_column)
    if filter_column_type == "text":
        filter_value = st.text_input(f"Enter value for {selected_filter_column}")
    elif filter_column_type == "integer":
        filter_value = st.number_input(f"Enter value for {selected_filter_column}", format="%d")
    
    # Execute query if a value is provided
    if st.button("Run Query") and filter_value:
        query = config[table_selection]["query"].format(filter_column=selected_filter_column, filter_value=f"'{filter_value}'" if filter_column_type == "text" else filter_value)
        results = run_athena_query(query)
        formatted_results = format_query_results(results)
        import pandas as pd
        df_results = pd.DataFrame(formatted_results)
        st.dataframe(df_results)

