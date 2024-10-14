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
    st.title("Athena Query Results")

    # Define queries
    queries = {
        "Load Product Data": """
            SELECT custom_label, part_number, supplier
            FROM "rtg_automotive"."product"
            LIMIT 10
        """,
        "Load Store Data": """
            SELECT item_id, custom_label, supplier, ebay_store
            FROM "rtg_automotive"."store"
            LIMIT 10
        """,
        "Load Supplier Stock Data": """
            SELECT part_number, supplier, updated_date
            FROM "rtg_automotive"."supplier_stock"
            LIMIT 10
        """
    }

    # Load data for each query
    for button_label, query in queries.items():
        if st.button(button_label):
            results = run_athena_query(query)
            if results:
                formatted_results = format_query_results(results)  # Format the results
                st.write(pd.DataFrame(formatted_results))  # Use formatted results

if __name__ == "__main__":
    app_table_viewer()
