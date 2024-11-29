import os
import time

import mysql.connector
from aws_utils import iam, rds
from mysql.connector import Error
import streamlit as st

iam.get_aws_credentials(st.secrets["aws_credentials"])

rds_handler = rds.RDSHandler()
rds_instance = rds_handler.get_rds_instance_by_identifier("rtg-automotive-db")
rds_endpoint = rds_instance["Endpoint"]


def create_connection():
    """Create a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=rds_endpoint,
            database="rtg_automotive",
            user="admin",
            password="password",
            connection_timeout=10,
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            print("Connected to MySQL Server version:", db_info)
            return connection
    except Error as e:
        print("Error while connecting to MySQL:", e)
        return None


def run_query(query: str):
    """Run a query using the connection.

    Returns:
        tuple: (results, columns) where results is a list of rows and columns is a list of column names
        None: if there's an error
    """
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            if query.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
                # Get column names from cursor description
                columns = [column[0] for column in cursor.description]
                return result, columns
            else:
                return None  # No results or columns for non-SELECT queries
        except Error as e:
            print("Error executing query:", e)
            return None
        finally:
            cursor.close()
            connection.close()
    return None
