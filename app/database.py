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
    """Run a query using the connection."""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print("Error executing query:", e)
            return None
        finally:
            cursor.close()
            connection.close()
    return None
