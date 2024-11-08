import streamlit as st
from stock_manager import app_stock_manager
from table_viewer import app_table_viewer
from config_viewer import app_config_viewer
from bulk_item_uploader import app_bulk_item_uploader
from log_viewer import app_log_viewer

STAGE = st.secrets["aws_credentials"]["STAGE"]

USERNAME = st.secrets["login_credentials"]["username"]
PASSWORD = st.secrets["login_credentials"]["password"]


def login() -> bool:
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if not st.session_state.logged_in:
        st.title("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            if username == USERNAME and password == PASSWORD:
                st.session_state.logged_in = True
                st.success("Logged in as {}".format(username))
                st.rerun()
            else:
                st.error("Incorrect username or password")

    return st.session_state.logged_in


if __name__ == "__main__":
    if login():
        st.sidebar.title("Navigation")
        app_mode = st.sidebar.selectbox(
            "Choose the app",
            (
                "Stock Manager",
                "Table Viewer",
                "Config Viewer",
                "Bulk Item Uploader",
                "Log Viewer",
            ),
        )

        if app_mode == "Stock Manager":
            app_stock_manager(STAGE)
        elif app_mode == "Table Viewer":
            app_table_viewer()
        elif app_mode == "Config Viewer":
            app_config_viewer()
        elif app_mode == "Bulk Item Uploader":
            app_bulk_item_uploader()
        elif app_mode == "Log Viewer":
            app_log_viewer()
