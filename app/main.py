import bulk_edits
import app.stock_manager_config as stock_manager_config
import log_viewer
import stock_manager
import streamlit as st
import table_viewer

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
                "Stock Manager Configuration",
                "Table Viewer",
                "Bulk Edits",
                "Log Viewer",
            ),
        )

        if app_mode == "Stock Manager":
            stock_manager.main()
        elif app_mode == "Stock Manager Configuration":
            stock_manager_config.main()
        elif app_mode == "Table Viewer":
            table_viewer.main()
        elif app_mode == "Bulk Edits":
            bulk_edits.main()
        elif app_mode == "Log Viewer":
            log_viewer.main()
