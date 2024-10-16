import streamlit as st
import pandas as pd
from io import BytesIO
import os
from aws_utils import logs, s3


def get_expected_schema():
    return [
        {"Name": "item_id", "Type": "bigint", "Display Type": "integer"},
        {"Name": "custom_label", "Type": "string", "Display Type": "text"},
        {"Name": "title", "Type": "string", "Display Type": "text"},
        {"Name": "current_price", "Type": "double", "Display Type": "decimal number"},
        {"Name": "prefix", "Type": "string or null", "Display Type": "text"},
        {"Name": "uk_rtg", "Type": "string", "Display Type": "text"},
        {"Name": "fps_wds_dir", "Type": "string", "Display Type": "text"},
        {"Name": "payment_profile_name", "Type": "string", "Display Type": "text"},
        {"Name": "shipping_profile_name", "Type": "string", "Display Type": "text"},
        {"Name": "return_profile_name", "Type": "string", "Display Type": "text"},
        {"Name": "supplier", "Type": "string", "Display Type": "text"},
        {"Name": "ebay_store", "Type": "string", "Display Type": "text"},
    ]


def display_expected_schema(expected_schema):
    with st.expander("Expected Schema", expanded=False):
        st.dataframe(pd.DataFrame(expected_schema).T, use_container_width=True)


def validate_schema(df, expected_schema):
    errors = []
    columns = [column["Name"] for column in expected_schema]
    df = df.copy()[columns]
    for column in expected_schema:
        col_name = column["Name"]
        if col_name not in df.columns:
            errors.append(f"Missing column: {col_name}")
        else:
            errors.extend(check_data_type(df[col_name], column))
    return errors, df


def check_data_type(column_data, column):
    errors = []
    if column["Type"] == "bigint" and not pd.api.types.is_integer_dtype(column_data):
        errors.append(f"Column '{column['Name']}' should be of type bigint.")
    elif column["Type"] == "double" and not pd.api.types.is_float_dtype(column_data):
        errors.append(f"Column '{column['Name']}' should be of type double.")
    elif column["Type"] == "string" and not pd.api.types.is_string_dtype(column_data):
        errors.append(f"Column '{column['Name']}' should be of type string.")
    elif column["Type"] == "string or null" and not (
        pd.api.types.is_string_dtype(column_data) or column_data.isnull().all()
    ):
        errors.append(f"Column '{column['Name']}' should be of type string or null.")
    return errors


def display_validation_results(errors, df):
    if errors:
        st.error("Validation Errors:")
        for error in errors:
            st.write(error)
    else:
        st.success("CSV matches the expected schema!")
        st.dataframe(df)


def update_database(df, aws_account_id):
    suppliers = df.groupby(["supplier", "ebay_store"])
    bucket_name = f"rtg-automotive-bucket-{aws_account_id}"

    st.write(f"Uploading {len(df)} items to the database...")

    s3_handler = s3.S3Handler(
        os.environ["AWS_ACCESS_KEY_ID"],
        os.environ["AWS_SECRET_ACCESS_KEY"],
        os.environ["AWS_SESSION_TOKEN"],
        "eu-west-2",
    )

    for (supplier, ebay_store), group in suppliers:
        file_path = create_file_path(ebay_store, supplier)
        try:
            combined_data = handle_existing_file(
                s3_handler, bucket_name, file_path, group
            )
            if combined_data is not None:
                upload_data(s3_handler, bucket_name, file_path, combined_data)
                st.success(
                    f"Uploaded {len(group)} items for supplier: {supplier} ebay_store: {ebay_store}"
                )

        except Exception as e:
            handle_file_not_found(s3_handler, e, bucket_name, file_path, group)


def create_file_path(ebay_store, supplier):
    return f"store/ebay_store={ebay_store}/supplier={supplier}/data.parquet"


def handle_existing_file(s3_handler, bucket_name, file_path, group):
    parquet_data = s3_handler.load_parquet_from_s3(bucket_name, file_path)
    existing_data = pd.read_parquet(BytesIO(parquet_data))
    combined_data = pd.concat([existing_data, group])

    if combined_data["item_id"].duplicated().any():
        st.error(
            "Duplicate item_ids found. Please resolve duplicates before uploading."
        )
        return None

    return combined_data


def upload_data(s3_handler, bucket_name, file_path, combined_data):
    parquet_buffer = BytesIO()
    combined_data.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    s3_handler.upload_parquet_to_s3(bucket_name, file_path, parquet_buffer.getvalue())
    logs_handler = logs.LogsHandler()
    logs_handler.log_action(
        bucket_name, "frontend", f"BULK_ITEM_UPLOADED | file_path={file_path}", "admin"
    )


def handle_file_not_found(s3_handler, e, bucket_name, file_path, group):
    if e.response["Error"]["Code"] == "404":
        parquet_buffer = BytesIO()
        group.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        s3_handler.upload_parquet_to_s3(
            bucket_name, file_path, parquet_buffer.getvalue()
        )
    else:
        st.error(f"An error occurred while accessing S3: {e}")


def app_bulk_item_uploader(aws_account_id):
    st.title("Bulk Item Uploader")
    expected_schema = get_expected_schema()
    display_expected_schema(expected_schema)

    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        errors, df = validate_schema(df, expected_schema)
        display_validation_results(errors, df)

        if not errors and st.button("Upload to Database"):
            update_database(df, aws_account_id)
