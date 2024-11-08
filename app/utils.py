from aws_utils import iam
import os

iam.get_aws_credentials(os.environ)

PROJECT_BUCKET_NAME = f"rtg-automotive-bucket-{os.environ['AWS_ACCOUNT_ID']}"
