import json
from typing import Dict, List, Optional

import boto3


class SQSHandler:
    def __init__(self) -> None:
        """
        Initializes the SQSHandler by creating an SQS client using AWS credentials
        from environment variables.
        """
        self.sqs_client = None

    def delete_all_sqs_messages(self, queue_url: str) -> None:
        """
        Deletes all messages from the specified SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue from which to delete messages.
        """
        pass

    def get_all_sqs_messages(self, queue_url: str) -> List[Dict[str, Optional[str]]]:
        """
        Retrieves all messages from the specified SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue from which to retrieve messages.

        Returns:
            List[Dict[str, Optional[str]]]: A list of dictionaries containing message Id and message Body
        """
        queue_name = queue_url.split("/")[-1]
        file_path = f"mocks/sqs/{queue_name}/sqsmessage.json"
        with open(file_path) as f:
            messages = json.load(f)
        return messages
