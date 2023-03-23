import base64
import json
from typing import Dict, List, Union

import boto3


class DataReader:
    def __init__(self, storage_client: boto3.client):
        self.storage_client = storage_client

    def _decode(self, data: bytes):
        return json.loads(base64.b64decode(data).decode("utf-8"))

    def get_all(self, table_name: str) -> Union[Dict, List]:
        response = self.storage_client.scan(
            TableName=table_name,
        )
        print(response)
        items = [self._decode(x["payload"]["B"]) for x in response["Items"]]
        return items
