import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict


class DataWriter:
    def __init__(self, client):
        self.client = client
        self.table_name = "CDRs"

    def write(self, priority_fields: Dict, metadata: Dict, data: bytes):
        metadata_mapper = {
            "StartTransactionRequest": self._start_transaction_request,
            "StartTransactionResponse": self._start_transaction_response,
            "MeterValuesRequest": self._meter_values_request,
        }
        writer = metadata_mapper.get(metadata["type"], lambda x: x)
        writer(priority_fields, data)

    def _ttl(self) -> int:
        seconds_in_one_day = 86400
        seconds_in_60_days = seconds_in_one_day*60
        return int((datetime.now(timezone.utc) + timedelta(seconds=seconds_in_60_days)).timestamp())

    def _start_transaction_request(self, priority_fields: Dict, data: bytes):
        item = {
            "message_id": {
                "S": str(priority_fields["message_id"]),
            },
            "payload": {
                "B": data,
            },
            "ttl_expiration": {
                "N": str(self._ttl())
            }
        }
        self.client.put_item(TableName="StartTransactionRequest", Item=item)

    def _start_transaction_response(self, priority_fields: Dict, data: bytes):
        item = {
            "message_id": {
                "S": str(priority_fields["message_id"]),
            },
            "transaction_id": {
                "N": str(priority_fields["transaction_id"]),
            },
            "payload": {
                "B": data,
            },
            "ttl_expiration": {
                "N": str(self._ttl())
            }
        }
        self.client.put_item(TableName="StartTransactionResponse", Item=item)

    def _meter_values_request(self, priority_fields: Dict, data: bytes):
        item = {
            "id": {
                "S": str(uuid.uuid4())
            },
            "transaction_id": {
                "N": str(priority_fields["transaction_id"]),
            },
            "payload": {
                "B": data,
            },
            "ttl_expiration": {
                "N": str(self._ttl())
            }
        }
        self.client.put_item(TableName="MeterValuesRequest", Item=item)

