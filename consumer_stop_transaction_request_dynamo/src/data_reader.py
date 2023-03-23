import base64
import json
from typing import Dict, List

from pandas import DataFrame
import pandas as pd


class DataReader:
    def __init__(self, client):
        self.client = client

    def _unpack_payload(self, body: Dict) -> Dict:
        return json.loads(base64.b64decode(body["payload"]["B"]).decode("utf-8"))

    def get_start_transaction_request(self, transaction_id):
        # command = f"select * from starttransactionrequest where messageid = (select messageid from starttransactionresponse where bodytransactionid = {transaction_id} limit 1)"
        start_transaction_response_response = self.client.query(
            TableName="StartTransactionResponse",
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='message_id',
            Limit=1,
            ConsistentRead=True,
            ScanIndexForward=False,
            KeyConditionExpression="transaction_id = :transaction_id",
            ExpressionAttributeValues={
                ':transaction_id': {
                    'N': str(transaction_id),
                }
            }
        )
        print(f"start_transaction_response_response: {start_transaction_response_response}")
        start_tx_response_tx_message_id = start_transaction_response_response["Items"][0]["message_id"]["S"] if start_transaction_response_response["Count"] > 0 else None
        start_transaction_request_response = self.client.query(
            TableName="StartTransactionRequest",
            Select='ALL_ATTRIBUTES',
            Limit=1,
            ConsistentRead=True,
            ScanIndexForward=False,
            KeyConditionExpression="message_id = :message_id",
            ExpressionAttributeValues={
                ':message_id': {
                    'S': str(start_tx_response_tx_message_id),
                }
            }
        )

        print(f"start_transaction_request_response: {start_transaction_request_response}")



        result = self._unpack_payload(start_transaction_request_response["Items"][0]) if start_transaction_request_response["Count"] > 0 else None
        return result


    def get_charging_sessions(self, transaction_id: int) -> List[Dict]:
        response = self.client.query(
            TableName="MeterValuesRequest",
            Select='ALL_ATTRIBUTES',
            ConsistentRead=True,
            ScanIndexForward=False,
            KeyConditionExpression="transaction_id = :transaction_id",
            ExpressionAttributeValues={
                ':transaction_id': {
                    'N': str(transaction_id),
                }
            }
        )
        print(f"meter values result: {response}")

        result = [self._unpack_payload(x) for x in response["Items"]] if response["Count"] > 0 else []
        return result




