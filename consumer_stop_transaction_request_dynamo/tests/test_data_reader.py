import boto3
from botocore.stub import Stubber

from data_reader import DataReader

class TestDataWriter:
    def test_get_start_transaction_request(self):
        client = boto3.client("dynamodb", region_name="eu-central-1")

        expected_request_start_tx_response = {
            'ConsistentRead': True,
            'ExpressionAttributeValues': {':transaction_id': {'N': str(1)}},
            'KeyConditionExpression': 'transaction_id = :transaction_id',
            'Limit': 1,
            'ProjectionExpression': 'message_id',
            'ScanIndexForward': False,
            'Select': 'SPECIFIC_ATTRIBUTES',
            'TableName': 'StartTransactionResponse'
        }

        response_start_tx_response = {
            'Items': [{
                "transaction_id": {
                    "N": str(1)
                },
                "message_id": {
                    "S": "239f96fd-bb1c-482b-9547-d5d75ad1be08"
                },
                "body": {
                    "B": 'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJib2R5IjogeyJ0cmFuc2FjdGlvbl9pZCI6IDEsICJvdGhlciI6ICJkYXRhIn19'
                }
            }],
            'Count': 1
        }

        expected_request_start_tx_request = {
            'ConsistentRead': True,
            'ExpressionAttributeValues': {':message_id': {'S': "239f96fd-bb1c-482b-9547-d5d75ad1be08"}},
            'KeyConditionExpression': 'message_id = :message_id',
            'Limit': 1,
            'ScanIndexForward': False,
            'Select': 'ALL_ATTRIBUTES',
            'TableName': 'StartTransactionRequest'
        }

        response_start_tx_request = {
            'Items': [{
                "transaction_id": {
                    "N": str(1)
                },
                "id": {
                    "S": "e879f4c3-d01c-4905-b8e3-8d9def447223"
                },
                "body": {
                    "B": 'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJzb21lIjogeyJkYXRhIjogImZvb2JhciJ9fQ=='
                }
            }],
            'Count': 1
        }

        with Stubber(client) as stubber:
            stubber.add_response('query', response_start_tx_response, expected_request_start_tx_response)
            stubber.add_response('query', response_start_tx_request, expected_request_start_tx_request)
            data_reader = DataReader(client)
            result = data_reader.get_start_transaction_request(transaction_id=1)
            assert result == {
                'body': {
                    'body': {
                        'other': 'data',
                        'transaction_id': 1
                    },
                'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd'},
                'transaction_id': '1'
            }
