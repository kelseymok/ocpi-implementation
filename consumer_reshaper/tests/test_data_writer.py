import uuid
from unittest.mock import patch

import boto3
from botocore.stub import Stubber

from data_writer import DataWriter

def mock_uuid():
    return uuid.UUID("9fab60f5-d158-4dcf-9c1f-045103bf12dd")


class TestDataWriter:
    def test__start_transaction_request(self):
        client = boto3.client("dynamodb", region_name="eu-central-1")

        priority_fields = {
            "message_id": "df984612-05af-4278-9e8a-155d10a91fbd"
        }
        data = b'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJzb21lIjogeyJkYXRhIjogImZvb2JhciJ9fQ=='

        expected_request = {
            "TableName": "StartTransactionRequest",
            "Item": {
                "message_id": {
                    "S": str(priority_fields["message_id"]),
                },
                "body": {
                    "S": str(data),
                },
            }

        }

        response = {
            'ResponseMetadata': {
                "HTTPStatusCode": 200
            },
        }

        with Stubber(client) as stubber:
            stubber.add_response('put_item', response, expected_request)
            data_writer = DataWriter(client)
            result = data_writer._start_transaction_request(priority_fields, data)
            assert result == None

    def test__start_transaction_response(self):
        client = boto3.client("dynamodb", region_name="eu-central-1")

        priority_fields = {
            "message_id": "df984612-05af-4278-9e8a-155d10a91fbd",
            "transaction_id": 1
        }
        data = b'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJib2R5IjogeyJ0cmFuc2FjdGlvbl9pZCI6IDEsICJvdGhlciI6ICJkYXRhIn19'

        expected_request = {
            "TableName": "StartTransactionResponse",
            "Item": {
                "message_id": {
                    "S": str(priority_fields["message_id"]),
                },
                "transaction_id": {
                    "N": str(priority_fields["transaction_id"])
                },
                "body": {
                    "S": str(data),
                },
            }

        }

        response = {
            'ResponseMetadata': {
                "HTTPStatusCode": 200
            },
        }

        with Stubber(client) as stubber:
            stubber.add_response('put_item', response, expected_request)
            data_writer = DataWriter(client)
            result = data_writer._start_transaction_response(priority_fields, data)
            assert result == None

    @patch("uuid.uuid4", mock_uuid)
    def test__meter_values_request(self):
        client = boto3.client("dynamodb", region_name="eu-central-1")

        priority_fields = {
            "transaction_id": 1
        }

        data = b'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJib2R5IjogeyJ0cmFuc2FjdGlvbl9pZCI6IDEsICJvdGhlciI6ICJkYXRhIn19'

        expected_request = {
                "TableName": "MeterValuesRequest",
                "Item": {
                    "id": {
                        "S": str("9fab60f5-d158-4dcf-9c1f-045103bf12dd")
                    },
                    "transaction_id": {
                        "N": str(priority_fields["transaction_id"])
                    },
                    "body": {
                        "S": str(data),
                    },
                }

            }

        response = {
            'ResponseMetadata': {
                "HTTPStatusCode": 200
            },
        }

        with Stubber(client) as stubber:
            stubber.add_response('put_item', response, expected_request)
            data_writer = DataWriter(client)
            result = data_writer._meter_values_request(priority_fields, data)
            assert result == None

