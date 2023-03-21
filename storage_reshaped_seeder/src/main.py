from time import sleep

import boto3
import os

delay = os.environ.get("DELAY_START_SECONDS", 240)
sleep(int(delay))

host = os.environ.get("HOST", "localhost")
port = os.environ.get("PORT", "8000")

dynamodb = boto3.client(
    'dynamodb',
    region_name='local',
    endpoint_url=f"http://{host}:{port}",
    aws_access_key_id="X",
    aws_secret_access_key="X"
)


def delete_table(table_name: str):
    dynamodb.delete_table(
        TableName=table_name
    )

def create_start_transaction_request():
    dynamodb.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'message_id',
                'AttributeType': 'S'
            }
        ],
        TableName="StartTransactionRequest",
        KeySchema=[
            {
                'AttributeName': 'message_id',
                'KeyType': 'HASH'
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )

def create_start_transaction_response():
    dynamodb.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'message_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'transaction_id',
                'AttributeType': 'N'
            }
        ],
        TableName="StartTransactionResponse",
        KeySchema=[
            {
                'AttributeName': 'message_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'transaction_id',
                'KeyType': 'RANGE'
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )

def create_meter_values_request():
    dynamodb.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'transaction_id',
                'AttributeType': 'N'
            }
        ],
        TableName="MeterValuesRequest",
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'transaction_id',
                'KeyType': 'RANGE'
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )


create_meter_values_request()
create_start_transaction_response()
create_start_transaction_request()