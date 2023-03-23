import base64
import json
import uuid
from datetime import datetime, timezone
from time import sleep

from dateutil import parser
import boto3
import os

delay = os.environ.get("DELAY_START_SECONDS", 240)
sleep(int(delay))

dynamodb = boto3.client('dynamodb', region_name='local', endpoint_url="http://ocpi-storage:8000", aws_access_key_id="X",
    aws_secret_access_key="X")


def delete_table(table_name: str):
    response = dynamodb.delete_table(
        TableName=table_name
    )
def create_table(table_name: str):
    dynamodb.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'lastUpdatedEpoch',
                'AttributeType': 'N'
            },
        ],
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'lastUpdatedEpoch',
                'KeyType': 'RANGE'
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )

def datetime_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def create_tariff():
    id = uuid.uuid4()
    tariff = {
        "country_code": "DE",
        "party_id": "ALL",
        "id": str(id),
        "currency": "EUR",
        "elements": [{
            "price_components": [
                {
                    "type": "FLAT",
                    "price": 0.00,
                    "step_size": 0
                }
            ]
        }],
        "last_updated": datetime_now()
    }

    encoded_body = base64.b64encode(json.dumps(tariff).encode("utf-8"))

    item = {
        "id": {
            "S": str(id),
        },
        "lastUpdatedEpoch": {
            "N": str(round(parser.parse(tariff["last_updated"]).timestamp())),
        },
        "payload": {
            "B": encoded_body
        }
    }
    dynamodb.put_item(TableName="Tariffs", Item=item)

def create_location():
    id = uuid.uuid4()
    location = {
        "country_code": "BE",
        "party_id": "BEC",
        "id": str(id),
        "publish": True,
        "name": "Gent Zuid",
        "address": "F.Rooseveltlaan 3A",
        "city": "Gent",
        "postal_code": "9000",
        "country": "BEL",
        "coordinates": {
        "latitude": "51.047599",
        "longitude": "3.729944"
        },
        "parking_type": "ON_STREET",
            "evses": [
                {
                    "uid": "3256",
                    "evse_id": "BE*BEC*E041503001",
                    "status": "AVAILABLE",
                    "capabilities": [
                        "RESERVABLE"
                    ],
                    "connectors": [
                {
                    "id": "1",
                    "standard": "IEC_62196_T2",
                    "format": "CABLE",
                    "power_type": "AC_3_PHASE",
                    "max_voltage": 220,
                    "max_amperage": 16,
                    "tariff_ids": ["11"],
                    "last_updated": "2015-03-16T10:10:02Z"
                },
                {
                    "id": "2",
                    "standard": "IEC_62196_T2",
                    "format": "SOCKET",
                    "power_type": "AC_3_PHASE",
                    "max_voltage": 220,
                    "max_amperage": 16,
                    "tariff_ids": ["13"],
                    "last_updated": "2015-03-18T08:12:01Z"
                }
            ],
                    "physical_reference": "1",
                    "floor_level": "-1",
                    "last_updated": "2015-06-28T08:12:01Z"
                },
                {
                    "uid": "3257",
                    "evse_id": "BE*BEC*E041503002",
                    "status": "RESERVED",
                    "capabilities": [
                      "RESERVABLE"
                    ],
                    "connectors": [
                        {
                            "id": "1",
                            "standard": "IEC_62196_T2",
                            "format": "SOCKET",
                            "power_type": "AC_3_PHASE",
                            "max_voltage": 220,
                            "max_amperage": 16,
                            "tariff_ids": ["12"],
                            "last_updated": "2015-06-29T20:39:09Z"
                        }
                    ],
                    "physical_reference": "2",
                    "floor_level": "-2",
                    "last_updated": "2015-06-29T20:39:09Z"
                }
            ],
            "operator": {
                "name": "BeCharged"
            },
            "time_zone": "Europe/Brussels",
            "last_updated": datetime_now()
        }
    encoded_body = base64.b64encode(json.dumps(location).encode("utf-8"))

    item = {
        "id": {
            "S": str(id),
        },
        "lastUpdatedEpoch": {
            "N": str(round(parser.parse(location["last_updated"]).timestamp())),
        },
        "payload": {
            "B": encoded_body
        }
    }
    dynamodb.put_item(TableName="Locations", Item=item)


def scan(table_name: str):
    response = dynamodb.scan(
        TableName=table_name,
    )
    return response


response = dynamodb.list_tables()
print(response)
# delete_table("Tariffs")
# delete_table("Locations")
create_table("Tariffs")
create_table("Locations")
create_table("CDRs")
create_tariff()
create_location()
response = dynamodb.list_tables()
print(response)
# results = scan("Locations")
# print(results)
# body_binary = results["Items"][0]["body"]["B"]
# body = json.loads(base64.b64decode(body_binary).decode("utf-8"))
# print(body)