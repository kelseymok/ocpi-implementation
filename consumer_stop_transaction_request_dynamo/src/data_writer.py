from dataclasses import asdict

from ocpi.v221.types import CDR
import base64
import json
from dateutil import parser


class DataWriter:
    def __init__(self, client):
        self.client = client
        self.table_name = "CDRs"

    def write(self, cdr_object: CDR):
        encoded_body = base64.b64encode(json.dumps(asdict(cdr_object)).encode("utf-8"))

        item = {
            "id": {
                "S": str(cdr_object.id),
            },
            "lastUpdatedEpoch": {
                "N": str(round(parser.parse(cdr_object.last_updated).timestamp())),
            },
            "body": {
                "B": encoded_body
            }
        }
        self.client.put_item(TableName=self.table_name, Item=item)