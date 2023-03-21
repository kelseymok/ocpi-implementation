import base64
from typing import Dict, Tuple

import json


class Transformer:

    def _convert_body_to_dict(self, x: Dict):
        x["body"] = json.loads(x["body"])
        return x

    def _encode(self, data: Dict) -> bytes:
        return base64.b64encode(json.dumps(data).encode("utf-8"))

    def reshapers(self):
        return {
            "StartTransactionRequest": self._start_transaction_request_reshaper,
            "StartTransactionResponse": self._start_transaction_response_reshaper,
            "MeterValuesRequest": self._meter_values_request_reshaper,
        }

    def _message_type_mapping(self):
        return {
            2: "Request",
            3: "Response"
        }

    def process(self, payload: str):
        data = json.loads(payload)
        t = f"{data['action']}{self._message_type_mapping()[data['message_type']]}"
        normalizer = self.reshapers()[t]
        result = normalizer(data)
        return result

    def _start_transaction_request_reshaper(self, data: Dict) -> Tuple[Dict, Dict, bytes]:
        priority_fields = {
            "message_id": data["message_id"]
        }

        metadata = {
            "type": "StartTransactionRequest"
        }

        return priority_fields, metadata, self._encode(data)

    def _start_transaction_response_reshaper(self, data: Dict) -> Tuple[Dict, Dict, bytes]:
        priority_fields = {
            "message_id": data["message_id"],
            "transaction_id": data["body"]["transaction_id"]
        }

        metadata = {
            "type": "StartTransactionResponse"
        }

        return priority_fields, metadata, self._encode(data)

    def _meter_values_request_reshaper(self, data: Dict) -> Tuple[Dict, Dict, bytes]:
        priority_fields = {
            "transaction_id": data["body"]["transaction_id"]
        }

        metadata = {
            "type": "MeterValuesRequest"
        }

        return priority_fields, metadata, self._encode(data)