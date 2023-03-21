from typing import Dict

import pandas as pd
import json


class Flattener:

    def _convert_body_to_dict(self, x: Dict):
        x["body"] = json.loads(x["body"])
        return x

    def normalizers(self):
        return {
            "HeartbeatRequest": self._heartbeat_request_normalizer,
            "BootNotificationRequest": self._boot_notification_request_normalizer,
            "StartTransactionRequest": self._start_transaction_request_normalizer,
            "MeterValuesRequest": self._meter_values_request_normalizer,
            "StopTransactionRequest": self._stop_transaction_request_normalizer,
            "HeartbeatResponse": self._heartbeat_response_normalizer,
            "BootNotificationResponse": self._boot_notification_response_normalizer,
            "StartTransactionResponse": self._start_transaction_response_normalizer,
            "MeterValuesResponse": self._meter_values_response_normalizer,
            "StopTransactionResponse": self._stop_transaction_response_normalizer
        }

    def _message_type_mapping(self):
        return {
            2: "Request",
            3: "Response"
        }

    def process(self, payload: str):
        data = json.loads(payload)
        t = f"{data['action']}{self._message_type_mapping()[data['message_type']]}"
        normalizer = self.normalizers()[t]
        df = normalizer(data)
        result = df.to_dict(orient="records")
        return result

    def _heartbeat_request_normalizer(self, data):
        df = pd.json_normalize(data)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df

    def _heartbeat_response_normalizer(self, data):
        df = pd.json_normalize(data)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df

    def _boot_notification_request_normalizer(self, data):
        df = pd.json_normalize(data)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df

    def _boot_notification_response_normalizer(self,data):
        df = pd.json_normalize(data)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df

    def _start_transaction_request_normalizer(self, data):
        df = pd.json_normalize(
            data,
        ).rename(columns={
            "body.connector_id": "connector_id",
            "body.id_tag": "id_tag",
            "body.meter_start": "meter_start",
            "body.timestamp": "timestamp",
        })
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df


    def _start_transaction_response_normalizer(self, data):
        df = pd.json_normalize(data)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df

    def _meter_values_request_normalizer(self, data):
        df = pd.json_normalize(
            data,
            record_path=['body', 'meter_value', 'sampled_value'],
            meta=[
                'message_id',
                'message_type',
                'charge_point_id',
                'action',
                'write_timestamp',
                'write_timestamp_epoch',
                ['body', 'connector_id'], ['body', 'transaction_id'],
                ['body', 'meter_value', 'timestamp']
            ]
        ).rename(columns={
            "body.connector_id": "connector_id",
            "body.meter_value.timestamp": "timestamp",
            "body.transaction_id": "transaction_id"}
        )
        df["value"] = df["value"].astype(float)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df

    def _meter_values_response_normalizer(self, data):
        df = pd.json_normalize(data)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df

    def _stop_transaction_request_normalizer(self, data):
        df = pd.json_normalize(
            data,
        ).rename(columns={
            "body.connector_id": "connector_id",
            "body.id_tag": "id_tag",
            "body.meter_stop": "meter_stop",
            "body.transaction_id": "transaction_id",
            "body.reason": "reason",
            "body.timestamp": "timestamp",
        })
        df.columns = [c.replace(".", "_").lower() for c in df.columns]

        return df

    def _stop_transaction_response_normalizer(self, data):
        df = pd.json_normalize(data)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df


