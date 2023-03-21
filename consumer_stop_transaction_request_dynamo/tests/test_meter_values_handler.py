from datetime import timedelta
import random
from unittest.mock import Mock

import pandas as pd
from dateutil import parser
import uuid

from meter_values_handler import MeterValuesHandler


class ChargeKeeper:
    def __init__(self):
        self.cumulative_charge = 0

class TestMeterValuesHandler:

    def _add_noise(self, noise_range: float, base: float) -> float:
        noise = random.uniform(noise_range*-1, noise_range)
        return round(base + noise, 2)

    def generate_session(self, start_time: str, charge_point_id: str, charge_keeper: ChargeKeeper):
        current_time = parser.parse(start_time)
        charge_point_id = "123"
        collected_events = []
        for i in range(3):
            message_id = str(uuid.uuid4())
            power_import = self._add_noise(20.0, 1330.50)
            charge_keeper.cumulative_charge = charge_keeper.cumulative_charge + power_import
            local_time = current_time + timedelta(minutes=i)

            payload = {
                "message_id": message_id,
                "message_type": 2,
                "charge_point_id": charge_point_id,
                "action": "MeterValues",
                "write_timestamp": local_time.isoformat(),
                "write_timestamp_epoch": int(local_time.timestamp()),
                "body": {
                    "connector_id": 2,
                    "meter_value": [
                        {
                            "timestamp": local_time.isoformat(),
                            "sampled_value": [
                                {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage",
                                 "phase": "L1-N", "location": None, "unit": "V"},
                                {"value": "5.76", "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Current.Import", "phase": "L1", "location": None, "unit": "A"},
                                {"value": power_import, "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Power.Active.Import", "phase": "L1", "location": None, "unit": "W"},
                                {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage",
                                 "phase": "L2-N", "location": None, "unit": "V"},
                                {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Current.Import", "phase": "L2", "location": None, "unit": "A"},
                                {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Power.Active.Import", "phase": "L2", "location": None, "unit": "W"},
                                {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage",
                                 "phase": "L3-N", "location": None, "unit": "V"},
                                {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Current.Import", "phase": "L3", "location": None, "unit": "A"},
                                {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Power.Active.Import", "phase": "L3", "location": None, "unit": "W"},
                                {"value": power_import * 2, "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Energy.Active.Import.Register", "phase": None, "location": None,
                                 "unit": "Wh"},
                                {"value": "5.76", "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Current.Import", "phase": None, "location": None, "unit": "A"},
                                {"value": power_import, "context": "Sample.Periodic", "format": "Raw",
                                 "measurand": "Power.Active.Import", "phase": None, "location": None, "unit": "W"}
                            ]
                        }
                    ],
                    "transaction_id": 3
                }
            }
            collected_events.append(payload)
        return collected_events

    def test_meter_values(self):
        charge = ChargeKeeper()
        meter_values = self.generate_session(start_time="2023-01-01T09:00:00Z",
                                             charge_point_id="ee361d57-74cb-4f66-bda0-f5ff6eab50bf",
                                             charge_keeper=charge) + \
                       self.generate_session(start_time="2023-01-01T11:00:00Z",
                                             charge_point_id="8916d07e-e8e6-489c-b0c3-fb1ce057adc4",
                                             charge_keeper=charge) + \
                       self.generate_session(start_time="2023-01-01T10:00:00Z",
                                             charge_point_id="1149ae78-1dd0-4cfa-9bd1-87d8fadb1f75",
                                             charge_keeper=charge)
        # df = pd.DataFrame(meter_values)

        handler = MeterValuesHandler()
        print(meter_values)
        df = pd.DataFrame(handler._reshape(meter_values))
        result = handler._split_meter_values_to_charging_sessions(df)
        assert len(result) == 3
        assert result[0].index.tolist() == [0, 1, 2]
        assert result[0]["timestamp"].tolist() == [
            '2023-01-01T09:00:00+00:00',
            '2023-01-01T09:01:00+00:00',
            '2023-01-01T09:02:00+00:00'
        ]
        assert result[1].index.tolist() == [3, 4, 5]
        assert result[1]["timestamp"].tolist() ==[
            '2023-01-01T10:00:00+00:00',
            '2023-01-01T10:01:00+00:00',
            '2023-01-01T10:02:00+00:00'
        ]
        assert result[2].index.tolist() == [6, 7, 8]
        assert result[2]["timestamp"].tolist() == [
            '2023-01-01T11:00:00+00:00',
            '2023-01-01T11:01:00+00:00',
            '2023-01-01T11:02:00+00:00'
        ]

    def test__get_charging_time(self):
        charge = ChargeKeeper()
        meter_values = self.generate_session(start_time="2023-01-01T09:00:00Z",
                                             charge_point_id="ee361d57-74cb-4f66-bda0-f5ff6eab50bf",
                                             charge_keeper=charge)
        handler = MeterValuesHandler()
        df = pd.DataFrame(handler._reshape(meter_values))
        df["timestamp_epoch"] = df["timestamp"].map(lambda x: parser.parse(x).timestamp())
        df["timestamp_epoch"].astype(float)
        result = handler._get_charging_time_seconds(df)
        assert result == 120.0

    def test__flatten(self):
        message_id = "f05a2a74-682e-40ad-8e28-d842c27e35d5"
        power_import = 1330.50
        current_time = parser.parse("2023-01-01T09:00:00Z")
        charge_point_id = "123"

        payload = {
            "message_id": message_id,
            "message_type": 2,
            "charge_point_id": charge_point_id,
            "action": "MeterValues",
            "write_timestamp": current_time.isoformat(),
            "write_timestamp_epoch": int(current_time.timestamp()),
            "body": {
                "connector_id": 2,
                "meter_value": [
                    {
                        "timestamp": current_time.isoformat(),
                        "sampled_value": [
                            {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage",
                             "phase": "L1-N", "location": None, "unit": "V"},
                            {"value": "5.76", "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Current.Import", "phase": "L1", "location": None, "unit": "A"},
                            {"value": power_import, "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Power.Active.Import", "phase": "L1", "location": None, "unit": "W"},
                            {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage",
                             "phase": "L2-N", "location": None, "unit": "V"},
                            {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Current.Import", "phase": "L2", "location": None, "unit": "A"},
                            {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Power.Active.Import", "phase": "L2", "location": None, "unit": "W"},
                            {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage",
                             "phase": "L3-N", "location": None, "unit": "V"},
                            {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Current.Import", "phase": "L3", "location": None, "unit": "A"},
                            {"value": "0.0", "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Power.Active.Import", "phase": "L3", "location": None, "unit": "W"},
                            {"value": power_import*2, "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Energy.Active.Import.Register", "phase": None, "location": None,
                             "unit": "Wh"},
                            {"value": "5.76", "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Current.Import", "phase": None, "location": None, "unit": "A"},
                            {"value": power_import, "context": "Sample.Periodic", "format": "Raw",
                             "measurand": "Power.Active.Import", "phase": None, "location": None, "unit": "W"}
                        ]
                    }
                ],
                "transaction_id": 3
            }
        }

        handler = MeterValuesHandler()
        result = handler._flatten(payload)
        assert result.to_dict(orient="records") == [
            {
                'action': 'MeterValues',
              'charge_point_id': '123',
              'connector_id': 2,
              'context': 'Sample.Periodic',
              'format': 'Raw',
              'location': None,
              'measurand': 'Voltage',
              'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
              'message_type': 2,
              'phase': 'L1-N',
              'timestamp': '2023-01-01T09:00:00+00:00',
              'transaction_id': 3,
              'unit': 'V',
              'value': 0.0,
              'write_timestamp': '2023-01-01T09:00:00+00:00',
              'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Current.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L1',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'A',
                'value': 5.76,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Power.Active.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L1',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'W',
                'value': 1330.5,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Voltage',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L2-N',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'V',
                'value': 0.0,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Current.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L2',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'A',
                'value': 0.0,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Power.Active.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L2',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'W',
                'value': 0.0,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Voltage',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L3-N',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'V',
                'value': 0.0,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Current.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L3',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'A',
                'value': 0.0,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Power.Active.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': 'L3',
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'W',
                'value': 0.0,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Energy.Active.Import.Register',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': None,
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'Wh',
                'value': 2661.0,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Current.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': None,
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'A',
                'value': 5.76,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            },
            {
                'action': 'MeterValues',
                'charge_point_id': '123',
                'connector_id': 2,
                'context': 'Sample.Periodic',
                'format': 'Raw',
                'location': None,
                'measurand': 'Power.Active.Import',
                'message_id': 'f05a2a74-682e-40ad-8e28-d842c27e35d5',
                'message_type': 2,
                'phase': None,
                'timestamp': '2023-01-01T09:00:00+00:00',
                'transaction_id': 3,
                'unit': 'W',
                'value': 1330.5,
                'write_timestamp': '2023-01-01T09:00:00+00:00',
                'write_timestamp_epoch': 1672563600
            }
        ]