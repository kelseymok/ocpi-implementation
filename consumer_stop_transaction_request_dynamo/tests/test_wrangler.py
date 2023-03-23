import uuid
from dataclasses import asdict
from unittest.mock import Mock, patch

import pandas as pd
from dateutil import parser
from datetime import timedelta
import random

from freezegun import freeze_time

from data_reader import DataReader
from data_writer import DataWriter
from meter_values_handler import MeterValuesHandler
from wrangler import Wrangler
import json



class ChargeKeeper:
    def __init__(self):
        self.cumulative_charge = 0

def mock_uuid():
    return uuid.UUID('df984612-05af-4278-9e8a-155d10a91fbd')

class TestWrangler:

    def _add_noise(self, noise_range: float, base: float) -> float:
        noise = random.uniform(noise_range*-1, noise_range)
        return round(base + noise, 2)

    def generate_session(self, start_time: str, charge_point_id: str, charge_keeper: ChargeKeeper):
        timestamp = parser.parse(start_time)
        collected_events = []
        for i in range(3):
            message_id = str(uuid.uuid4())
            power_import = self._add_noise(20.0, 1330.50)
            charge_keeper.cumulative_charge = charge_keeper.cumulative_charge + power_import
            current_time = timestamp + timedelta(minutes=i)

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
                             {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L1-N", "location": None, "unit": "V"},
                             {"value": "5.76", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L1", "location": None, "unit": "A"},
                             {"value": power_import, "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L1", "location": None, "unit": "W"},
                             {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L2-N", "location": None, "unit": "V"},
                             {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L2", "location": None, "unit": "A"},
                             {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L2", "location": None, "unit": "W"},
                             {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L3-N", "location": None, "unit": "V"},
                             {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L3", "location": None, "unit": "A"},
                             {"value": "0.0", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L3", "location": None, "unit": "W"},
                             {"value": charge_keeper.cumulative_charge, "context": "Sample.Periodic", "format": "Raw", "measurand": "Energy.Active.Import.Register", "phase": None, "location": None, "unit": "Wh"},
                             {"value": "5.76", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": None, "location": None, "unit": "A"},
                             {"value": power_import, "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": None, "location": None, "unit": "W"}
                         ]
                     }
                ],
                "transaction_id": 3
                }
            }

            collected_events.append(payload)


        return collected_events


    @freeze_time("2023-01-01T09:00:00Z")
    @patch('uuid.uuid4', mock_uuid)
    def test_process(self):
        random.seed(10)
        data_reader = Mock()
        data_reader.get_start_transaction_request = Mock(return_value={
            'message_id': '3513f59b-b11e-4d72-8809-bfb810e8286d',
            'message_type': 2,
            'charge_point_id': '8186ad01-70b6-4f7d-9220-7b02e4fd8177',
            'action': 'StartTransaction',
            'write_timestamp': '2023-01-01T11:06:42.067257+00:00',
            'body': {
                'connector_id': 3,
                'id_tag': '1c69b06e-376d-446f-8d51-e33f3d053a60',
                'meter_start': 1000,
                'timestamp': '2023-01-01T09:00:00+00:00',
                'reservation_id': None
            },
            'write_timestamp_epoch': 1672571202067
        })



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
        data_reader.get_charging_sessions = Mock(return_value=meter_values)

        handler = Mock()
        handler_result = [
            pd.DataFrame([{'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': 'ee361d57-74cb-4f66-bda0-f5ff6eab50bf', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T09:00:00+00:00', 'write_timestamp_epoch': 1672563600, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T09:00:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1333.36, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 1333.36, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1333.36, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T09:00:00+00:00', 'timestamp_epoch': 1672563600.0}, {'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': 'ee361d57-74cb-4f66-bda0-f5ff6eab50bf', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T09:01:00+00:00', 'write_timestamp_epoch': 1672563660, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T09:01:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1327.66, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 2661.02, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1327.66, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T09:01:00+00:00', 'timestamp_epoch': 1672563660.0}, {'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': 'ee361d57-74cb-4f66-bda0-f5ff6eab50bf', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T09:02:00+00:00', 'write_timestamp_epoch': 1672563720, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T09:02:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1333.62, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 3994.64, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1333.62, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T09:02:00+00:00', 'timestamp_epoch': 1672563720.0}]),
            pd.DataFrame([{'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': '1149ae78-1dd0-4cfa-9bd1-87d8fadb1f75', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T10:00:00+00:00', 'write_timestamp_epoch': 1672567200, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T10:00:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1336.64, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 9336.49, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1336.64, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T10:00:00+00:00', 'timestamp_epoch': 1672567200.0}, {'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': '1149ae78-1dd0-4cfa-9bd1-87d8fadb1f75', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T10:01:00+00:00', 'write_timestamp_epoch': 1672567260, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T10:01:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1316.91, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 10653.4, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1316.91, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T10:01:00+00:00', 'timestamp_epoch': 1672567260.0}, {'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': '1149ae78-1dd0-4cfa-9bd1-87d8fadb1f75', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T10:02:00+00:00', 'write_timestamp_epoch': 1672567320, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T10:02:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1331.33, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 11984.73, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1331.33, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T10:02:00+00:00', 'timestamp_epoch': 1672567320.0}]),
            pd.DataFrame([{'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': '8916d07e-e8e6-489c-b0c3-fb1ce057adc4', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T11:00:00+00:00', 'write_timestamp_epoch': 1672570800, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T11:00:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1318.74, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 5313.38, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1318.74, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T11:00:00+00:00', 'timestamp_epoch': 1672570800.0}, {'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': '8916d07e-e8e6-489c-b0c3-fb1ce057adc4', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T11:01:00+00:00', 'write_timestamp_epoch': 1672570860, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T11:01:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1343.03, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 6656.41, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1343.03, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T11:01:00+00:00', 'timestamp_epoch': 1672570860.0}, {'message_id': 'df984612-05af-4278-9e8a-155d10a91fbd', 'message_type': 2, 'charge_point_id': '8916d07e-e8e6-489c-b0c3-fb1ce057adc4', 'action': 'MeterValues', 'write_timestamp': '2023-01-01T11:02:00+00:00', 'write_timestamp_epoch': 1672570920, 'body': {'connector_id': 2, 'meter_value': [{'timestamp': '2023-01-01T11:02:00+00:00', 'sampled_value': [{'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'}, {'value': 1343.44, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'}, {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'}, {'value': 7999.85, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'}, {'value': '5.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'}, {'value': 1343.44, 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}]}], 'transaction_id': 3}, 'timestamp': '2023-01-01T11:02:00+00:00', 'timestamp_epoch': 1672570920.0}])
        ]
        handler.handle.return_value = [pd.DataFrame(x) for x in handler_result]
        wrangler = Wrangler(data_reader=data_reader, meter_values_handler=handler)
        data = {
            "body": {
                "meter_stop": charge.cumulative_charge,
                "transaction_id": 6,
                "timestamp": "2023-01-01T11:30:00Z",
                "id_tag": "124eabad-9506-4d47-b2bf-aaef8e595a06"
            }
        }
        result = wrangler.process(data)
        json_result = json.dumps(asdict(result))
        assert json_result == '{"country_code": "BE", "party_id": "BEC", "id": "df984612-05af-4278-9e8a-155d10a91fbd", "start_date_time": "2023-01-01T09:00:00+00:00", "end_date_time": "2023-01-01T11:30:00Z", "cdr_token": {"uid": "124eabad-9506-4d47-b2bf-aaef8e595a06", "type": "RFID", "contract_id": "df984612-05af-4278-9e8a-155d10a91fbd"}, "auth_method": "AUTH_REQUEST", "last_updated": "2023-01-01T09:00:00Z", "cdr_location": {"id": "df984612-05af-4278-9e8a-155d10a91fbd", "address": "F.Rooseveltlaan 3A", "city": "Gent", "postal_code": "9000", "country": "BEL", "evse_uid": "3256", "evse_id": "BE*BEC*E041503003", "connector_id": "1", "coordinates": {"latitude": "3.729944", "longitude": "51.047599"}, "connector_standard": "IEC_62196_T2", "connector_format": "SOCKET", "connector_power_type": "AC_1_PHASE", "name": "Gent Zuid"}, "currency": "EUR", "total_cost": {"excl_vat": 123, "incl_vat": null}, "total_energy": 10984, "total_time": 2.5, "tariffs": [{"country_code": "BE", "party_id": "BEC", "id": "12", "currency": "EUR", "last_updated": "2023-01-01T09:00:00Z", "type": null, "tariff_alt_text": [], "tariff_alt_url": null, "min_price": null, "max_price": null, "elements": [{"price_components": {"type": "TIME", "price": 2.0, "step_size": 300, "vat": 10.0}, "restrictions": null}], "start_date_time": null, "end_date_time": null, "energy_mix": null}], "charging_periods": [{"start_date_time": "2023-01-01T09:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}, {"start_date_time": "2023-01-01T10:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}, {"start_date_time": "2023-01-01T11:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}], "session_id": null, "authorization_reference": null, "meter_id": null, "signed_data": null, "total_fixed_cost": null, "total_energy_cost": null, "total_time_cost": null, "total_parking_time": 2.4, "total_parking_cost": null, "total_reservation_cost": null, "remark": null, "invoice_reference_id": null, "credit": null, "credit_reference_id": null}'
        assert json.loads(json_result) == {
            'country_code': 'BE',
            'party_id': 'BEC',
            'id': 'df984612-05af-4278-9e8a-155d10a91fbd',
            'start_date_time': '2023-01-01T09:00:00+00:00',
            'end_date_time': '2023-01-01T11:30:00Z',
            'cdr_token': {
                'uid': '124eabad-9506-4d47-b2bf-aaef8e595a06',
                'type': 'RFID',
                'contract_id': 'df984612-05af-4278-9e8a-155d10a91fbd'
            },
            'auth_method': 'AUTH_REQUEST',
            'last_updated': '2023-01-01T09:00:00Z',
            'cdr_location': {
                'id': 'df984612-05af-4278-9e8a-155d10a91fbd',
                'address': 'F.Rooseveltlaan 3A',
                'city': 'Gent',
                'postal_code': '9000',
                'country': 'BEL',
                'evse_uid': '3256',
                'evse_id': 'BE*BEC*E041503003',
                'connector_id': '1',
                'coordinates': {
                    'latitude': '3.729944',
                    'longitude': '51.047599'
                },
                'connector_standard': 'IEC_62196_T2',
                'connector_format': 'SOCKET',
                'connector_power_type': 'AC_1_PHASE',
                'name': 'Gent Zuid'
            },
            'currency': 'EUR',
            'total_cost': {
                'excl_vat': 123,
                'incl_vat': None
            },
            'total_energy': 10984,
            'total_time': 2.5,
            'tariffs': [
                {
                    'country_code': 'BE',
                    'party_id': 'BEC',
                    'id': '12',
                    'currency': 'EUR',
                    'last_updated': '2023-01-01T09:00:00Z',
                    'type': None,
                    'tariff_alt_text': [],
                    'tariff_alt_url': None,
                    'min_price': None,
                    'max_price': None,
                    'elements': [
                        {
                            'price_components': {
                                'type': 'TIME',
                                'price': 2.0,
                                'step_size': 300,
                                'vat': 10.0
                            },
                            'restrictions': None
                        }
                    ],
                    'start_date_time': None,
                    'end_date_time': None,
                    'energy_mix': None
                }
            ],
            'charging_periods': [
                {
                    'start_date_time': '2023-01-01T09:00:00Z',
                    'dimensions': [
                        {'type': 'TIME', 'volume': 0.03333333333333333}
                    ],
                    'tariff_id': '12'
                },
                {
                    'start_date_time': '2023-01-01T10:00:00Z',
                    'dimensions': [
                        {'type': 'TIME', 'volume': 0.03333333333333333}
                    ],
                    'tariff_id': '12'
                },
                {
                    'start_date_time': '2023-01-01T11:00:00Z',
                    'dimensions': [
                        {'type': 'TIME', 'volume': 0.03333333333333333}
                    ],
                    'tariff_id': '12'}
            ],
            'session_id': None,
            'authorization_reference': None,
            'meter_id': None,
            'signed_data': None,
            'total_fixed_cost': None,
            'total_energy_cost': None,
            'total_time_cost': None,
            'total_parking_time': 2.4,
            'total_parking_cost': None,
            'total_reservation_cost': None,
            'remark': None,
            'invoice_reference_id': None,
            'credit': None,
            'credit_reference_id': None
        }


    def test__calculate_total_charging_time(self):
        df_1 = pd.DataFrame({
                "timestamp_epoch": [1672563600.0, 1672563600.0, 1672563600.0, 1672563660.0, 1672563660.0, 1672563660.0, 1672563720.0, 1672563720.0, 1672563720.0]
            })

        df_2 = pd.DataFrame({
                "timestamp_epoch": [1672567200.0, 1672567200.0, 1672567200.0, 1672567260.0, 1672567260.0, 1672567260.0, 1672567320.0, 1672567320.0, 1672567320.0]
            })

        df_3 = pd.DataFrame({
                "timestamp_epoch": [1672570800.0, 1672570800.0, 1672570800.0, 1672570860.0, 1672570860.0, 1672570860.0, 1672570920.0, 1672570920.0, 1672570920.0]
            })

        data_reader = Mock(DataReader)
        handler = Mock(MeterValuesHandler)
        wrangler = Wrangler(data_reader, handler)
        result = wrangler._total_charging_time_seconds([df_1, df_2, df_3])
        assert result == 360.0