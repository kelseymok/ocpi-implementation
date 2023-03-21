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



    # def test__get_charging_time(self):
    #     charge = ChargeKeeper()
    #     meter_values = self.generate_session(start_time="2023-01-01T09:00:00Z",
    #                                          charge_point_id="ee361d57-74cb-4f66-bda0-f5ff6eab50bf",
    #                                          charge_keeper=charge)
    #     df = pd.DataFrame(meter_values)
    #     df["eventtimestamp_epoch"] = df["eventtimestamp"].map(lambda x: parser.parse(x).timestamp())
    #     df["eventtimestamp_epoch"].astype(float)
    #     data_reader = Mock(DataReader)
    #     data_writer = Mock(DataWriter)
    #     wrangler = Wrangler(data_reader, data_writer)
    #     result = wrangler._get_charging_time_seconds(df)
    #     assert result == 120.0


    @freeze_time("2023-01-01T09:00:00Z")
    @patch('uuid.uuid4', mock_uuid)
    def test_process(self):
        random.seed(10)
        data_reader = Mock()
        data_reader.get_start_transaction_request = Mock(return_value={
            "starttime": parser.parse("2023-01-01T09:00:00Z").isoformat(),
            "meterstart": 1000,
            "eventtimestamp": parser.parse("2023-01-01T09:00:00Z").isoformat()
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
        df = pd.DataFrame(meter_values)
        data_reader.get_charging_sessions = Mock(return_value=df)

        data_writer = Mock()
        data_writer.write.return_value = True

        wrangler = Wrangler(data_reader=data_reader, data_writer=data_writer)
        data = {
            "id_tag": {
                "id_token": "123"
            },
            "body": {
                "meter_stop": charge.cumulative_charge,
                "transaction_id": 6,
                "timestamp": "2023-01-01T11:30:00Z",
            }
        }
        result = wrangler.process(data)
        json_result = json.dumps(asdict(result))
        assert json_result == '{"country_code": "BE", "party_id": "BEC", "id": "df984612-05af-4278-9e8a-155d10a91fbd", "start_date_time": "2023-01-01T09:00:00+00:00", "end_date_time": "2023-01-01T11:30:00Z", "cdr_token": {"uid": "123", "type": "RFID", "contract_id": "df984612-05af-4278-9e8a-155d10a91fbd"}, "auth_method": "AUTH_REQUEST", "last_updated": "2023-01-01T09:00:00Z", "cdr_location": {"id": "df984612-05af-4278-9e8a-155d10a91fbd", "address": "F.Rooseveltlaan 3A", "city": "Gent", "postal_code": "9000", "country": "BEL", "evse_uid": "3256", "evse_id": "BE*BEC*E041503003", "connector_id": "1", "coordinates": {"latitude": "3.729944", "longitude": "51.047599"}, "connector_standard": "IEC_62196_T2", "connector_format": "SOCKET", "connector_power_type": "AC_1_PHASE", "name": "Gent Zuid"}, "currency": "EUR", "total_cost": {"excl_vat": 123, "incl_vat": None}, "total_energy": 10984.73, "total_time": 2.5, "tariffs": [{"country_code": "BE", "party_id": "BEC", "id": "12", "currency": "EUR", "last_updated": "2023-01-01T09:00:00Z", "type": None, "tariff_alt_text": [], "tariff_alt_url": None, "min_price": None, "max_price": None, "elements": [{"price_components": {"type": "TIME", "price": 2.0, "step_size": 300, "vat": 10.0}, "restrictions": None}], "start_date_time": None, "end_date_time": None, "energy_mix": None}], "charging_periods": [{"start_date_time": "2023-01-01T09:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}, {"start_date_time": "2023-01-01T10:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}, {"start_date_time": "2023-01-01T11:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}], "session_id": None, "authorization_reference": None, "meter_id": None, "signed_data": None, "total_fixed_cost": None, "total_energy_cost": None, "total_time_cost": None, "total_parking_time": 2.4, "total_parking_cost": None, "total_reservation_cost": None, "remark": None, "invoice_reference_id": None, "credit": None, "credit_reference_id": None}'
        assert json.loads(json_result) == {
            "country_code": "BE",
            "party_id": "BEC",
            "id": "df984612-05af-4278-9e8a-155d10a91fbd",
            "start_date_time": "2023-01-01T09:00:00+00:00",
            "end_date_time": "2023-01-01T11:30:00Z",
            "cdr_token": {
                "uid": "123",
                "type": "RFID",
                "contract_id": "df984612-05af-4278-9e8a-155d10a91fbd"
            },
            "auth_method": "AUTH_REQUEST",
            "last_updated": "2023-01-01T09:00:00Z",
            "cdr_location": {
                "id": "df984612-05af-4278-9e8a-155d10a91fbd",
                "address": "F.Rooseveltlaan 3A",
                "city": "Gent",
                "postal_code": "9000",
                "country": "BEL",
                "evse_uid": "3256",
                "evse_id": "BE*BEC*E041503003",
                "connector_id": "1",
                "coordinates": {
                  "latitude": "3.729944",
                  "longitude": "51.047599"
                },
                "connector_standard": "IEC_62196_T2",
                "connector_format": "SOCKET",
                "connector_power_type": "AC_1_PHASE",
                "name": "Gent Zuid"
            },
            "currency": "EUR",
            "total_cost": {
                "excl_vat": 123,
                "incl_vat": None
            },
            "total_energy": 10984.73,
            "total_time": 2.5,
            "tariffs": [
                {
                  "country_code": "BE",
                  "party_id": "BEC",
                  "id": "12",
                  "currency": "EUR",
                  "last_updated": "2023-01-01T09:00:00Z",
                  "type": None,
                  "tariff_alt_text": [],
                  "tariff_alt_url": None,
                  "min_price": None,
                  "max_price": None,
                  "elements": [
                    {
                      "price_components": {
                        "type": "TIME",
                        "price": 2.0,
                        "step_size": 300,
                        "vat": 10.0
                      },
                      "restrictions": None
                    }
                  ],
                  "start_date_time": None,
                  "end_date_time": None,
                  "energy_mix": None
                }
            ],
            "charging_periods": [
                {
                  "start_date_time": "2023-01-01T09:00:00Z",
                  "dimensions": [
                    {
                      "type": "TIME",
                      "volume": 0.03333333333333333
                    }
                  ],
                  "tariff_id": "12"
                },
                {
                    "start_date_time": "2023-01-01T10:00:00Z",
                    "dimensions": [
                        {
                            "type": "TIME",
                            "volume": 0.03333333333333333
                        }
                    ],
                    "tariff_id": "12"
                },
                {
                    "start_date_time": "2023-01-01T11:00:00Z",
                    "dimensions": [
                        {
                          "type": "TIME",
                          "volume": 0.03333333333333333
                        }
                    ],
                    "tariff_id": "12"
                }
            ],
            "session_id": None,
            "authorization_reference": None,
            "meter_id": None,
            "signed_data": None,
            "total_fixed_cost": None,
            "total_energy_cost": None,
            "total_time_cost": None,
            "total_parking_time": 2.4,
            "total_parking_cost": None,
            "total_reservation_cost": None,
            "remark": None,
            "invoice_reference_id": None,
            "credit": None,
            "credit_reference_id": None
            }

    def test__calculate_total_charging_time(self):
        df_1 = pd.DataFrame({
                "eventtimestamp_epoch": [1672563600.0, 1672563600.0, 1672563600.0, 1672563660.0, 1672563660.0, 1672563660.0, 1672563720.0, 1672563720.0, 1672563720.0]
            })

        df_2 = pd.DataFrame({
                "eventtimestamp_epoch": [1672567200.0, 1672567200.0, 1672567200.0, 1672567260.0, 1672567260.0, 1672567260.0, 1672567320.0, 1672567320.0, 1672567320.0]
            })

        df_3 = pd.DataFrame({
                "eventtimestamp_epoch": [1672570800.0, 1672570800.0, 1672570800.0, 1672570860.0, 1672570860.0, 1672570860.0, 1672570920.0, 1672570920.0, 1672570920.0]
            })

        data_reader = Mock(DataReader)
        data_writer = Mock(DataWriter)
        wrangler = Wrangler(data_reader, data_writer)
        result = wrangler._total_charging_time_seconds([df_1, df_2, df_3])
        assert result == 360.0