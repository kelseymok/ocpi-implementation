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
            collected_events = collected_events + [
                {"messageid": message_id, "messagetype": 2, "chargepointid": charge_point_id,
                 "actionname": "MeterValues", "writetimestamp": current_time.isoformat(),
                 "writetimestampepoch": int(current_time.timestamp()), "eventvalue": power_import,
                 "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L1",
                 "location": None, "unit": "W", "connectorid": 3, "transactionid": 6,
                 "eventtimestamp": current_time.isoformat()},
                {"messageid": message_id, "messagetype": 2, "chargepointid": charge_point_id,
                 "actionname": "MeterValues", "writetimestamp": current_time.isoformat(),
                 "writetimestampepoch": int(current_time.timestamp()), "eventvalue": power_import,
                 "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": None,
                 "location": None, "unit": "W", "connectorid": 3, "transactionid": 6,
                 "eventtimestamp": current_time.isoformat()},
                {"messageid": message_id, "messagetype": 2, "chargepointid": charge_point_id,
                 "actionname": "MeterValues", "writetimestamp": current_time.isoformat(),
                 "writetimestampepoch": int(current_time.timestamp()), "eventvalue": charge_keeper.cumulative_charge,
                 "context": "Sample.Periodic", "format": "Raw", "measurand": "Energy.Active.Import.Register",
                 "phase": None, "location": None, "unit": "Wh", "connectorid": 3, "transactionid": 6,
                 "eventtimestamp": current_time.isoformat()},
            ]
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
        df = pd.DataFrame(meter_values)

        data_reader = Mock(DataReader)
        data_writer = Mock(DataWriter)
        wrangler = Wrangler(data_reader, data_writer)
        result = wrangler._split_meter_values_to_charging_sessions(df)
        assert len(result) == 3
        assert result[0].index.tolist() == [0, 1, 2, 3, 4, 5, 6, 7, 8]
        assert result[0]["eventtimestamp"].tolist() == ['2023-01-01T09:00:00+00:00', '2023-01-01T09:00:00+00:00', '2023-01-01T09:00:00+00:00', '2023-01-01T09:01:00+00:00', '2023-01-01T09:01:00+00:00', '2023-01-01T09:01:00+00:00', '2023-01-01T09:02:00+00:00', '2023-01-01T09:02:00+00:00', '2023-01-01T09:02:00+00:00']
        assert result[1].index.tolist() == [9, 10, 11, 12, 13, 14, 15, 16, 17]
        assert result[1]["eventtimestamp"].tolist() == ['2023-01-01T10:00:00+00:00', '2023-01-01T10:00:00+00:00', '2023-01-01T10:00:00+00:00', '2023-01-01T10:01:00+00:00', '2023-01-01T10:01:00+00:00', '2023-01-01T10:01:00+00:00', '2023-01-01T10:02:00+00:00', '2023-01-01T10:02:00+00:00', '2023-01-01T10:02:00+00:00']
        assert result[2].index.tolist() == [18, 19, 20, 21, 22, 23, 24, 25, 26]
        assert result[2]["eventtimestamp"].tolist() == ['2023-01-01T11:00:00+00:00', '2023-01-01T11:00:00+00:00', '2023-01-01T11:00:00+00:00', '2023-01-01T11:01:00+00:00', '2023-01-01T11:01:00+00:00', '2023-01-01T11:01:00+00:00', '2023-01-01T11:02:00+00:00', '2023-01-01T11:02:00+00:00', '2023-01-01T11:02:00+00:00']


    def test__get_charging_time(self):
        charge = ChargeKeeper()
        meter_values = self.generate_session(start_time="2023-01-01T09:00:00Z",
                                             charge_point_id="ee361d57-74cb-4f66-bda0-f5ff6eab50bf",
                                             charge_keeper=charge)
        df = pd.DataFrame(meter_values)
        df["eventtimestamp_epoch"] = df["eventtimestamp"].map(lambda x: parser.parse(x).timestamp())
        df["eventtimestamp_epoch"].astype(float)
        data_reader = Mock(DataReader)
        data_writer = Mock(DataWriter)
        wrangler = Wrangler(data_reader, data_writer)
        result = wrangler._get_charging_time_seconds(df)
        assert result == 120.0


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
        assert json_result == '{"country_code": "BE", "party_id": "BEC", "id": "df984612-05af-4278-9e8a-155d10a91fbd", "start_date_time": "2023-01-01T09:00:00+00:00", "end_date_time": "2023-01-01T11:30:00Z", "cdr_token": {"uid": "123", "type": "RFID", "contract_id": "df984612-05af-4278-9e8a-155d10a91fbd"}, "auth_method": "AUTH_REQUEST", "last_updated": "2023-01-01T09:00:00Z", "cdr_location": {"id": "df984612-05af-4278-9e8a-155d10a91fbd", "address": "F.Rooseveltlaan 3A", "city": "Gent", "postal_code": "9000", "country": "BEL", "evse_uid": "3256", "evse_id": "BE*BEC*E041503003", "connector_id": "1", "coordinates": {"latitude": "3.729944", "longitude": "51.047599"}, "connector_standard": "IEC_62196_T2", "connector_format": "SOCKET", "connector_power_type": "AC_1_PHASE", "name": "Gent Zuid"}, "currency": "EUR", "total_cost": {"excl_vat": 123, "incl_vat": null}, "total_energy": 10984.73, "total_time": 2.5, "tariffs": [{"country_code": "BE", "party_id": "BEC", "id": "12", "currency": "EUR", "last_updated": "2023-01-01T09:00:00Z", "type": null, "tariff_alt_text": [], "tariff_alt_url": null, "min_price": null, "max_price": null, "elements": [{"price_components": {"type": "TIME", "price": 2.0, "step_size": 300, "vat": 10.0}, "restrictions": null}], "start_date_time": null, "end_date_time": null, "energy_mix": null}], "charging_periods": [{"start_date_time": "2023-01-01T09:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}, {"start_date_time": "2023-01-01T10:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}, {"start_date_time": "2023-01-01T11:00:00Z", "dimensions": [{"type": "TIME", "volume": 0.03333333333333333}], "tariff_id": "12"}], "session_id": null, "authorization_reference": null, "meter_id": null, "signed_data": null, "total_fixed_cost": null, "total_energy_cost": null, "total_time_cost": null, "total_parking_time": 2.4, "total_parking_cost": null, "total_reservation_cost": null, "remark": null, "invoice_reference_id": null, "credit": null, "credit_reference_id": null}'
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