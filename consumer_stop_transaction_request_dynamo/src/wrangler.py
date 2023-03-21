from datetime import datetime, timezone
from typing import Dict, List

import json
import uuid
from dateutil import parser
from pandas import DataFrame
from pytz import UTC

from ocpi.v221.enums import TokenType, AuthMethod, ConnectorType, ConnectorFormat, PowerType, TariffDimensionType, \
    CdrDimensionType

from data_reader import DataReader
from data_writer import DataWriter

from ocpi.v221.types import CDR, CdrToken, CdrLocation, GeoLocation, Price, PriceComponent, TariffElement, Tariff, \
    ChargingPeriod, CdrDimension

from meter_values_handler import MeterValuesHandler


class Wrangler:
    def __init__(self, data_reader: DataReader, data_writer: DataWriter, meter_values_handler: MeterValuesHandler):
        self.data_reader = data_reader
        self.data_writer = data_writer
        self.meter_values_handler = meter_values_handler

    def _convert_body_to_dict(self, x: Dict):
        x["body"] = json.loads(x["body"])
        return x

    def _mock_location(self) -> CdrLocation:
        # TODO: this should come from the most recent Location for the Charge Point (OCPI Data)
        return CdrLocation(
            id=str(uuid.uuid4()),
            name="Gent Zuid",
            address="F.Rooseveltlaan 3A",
            city="Gent",
            postal_code="9000",
            country="BEL",
            coordinates=GeoLocation(
                latitude="3.729944",
                longitude="51.047599"
            ),
            evse_uid="3256",
            evse_id="BE*BEC*E041503003",
            connector_id="1",
            connector_standard=ConnectorType.iec_62196_t2,
            connector_format=ConnectorFormat.socket,
            connector_power_type=PowerType.ac_1_phase
        )

    def _mock_tariff(self) -> Tariff:
        # TODO: this should be fetched from OCPI Database for relevant tariff
        return Tariff(
                    country_code="BE",
                    party_id="BEC",
                    id="12",
                    currency="EUR",
                    elements=[TariffElement(
                        price_components=PriceComponent(
                            type=TariffDimensionType.time,
                            price=2.00,
                            vat=10.0,
                            step_size=300
                        )
                    )],
                    last_updated="2023-01-01T09:00:00Z"
                )


    def _charging_session_to_charging_period(self, df: DataFrame):
        # This solution is simplistic. In reality, we need to line up charge times with Tariff blocks
        # and calculate ChargingPeriods based on large Tariff blocks.

        sorted_df = df.sort_values(by=['timestamp_epoch'], ignore_index=True)
        return ChargingPeriod(
            start_date_time=parser.parse(sorted_df["timestamp"].iloc[0]).strftime('%Y-%m-%dT%H:%M:%SZ'),  # TODO
            dimensions=[
                CdrDimension(
                    type=CdrDimensionType.time,
                    volume=self._seconds_to_hours(self._get_charging_time_seconds(sorted_df))
                )
            ],
            tariff_id="12"  # TODO this should come from OCPI data Tariff object
        )

    def _get_charging_time_seconds(self, df: DataFrame):
        sorted_df = df.sort_values(by=['eventtimestamp_epoch'], ignore_index=True)
        tmp_df = sorted_df["eventtimestamp_epoch"].iloc[[0, -1]].tolist()
        charging_time_seconds = tmp_df[1] - tmp_df[0]

        return charging_time_seconds

    def _total_charging_time_seconds(self, dfs: List[DataFrame]):
        total_charging_time_seconds = sum([self._get_charging_time_seconds(df) for df in dfs])
        return total_charging_time_seconds

    def _seconds_to_hours(self, seconds: float):
        seconds_in_1_hour = 3600
        return seconds / seconds_in_1_hour

    def process(self, data: Dict):
        print(data)
        start_transaction_request_record = self.data_reader.get_start_transaction_request(transaction_id=data["body"]["transaction_id"])["body"]
        print(f"start_transaction_request_record: {start_transaction_request_record}")

        meter_values = self.data_reader.get_charging_sessions(transaction_id=data["body"]["transaction_id"])
        print(f"meter_values: {meter_values}")
        charging_sessions = self.meter_values_handler.handle(meter_values)

        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        charging_periods = [self._charging_session_to_charging_period(df) for df in charging_sessions]
        total_time = (parser.parse(data["body"]["timestamp"]) - parser.parse(start_transaction_request_record["body"]["timestamp"]).astimezone(UTC)).total_seconds()
        cdr = CDR(
            country_code="BE",
            party_id="BEC",  # following the ISO-15118 standard
            id=str(uuid.uuid4()),
            start_date_time=start_transaction_request_record["body"]["timestamp"],
            end_date_time=data["body"]["timestamp"],
            cdr_token=CdrToken(
                uid=data["id_tag"]["id_token"],
                type=TokenType.rfid,
                contract_id=str(uuid.uuid4())  # TODO: Pull from backoffice data
            ),
            auth_method=AuthMethod.auth_request,
            last_updated=now,
            cdr_location=self._mock_location(),
            currency="EUR",
            total_cost=Price(excl_vat=123),  # TODO
            total_energy=int(data["body"]["meter_stop"]) - int(start_transaction_request_record["body"]["meter_start"]),
            total_time=self._seconds_to_hours(total_time),   # hours
            total_parking_time=self._seconds_to_hours(total_time-self._total_charging_time_seconds(charging_sessions)),
            tariffs=[
                self._mock_tariff()
            ],
            charging_periods=charging_periods
        )

        self.data_writer.write(cdr_object=cdr)

        return cdr