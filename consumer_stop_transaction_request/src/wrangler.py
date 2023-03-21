from datetime import datetime, timezone
from typing import Dict

import json
import uuid
from dateutil import parser
from pytz import UTC

from ocpi.v221.enums import TokenType, AuthMethod, ConnectorType, ConnectorFormat, PowerType, TariffDimensionType, \
    CdrDimensionType

from data_reader import DataReader
from data_writer import DataWriter

from ocpi.v221.types import CDR, CdrToken, CdrLocation, GeoLocation, Price, PriceComponent, TariffElement, Tariff, \
    ChargingPeriod, CdrDimension


class Wrangler:
    def __init__(self, data_reader: DataReader, data_writer: DataWriter):
        self.data_reader = data_reader
        self.data_writer = data_writer

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

    def process(self, data: Dict):
        print(data)
        start_transaction_request_record = self.data_reader.get_start_transaction_request(transaction_id=data["body"]["transaction_id"])
        print(f"start_transaction_request_record: {start_transaction_request_record}")

        meter_values = self.data_reader.get_charging_sessions(transaction_id=data["body"]["transaction_id"])
        print(f"meter_values: {meter_values}")
        # TODO: Split out by charging sessions (pauses in charging), aggregate by V, A, kWh (CdrDimensions) and shape into ChargingPeriods
        # Instead of returning a Dict above, it should return a dataframe
        # Window + calculate time difference

        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        cdr = CDR(
            country_code="BE",
            party_id="BEC",  # following the ISO-15118 standard
            id=str(uuid.uuid4()),
            start_date_time=start_transaction_request_record.start_time,
            end_date_time=data["timestamp"],
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
            total_energy=data["body"]["meter_stop"] - start_transaction_request_record["meter_start"],
            total_time=(parser.parse(data["body"]["timestamp"]) - start_transaction_request_record["timestamp"].astimezone(UTC)).total_seconds() / 3600,   # hours
            tariffs=[
                self._mock_tariff()
            ],
            charging_periods=[  # TODO: Fetch from MeterValues Window pull
                ChargingPeriod(
                    start_date_time="",  # TODO
                    dimensions=[
                        CdrDimension(
                            type=CdrDimensionType.time,
                            volume=123  # TODO
                        )
                    ],
                    tariff_id="12"  # TODO this should come from OCPI data Tariff object
                ),

            ]
        )

        self.data_writer.write(cdr_object=cdr)

        return cdr