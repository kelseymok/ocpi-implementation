from typing import Callable


class DataWriter:
    def __init__(self, conn_cursor):
        self.conn_cursor = conn_cursor

    def write(self, data):
        message_type_mapper = {
            2: "Request",
            3: "Response",
        }
        print(f"Data: {data}")
        key = f"{data['action']}{message_type_mapper[data['message_type']]}"
        print(f"Getting mapper for key {key}")
        return self._mapper(key)(data)

    def _backup(self, data):
        print(f"No mapper handled for {data}")
        return lambda x: x

    def _mapper(self, key) -> Callable:
        backup = lambda x: x
        return {
            "HeartbeatRequest": self._write_heartbeat_request,
            "HeartbeatResponse": self._write_heartbeat_response,
            "BootNotificationRequest": self._write_boot_notification_request,
            "BootNotificationResponse": self._write_boot_notification_response,
            "StartTransactionRequest": self._write_start_transaction_request,
            "StartTransactionResponse": self._write_start_transaction_response,
            "StopTransactionRequest": self._write_stop_transaction_request,
            "StopTransactionResponse": self._write_stop_transaction_response,
            "MeterValuesRequest": self._write_meter_values_request,
            "MeterValuesResponse": self._write_meter_values_response,
        }.get(key, backup)

    def _write_heartbeat_request(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
        )
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
            )
        table_name = "heartbeatrequest"
        self._execute(table_name=table_name, columns=columns, values=values)

    def _write_heartbeat_response(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "bodyCurrentTime"
        )
        values = (
            data["message_id"],
            data["message_type"],
            data["charge_point_id"],
            data["action"],
            data["write_timestamp"],
            data["write_timestamp_epoch"],
            data["body_current_time"],
        )
        table_name = "heartbeatresponse"
        self._execute(table_name=table_name, columns=columns, values=values)

    def _write_boot_notification_request(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "bodyChargePointModel",
            "bodyChargePointVendor",
            "bodyChargeBoxSerialNumber",
            "bodyChargePointSerialNumber",
            "bodyIccid",
            "bodyImsi",
            "bodyMeterSerialNumber",
            "bodyMeterType",
        )
        table_name = "bootNotificationRequest"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
                data["body_charge_point_model"],
                data["body_charge_point_vendor"],
                data["body_charge_box_serial_number"],
                data["body_charge_point_serial_number"],
                data["body_iccid"],
                data["body_imsi"],
                data["body_meter_serial_number"],
                data["body_meter_type"],
            )
        self._execute(table_name, columns, values)

    def _write_boot_notification_response(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "bodyCurrentTime",
            "bodyInterval",
            "bodyStatus",

        )
        table_name = "bootnotificationresponse"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
                data["body_current_time"],
                data["body_interval"],
                data["body_status"],

            )
        self._execute(table_name, columns, values)

    def _write_start_transaction_request(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "connectorId",
            "idTag",
            "meterStart",
            "eventTimestamp",

        )
        table_name = "starttransactionrequest"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
                data["connector_id"],
                data["id_tag"],
                data["meter_start"],
                data["timestamp"],

            )
        self._execute(table_name, columns, values)

    def _write_start_transaction_response(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "bodyTransactionId",
            "bodyIdTagInfoStatus",
            "bodyIdTagInfoParentIdTag",
            "bodyIdTagInfoExpiryDate",

        )
        table_name = "starttransactionresponse"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
                data["body_transaction_id"],
                data["body_id_tag_info_status"],
                data["body_id_tag_info_parent_id_tag"],
                data["body_id_tag_info_expiry_date"],

            )
        self._execute(table_name, columns, values)

    def _write_stop_transaction_request(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "transactionId",
            "reason",
            "idTag",
            "bodyTransactionData",

        )
        table_name = "stoptransactionrequest"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
                data["transaction_id"],
                data["reason"],
                data["id_tag"],
                data["body_transaction_data"],

            )
        self._execute(table_name, columns, values)

    def _write_stop_transaction_response(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "bodyIdTagInfoStatus",
            "bodyIdTagInfoParentIdTag",
            "bodyIdTagInfoExpiryDate",

        )
        table_name = "stoptransactionresponse"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
                data["body_id_tag_info_status"],
                data["body_id_tag_info_parent_id_tag"],
                data["body_id_tag_info_expiry_date"],

            )
        self._execute(table_name, columns, values)

    def _write_meter_values_request(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",
            "eventValue",
            "context",
            "format",
            "measurand",
            "phase",
            "location",
            "unit",
            "connectorId",
            "transactionId",
            "eventTimestamp",

        )
        table_name = "metervaluesrequest"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],
                data["value"],
                data["context"],
                data["format"],
                data["measurand"],
                data["phase"],
                data["location"],
                data["unit"],
                data["connector_id"],
                data["transaction_id"],
                data["timestamp"],

            )
        self._execute(table_name, columns, values)

    def _write_meter_values_response(self, data):
        columns = (
            "messageId",
            "messageType",
            "chargePointId",
            "actionName",
            "writeTimestamp",
            "writeTimestampEpoch",

        )
        table_name = "metervaluesresponse"
        values = (
                data["message_id"],
                data["message_type"],
                data["charge_point_id"],
                data["action"],
                data["write_timestamp"],
                data["write_timestamp_epoch"],

            )
        self._execute(table_name, columns, values)

    def _execute(self, table_name: str, columns: tuple, values: tuple):
        command = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})"
        print(f"Command: {command}")
        print(f"Values: {values}")
        self.conn_cursor.execute(command, values)
