from flattener import Flattener
import json

class TestFlattener:
    def test_meter_values_request(self):
        data = {
            "message_id": "78e9e0a8-717a-48a9-b505-c157ae758b1b",
            "message_type": 2,
            "charge_point_id": "8186ad01-70b6-4f7d-9220-7b02e4fd8177",
            "action": "MeterValues",
            "write_timestamp": "2023-01-01T11:06:47.067257+00:00",
            "body": "{\"connector_id\": 3, \"meter_value\": [{\"timestamp\": \"2023-01-01T11:06:47.067257+00:00\", \"sampled_value\": [{\"value\": \"0.0\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Voltage\", \"phase\": \"L1-N\", \"location\": null, \"unit\": \"V\"}, {\"value\": \"6.32\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Current.Import\", \"phase\": \"L1\", \"location\": null, \"unit\": \"A\"}, {\"value\": \"1412.06\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Power.Active.Import\", \"phase\": \"L1\", \"location\": null, \"unit\": \"W\"}, {\"value\": \"0.0\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Voltage\", \"phase\": \"L2-N\", \"location\": null, \"unit\": \"V\"}, {\"value\": \"0.0\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Current.Import\", \"phase\": \"L2\", \"location\": null, \"unit\": \"A\"}, {\"value\": \"0.0\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Power.Active.Import\", \"phase\": \"L2\", \"location\": null, \"unit\": \"W\"}, {\"value\": \"0.0\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Voltage\", \"phase\": \"L3-N\", \"location\": null, \"unit\": \"V\"}, {\"value\": \"0.0\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Current.Import\", \"phase\": \"L3\", \"location\": null, \"unit\": \"A\"}, {\"value\": \"0.0\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Power.Active.Import\", \"phase\": \"L3\", \"location\": null, \"unit\": \"W\"}, {\"value\": \"1412.06\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Energy.Active.Import.Register\", \"phase\": null, \"location\": null, \"unit\": \"Wh\"}, {\"value\": \"6.32\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Current.Import\", \"phase\": null, \"location\": null, \"unit\": \"A\"}, {\"value\": \"1412.06\", \"context\": \"Sample.Periodic\", \"format\": \"Raw\", \"measurand\": \"Power.Active.Import\", \"phase\": null, \"location\": null, \"unit\": \"W\"}]}], \"transaction_id\": 1}",
            "write_timestamp_epoch": 1672571207067
        }
        data["body"] = json.loads(data["body"])
        flattener = Flattener()
        result = flattener._meter_values_request_normalizer(data)
        assert result.columns.tolist() == [
            'value',
            'context',
            'format',
            'measurand',
            'phase',
            'location',
            'unit',
            'message_id',
            'charge_point_id',
            'action',
            'write_timestamp',
            'write_timestamp_epoch',
            'connector_id',
            'transaction_id',
            'timestamp'
        ]
        assert len(result.to_dict(orient="records")) == 12