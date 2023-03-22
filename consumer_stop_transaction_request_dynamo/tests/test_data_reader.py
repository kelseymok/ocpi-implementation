import boto3
from botocore.stub import Stubber

from data_reader import DataReader


class TestDataReader:
    def test_get_start_transaction_request(self):
        client = boto3.client("dynamodb", region_name="eu-central-1")

        expected_request_start_tx_response = {
            'ConsistentRead': True,
            'ExpressionAttributeValues': {':transaction_id': {'N': str(1)}},
            'KeyConditionExpression': 'transaction_id = :transaction_id',
            'Limit': 1,
            'ProjectionExpression': 'message_id',
            'ScanIndexForward': False,
            'Select': 'SPECIFIC_ATTRIBUTES',
            'TableName': 'StartTransactionResponse'
        }

        response_start_tx_response = {
            'Items': [{
                "message_id": {
                    "S": "3513f59b-b11e-4d72-8809-bfb810e8286d"
                },
            }],
            'Count': 1
        }

        expected_request_start_tx_request = {
            'ConsistentRead': True,
            'ExpressionAttributeValues': {':message_id': {'S': "3513f59b-b11e-4d72-8809-bfb810e8286d"}},
            'KeyConditionExpression': 'message_id = :message_id',
            'Limit': 1,
            'ScanIndexForward': False,
            'Select': 'ALL_ATTRIBUTES',
            'TableName': 'StartTransactionRequest'
        }

        response_start_tx_request = {
            'Items': [
                {
                    'payload': {
                        'B': b'eyJtZXNzYWdlX2lkIjogIjM1MTNmNTliLWIxMWUtNGQ3Mi04ODA5LWJmYjgxMGU4Mjg2ZCIsICJtZXNzYWdlX3R5cGUiOiAyLCAiY2hhcmdlX3BvaW50X2lkIjogIjgxODZhZDAxLTcwYjYtNGY3ZC05MjIwLTdiMDJlNGZkODE3NyIsICJhY3Rpb24iOiAiU3RhcnRUcmFuc2FjdGlvbiIsICJ3cml0ZV90aW1lc3RhbXAiOiAiMjAyMy0wMS0wMVQxMTowNjo0Mi4wNjcyNTcrMDA6MDAiLCAiYm9keSI6IHsiY29ubmVjdG9yX2lkIjogMywgImlkX3RhZyI6ICIxYzY5YjA2ZS0zNzZkLTQ0NmYtOGQ1MS1lMzNmM2QwNTNhNjAiLCAibWV0ZXJfc3RhcnQiOiAwLCAidGltZXN0YW1wIjogIjIwMjMtMDEtMDFUMTE6MDY6NDEuMDY3MjU3KzAwOjAwIiwgInJlc2VydmF0aW9uX2lkIjogbnVsbH0sICJ3cml0ZV90aW1lc3RhbXBfZXBvY2giOiAxNjcyNTcxMjAyMDY3fQ=='
                    },
                    'message_id': {
                        'S': '3513f59b-b11e-4d72-8809-bfb810e8286d'
                    }
                }
            ],
            'Count': 1
        }

        with Stubber(client) as stubber:
            stubber.add_response('query', response_start_tx_response, expected_request_start_tx_response)
            stubber.add_response('query', response_start_tx_request, expected_request_start_tx_request)
            data_reader = DataReader(client)
            result = data_reader.get_start_transaction_request(transaction_id=1)
            assert result == {
                'message_id': '3513f59b-b11e-4d72-8809-bfb810e8286d',
                'message_type': 2,
                'charge_point_id': '8186ad01-70b6-4f7d-9220-7b02e4fd8177',
                'action': 'StartTransaction',
                'write_timestamp': '2023-01-01T11:06:42.067257+00:00',
                'body': {
                    'connector_id': 3,
                    'id_tag': '1c69b06e-376d-446f-8d51-e33f3d053a60',
                    'meter_start': 0,
                    'timestamp': '2023-01-01T11:06:41.067257+00:00',
                    'reservation_id': None
                },
                'write_timestamp_epoch': 1672571202067
            }

    def test_get_charging_sessions(self):
        response = {
            'Items': [
                {'transaction_id': {'N': '1'},
                 'payload': {
                     'B': b'eyJtZXNzYWdlX2lkIjogIjcxY2FlZmZiLWJlM2ItNDg1OC05MGZiLTZiMDEyMjkzZDlkYyIsICJtZXNzYWdlX3R5cGUiOiAyLCAiY2hhcmdlX3BvaW50X2lkIjogIjgxODZhZDAxLTcwYjYtNGY3ZC05MjIwLTdiMDJlNGZkODE3NyIsICJhY3Rpb24iOiAiTWV0ZXJWYWx1ZXMiLCAid3JpdGVfdGltZXN0YW1wIjogIjIwMjMtMDEtMDFUMTY6MzU6NTQuMDE0MTIzKzAwOjAwIiwgImJvZHkiOiB7ImNvbm5lY3Rvcl9pZCI6IDMsICJtZXRlcl92YWx1ZSI6IFt7InRpbWVzdGFtcCI6ICIyMDIzLTAxLTAxVDE2OjM1OjU0LjAxNDEyMyswMDowMCIsICJzYW1wbGVkX3ZhbHVlIjogW3sidmFsdWUiOiAiMC4wIiwgImNvbnRleHQiOiAiU2FtcGxlLlBlcmlvZGljIiwgImZvcm1hdCI6ICJSYXciLCAibWVhc3VyYW5kIjogIlZvbHRhZ2UiLCAicGhhc2UiOiAiTDEtTiIsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIlYifSwgeyJ2YWx1ZSI6ICI1LjQ1IiwgImNvbnRleHQiOiAiU2FtcGxlLlBlcmlvZGljIiwgImZvcm1hdCI6ICJSYXciLCAibWVhc3VyYW5kIjogIkN1cnJlbnQuSW1wb3J0IiwgInBoYXNlIjogIkwxIiwgImxvY2F0aW9uIjogbnVsbCwgInVuaXQiOiAiQSJ9LCB7InZhbHVlIjogIjE1MzMuNzYiLCAiY29udGV4dCI6ICJTYW1wbGUuUGVyaW9kaWMiLCAiZm9ybWF0IjogIlJhdyIsICJtZWFzdXJhbmQiOiAiUG93ZXIuQWN0aXZlLkltcG9ydCIsICJwaGFzZSI6ICJMMSIsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIlcifSwgeyJ2YWx1ZSI6ICIwLjAiLCAiY29udGV4dCI6ICJTYW1wbGUuUGVyaW9kaWMiLCAiZm9ybWF0IjogIlJhdyIsICJtZWFzdXJhbmQiOiAiVm9sdGFnZSIsICJwaGFzZSI6ICJMMi1OIiwgImxvY2F0aW9uIjogbnVsbCwgInVuaXQiOiAiViJ9LCB7InZhbHVlIjogIjAuMCIsICJjb250ZXh0IjogIlNhbXBsZS5QZXJpb2RpYyIsICJmb3JtYXQiOiAiUmF3IiwgIm1lYXN1cmFuZCI6ICJDdXJyZW50LkltcG9ydCIsICJwaGFzZSI6ICJMMiIsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIkEifSwgeyJ2YWx1ZSI6ICIwLjAiLCAiY29udGV4dCI6ICJTYW1wbGUuUGVyaW9kaWMiLCAiZm9ybWF0IjogIlJhdyIsICJtZWFzdXJhbmQiOiAiUG93ZXIuQWN0aXZlLkltcG9ydCIsICJwaGFzZSI6ICJMMiIsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIlcifSwgeyJ2YWx1ZSI6ICIwLjAiLCAiY29udGV4dCI6ICJTYW1wbGUuUGVyaW9kaWMiLCAiZm9ybWF0IjogIlJhdyIsICJtZWFzdXJhbmQiOiAiVm9sdGFnZSIsICJwaGFzZSI6ICJMMy1OIiwgImxvY2F0aW9uIjogbnVsbCwgInVuaXQiOiAiViJ9LCB7InZhbHVlIjogIjAuMCIsICJjb250ZXh0IjogIlNhbXBsZS5QZXJpb2RpYyIsICJmb3JtYXQiOiAiUmF3IiwgIm1lYXN1cmFuZCI6ICJDdXJyZW50LkltcG9ydCIsICJwaGFzZSI6ICJMMyIsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIkEifSwgeyJ2YWx1ZSI6ICIwLjAiLCAiY29udGV4dCI6ICJTYW1wbGUuUGVyaW9kaWMiLCAiZm9ybWF0IjogIlJhdyIsICJtZWFzdXJhbmQiOiAiUG93ZXIuQWN0aXZlLkltcG9ydCIsICJwaGFzZSI6ICJMMyIsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIlcifSwgeyJ2YWx1ZSI6ICI2Njg0My4wMTAwMDAwMDAwMSIsICJjb250ZXh0IjogIlNhbXBsZS5QZXJpb2RpYyIsICJmb3JtYXQiOiAiUmF3IiwgIm1lYXN1cmFuZCI6ICJFbmVyZ3kuQWN0aXZlLkltcG9ydC5SZWdpc3RlciIsICJwaGFzZSI6IG51bGwsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIldoIn0sIHsidmFsdWUiOiAiNS40NSIsICJjb250ZXh0IjogIlNhbXBsZS5QZXJpb2RpYyIsICJmb3JtYXQiOiAiUmF3IiwgIm1lYXN1cmFuZCI6ICJDdXJyZW50LkltcG9ydCIsICJwaGFzZSI6IG51bGwsICJsb2NhdGlvbiI6IG51bGwsICJ1bml0IjogIkEifSwgeyJ2YWx1ZSI6ICIxNTMzLjc2IiwgImNvbnRleHQiOiAiU2FtcGxlLlBlcmlvZGljIiwgImZvcm1hdCI6ICJSYXciLCAibWVhc3VyYW5kIjogIlBvd2VyLkFjdGl2ZS5JbXBvcnQiLCAicGhhc2UiOiBudWxsLCAibG9jYXRpb24iOiBudWxsLCAidW5pdCI6ICJXIn1dfV0sICJ0cmFuc2FjdGlvbl9pZCI6IDF9LCAid3JpdGVfdGltZXN0YW1wX2Vwb2NoIjogMTY3MjU5MDk1NDAxNH0='
                 },
                 'id': {'S': 'f050b8be-f990-4924-bafb-b56e73bd7640'}
                 }
            ],
            'Count': 1,
            'ScannedCount': 1,
            'LastEvaluatedKey': {
                'transaction_id': {'N': '1'}, 'id': {'S': 'f050b8be-f990-4924-bafb-b56e73bd7640'}
            },
            'ResponseMetadata': {
                'RequestId': '6911f0d0-02e0-41cd-bcd8-6050c4279926',
                'HTTPStatusCode': 200,
            }
        }

        request = {
            'ConsistentRead': True,
            'ExpressionAttributeValues': {':transaction_id': {'N': '1'}},
            'KeyConditionExpression': 'transaction_id = :transaction_id',
            'ScanIndexForward': False,
            'Select': 'ALL_ATTRIBUTES',
            'TableName': 'MeterValuesRequest'
        }

        client = boto3.client("dynamodb", region_name="eu-central-1")

        with Stubber(client) as stubber:
            stubber.add_response('query', response, request)
            data_reader = DataReader(client)
            result = data_reader.get_charging_sessions(transaction_id=1)
            assert result == [{
                'message_id': '71caeffb-be3b-4858-90fb-6b012293d9dc',
                'message_type': 2,
                'charge_point_id': '8186ad01-70b6-4f7d-9220-7b02e4fd8177',
                'action': 'MeterValues',
                'write_timestamp': '2023-01-01T16:35:54.014123+00:00',
                'body': {
                    'connector_id': 3,
                    'meter_value': [
                        {
                            'timestamp': '2023-01-01T16:35:54.014123+00:00',
                            'sampled_value': [
                                {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L1-N', 'location': None, 'unit': 'V'},
                                {'value': '5.45', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L1', 'location': None, 'unit': 'A'},
                                {'value': '1533.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L1', 'location': None, 'unit': 'W'},
                                {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L2-N', 'location': None, 'unit': 'V'},
                                {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L2', 'location': None, 'unit': 'A'},
                                {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L2', 'location': None, 'unit': 'W'},
                                {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Voltage', 'phase': 'L3-N', 'location': None, 'unit': 'V'},
                                {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': 'L3', 'location': None, 'unit': 'A'},
                                {'value': '0.0', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': 'L3', 'location': None, 'unit': 'W'},
                                {'value': '66843.01000000001', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Energy.Active.Import.Register', 'phase': None, 'location': None, 'unit': 'Wh'},
                                {'value': '5.45', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Current.Import', 'phase': None, 'location': None, 'unit': 'A'},
                                {'value': '1533.76', 'context': 'Sample.Periodic', 'format': 'Raw', 'measurand': 'Power.Active.Import', 'phase': None, 'location': None, 'unit': 'W'}
                            ]
                        }
                    ],
                    'transaction_id': 1
                },
                'write_timestamp_epoch': 1672590954014
            }]
