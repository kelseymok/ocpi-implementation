import boto3
from botocore.stub import Stubber
from data_reader import DataReader

class TestDataReader:
    def test_read_cdrs(self):
        client = boto3.client("dynamodb", region_name="eu-central-1")

        request = {
            'TableName': 'CDRs'
        }

        response = {
            'Items': [
                {
                    'lastUpdatedEpoch': {'N': '1679562423'},
                    'payload': {
                        'B': b'eyJjb3VudHJ5X2NvZGUiOiAiQkUiLCAicGFydHlfaWQiOiAiQkVDIiwgImlkIjogIjRlZTAwOWRkLWEzYWItNDQ5Zi1iY2FiLTY4NzU2ODQ2ZWZjOSIsICJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxNjozMzo0Ny41NjYyNjUrMDA6MDAiLCAiZW5kX2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE5OjM1OjQ4LjE2NTM0OCswMDowMCIsICJjZHJfdG9rZW4iOiB7InVpZCI6ICIxYzY5YjA2ZS0zNzZkLTQ0NmYtOGQ1MS1lMzNmM2QwNTNhNjAiLCAidHlwZSI6ICJSRklEIiwgImNvbnRyYWN0X2lkIjogIjY2YjU3MWMwLWZiZDctNDFhMC1hNDYxLWRkZDJiOTU1YTZiOCJ9LCAiYXV0aF9tZXRob2QiOiAiQVVUSF9SRVFVRVNUIiwgImxhc3RfdXBkYXRlZCI6ICIyMDIzLTAzLTIzVDA5OjA3OjAzWiIsICJjZHJfbG9jYXRpb24iOiB7ImlkIjogIjFiOGFjODM5LTZmOTgtNDQyOC1hOTZhLTg1NjU2NzhmYmU0MSIsICJhZGRyZXNzIjogIkYuUm9vc2V2ZWx0bGFhbiAzQSIsICJjaXR5IjogIkdlbnQiLCAicG9zdGFsX2NvZGUiOiAiOTAwMCIsICJjb3VudHJ5IjogIkJFTCIsICJldnNlX3VpZCI6ICIzMjU2IiwgImV2c2VfaWQiOiAiQkUqQkVDKkUwNDE1MDMwMDMiLCAiY29ubmVjdG9yX2lkIjogIjEiLCAiY29vcmRpbmF0ZXMiOiB7ImxhdGl0dWRlIjogIjMuNzI5OTQ0IiwgImxvbmdpdHVkZSI6ICI1MS4wNDc1OTkifSwgImNvbm5lY3Rvcl9zdGFuZGFyZCI6ICJJRUNfNjIxOTZfVDIiLCAiY29ubmVjdG9yX2Zvcm1hdCI6ICJTT0NLRVQiLCAiY29ubmVjdG9yX3Bvd2VyX3R5cGUiOiAiQUNfMV9QSEFTRSIsICJuYW1lIjogIkdlbnQgWnVpZCJ9LCAiY3VycmVuY3kiOiAiRVVSIiwgInRvdGFsX2Nvc3QiOiB7ImV4Y2xfdmF0IjogMTIzLCAiaW5jbF92YXQiOiBudWxsfSwgInRvdGFsX2VuZXJneSI6IDM3NzE0LCAidG90YWxfdGltZSI6IDMuMDMzNDk5NzQ1Mjc3Nzc3NywgInRhcmlmZnMiOiBbeyJjb3VudHJ5X2NvZGUiOiAiQkUiLCAicGFydHlfaWQiOiAiQkVDIiwgImlkIjogIjEyIiwgImN1cnJlbmN5IjogIkVVUiIsICJsYXN0X3VwZGF0ZWQiOiAiMjAyMy0wMS0wMVQwOTowMDowMFoiLCAidHlwZSI6IG51bGwsICJ0YXJpZmZfYWx0X3RleHQiOiBbXSwgInRhcmlmZl9hbHRfdXJsIjogbnVsbCwgIm1pbl9wcmljZSI6IG51bGwsICJtYXhfcHJpY2UiOiBudWxsLCAiZWxlbWVudHMiOiBbeyJwcmljZV9jb21wb25lbnRzIjogeyJ0eXBlIjogIlRJTUUiLCAicHJpY2UiOiAyLjAsICJzdGVwX3NpemUiOiAzMDAsICJ2YXQiOiAxMC4wfSwgInJlc3RyaWN0aW9ucyI6IG51bGx9XSwgInN0YXJ0X2RhdGVfdGltZSI6IG51bGwsICJlbmRfZGF0ZV90aW1lIjogbnVsbCwgImVuZXJneV9taXgiOiBudWxsfV0sICJjaGFyZ2luZ19wZXJpb2RzIjogW3sic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTY6MzM6NTNaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxNjozODo1M1oiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE2OjQzOjUzWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTY6NDg6NTNaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxNjo1Mzo1M1oiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE2OjU4OjUzWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTc6MDM6NTNaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxNzowODo1M1oiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE3OjEzOjUzWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTc6MTg6NTNaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxNzoyMzo1M1oiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE3OjI4OjUzWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTc6MzM6NTNaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxODowNzowMloiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE4OjEyOjAyWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTg6MTc6MDJaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxODoyMjowMloiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE4OjI3OjAyWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTg6MzI6MDJaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxODozNzowMloiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE5OjAxOjMxWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTk6MDY6MzFaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxOToxMTozMVoiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE5OjE2OjMxWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn0sIHsic3RhcnRfZGF0ZV90aW1lIjogIjIwMjMtMDEtMDFUMTk6MjE6MzFaIiwgImRpbWVuc2lvbnMiOiBbeyJ0eXBlIjogIlRJTUUiLCAidm9sdW1lIjogMC4wfV0sICJ0YXJpZmZfaWQiOiAiMTIifSwgeyJzdGFydF9kYXRlX3RpbWUiOiAiMjAyMy0wMS0wMVQxOToyNjozMVoiLCAiZGltZW5zaW9ucyI6IFt7InR5cGUiOiAiVElNRSIsICJ2b2x1bWUiOiAwLjB9XSwgInRhcmlmZl9pZCI6ICIxMiJ9LCB7InN0YXJ0X2RhdGVfdGltZSI6ICIyMDIzLTAxLTAxVDE5OjMxOjMxWiIsICJkaW1lbnNpb25zIjogW3sidHlwZSI6ICJUSU1FIiwgInZvbHVtZSI6IDAuMH1dLCAidGFyaWZmX2lkIjogIjEyIn1dLCAic2Vzc2lvbl9pZCI6IG51bGwsICJhdXRob3JpemF0aW9uX3JlZmVyZW5jZSI6IG51bGwsICJtZXRlcl9pZCI6IG51bGwsICJzaWduZWRfZGF0YSI6IG51bGwsICJ0b3RhbF9maXhlZF9jb3N0IjogbnVsbCwgInRvdGFsX2VuZXJneV9jb3N0IjogbnVsbCwgInRvdGFsX3RpbWVfY29zdCI6IG51bGwsICJ0b3RhbF9wYXJraW5nX3RpbWUiOiAzLjAzMzQ5OTc0NTI3Nzc3NzcsICJ0b3RhbF9wYXJraW5nX2Nvc3QiOiBudWxsLCAidG90YWxfcmVzZXJ2YXRpb25fY29zdCI6IG51bGwsICJyZW1hcmsiOiBudWxsLCAiaW52b2ljZV9yZWZlcmVuY2VfaWQiOiBudWxsLCAiY3JlZGl0IjogbnVsbCwgImNyZWRpdF9yZWZlcmVuY2VfaWQiOiBudWxsfQ=='},
                    'id': {'S': '4ee009dd-a3ab-449f-bcab-68756846efc9'}
                },
            ],
            'Count': 11,
            'ScannedCount': 11,
            'ResponseMetadata': {
                'RequestId': 'a0568385-1c84-4b6b-8fb4-479f8b15f7fb',
                'HTTPStatusCode': 200,
            }
        }


        with Stubber(client) as stubber:
            stubber.add_response('scan', response, request)
            data_reader = DataReader(client)
            result = data_reader.get_all(table_name="CDRs")
            assert result == [
                {
                    'country_code': 'BE',
                    'party_id': 'BEC',
                    'id': '4ee009dd-a3ab-449f-bcab-68756846efc9',
                    'start_date_time': '2023-01-01T16:33:47.566265+00:00',
                    'end_date_time': '2023-01-01T19:35:48.165348+00:00',
                    'cdr_token': {
                        'uid': '1c69b06e-376d-446f-8d51-e33f3d053a60',
                        'type': 'RFID',
                        'contract_id': '66b571c0-fbd7-41a0-a461-ddd2b955a6b8'
                    },
                    'auth_method': 'AUTH_REQUEST',
                    'last_updated': '2023-03-23T09:07:03Z',
                    'cdr_location': {
                        'id': '1b8ac839-6f98-4428-a96a-8565678fbe41',
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
                    'total_energy': 37714,
                    'total_time': 3.0334997452777777,
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
                        'start_date_time': '2023-01-01T16:33:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T16:38:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T16:43:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T16:48:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T16:53:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T16:58:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T17:03:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T17:08:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T17:13:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T17:18:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T17:23:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T17:28:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T17:33:53Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T18:07:02Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T18:12:02Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T18:17:02Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T18:22:02Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T18:27:02Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T18:32:02Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T18:37:02Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T19:01:31Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T19:06:31Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T19:11:31Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T19:16:31Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T19:21:31Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T19:26:31Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        },
                        {
                        'start_date_time': '2023-01-01T19:31:31Z',
                        'dimensions': [
                          {
                            'type': 'TIME',
                            'volume': 0.0
                          }
                        ],
                        'tariff_id': '12'
                        }
                    ],
                    'session_id': None,
                    'authorization_reference': None,
                    'meter_id': None,
                    'signed_data': None,
                    'total_fixed_cost': None,
                    'total_energy_cost': None,
                    'total_time_cost': None,
                    'total_parking_time': 3.0334997452777777,
                    'total_parking_cost': None,
                    'total_reservation_cost': None,
                    'remark': None,
                    'invoice_reference_id': None,
                    'credit': None,
                    'credit_reference_id': None
                }
            ]
