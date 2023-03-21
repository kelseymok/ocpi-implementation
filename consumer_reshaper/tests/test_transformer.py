from transformer import Transformer


class TestTransformer:
    def test__start_transaction_request_reshaper(self):
        data = {
            "message_id": "df984612-05af-4278-9e8a-155d10a91fbd",
            "some": {
                "data": "foobar"
            }
        }
        transformer = Transformer()
        priority_fields, metadata, data_bytes = transformer._start_transaction_request_reshaper(data)
        assert priority_fields == {
            "message_id": "df984612-05af-4278-9e8a-155d10a91fbd"
        }
        assert metadata == {
            "type": "StartTransactionRequest"
        }
        assert data_bytes == b'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJzb21lIjogeyJkYXRhIjogImZvb2JhciJ9fQ=='

    def test__start_transaction_response_reshaper(self):
        data = {
            "message_id": "df984612-05af-4278-9e8a-155d10a91fbd",
            "body": {
                "transaction_id": 1,
                "other": "data"
            }
        }
        transformer = Transformer()
        priority_fields, metadata, data_bytes = transformer._start_transaction_response_reshaper(data)
        assert priority_fields == {
            "message_id": "df984612-05af-4278-9e8a-155d10a91fbd",
            "transaction_id": 1
        }
        assert metadata == {
            "type": "StartTransactionResponse"
        }
        assert data_bytes == b'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJib2R5IjogeyJ0cmFuc2FjdGlvbl9pZCI6IDEsICJvdGhlciI6ICJkYXRhIn19'

    def test__meter_values_request_reshaper(self):
        data = {
            "message_id": "df984612-05af-4278-9e8a-155d10a91fbd",
            "body": {
                "transaction_id": 1,
                "other": "data"
            }
        }
        transformer = Transformer()
        priority_fields, metadata, data_bytes = transformer._meter_values_request_reshaper(data)
        assert priority_fields == {
            "transaction_id": 1
        }
        assert metadata == {
            "type": "MeterValuesRequest"
        }
        assert data_bytes == b'eyJtZXNzYWdlX2lkIjogImRmOTg0NjEyLTA1YWYtNDI3OC05ZThhLTE1NWQxMGE5MWZiZCIsICJib2R5IjogeyJ0cmFuc2FjdGlvbl9pZCI6IDEsICJvdGhlciI6ICJkYXRhIn19'
