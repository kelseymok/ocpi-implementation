import pandas as pd
from sqlalchemy import text


class DataReader:
    def __init__(self, engine):
        self.engine = engine

    def get_start_transaction_request(self, transaction_id):
        command = f"select * from starttransactionrequest where messageid = (select messageid from starttransactionresponse where bodytransactionid = {transaction_id} limit 1)"
        df = self._execute(command)
        records = df.to_dict(orient="records")
        print(records)
        return records

    def get_charging_sessions(self, transaction_id):
        command = f"select * from metervaluesrequest where transactionId = {transaction_id}"
        df = self._execute(command)
        records = df.to_dict(orient="records")
        print(records)
        return records

    def _execute(self, command: str):
        with self.engine.connect() as conn:
            df = pd.read_sql_query(text(command), con=conn)
            return df


