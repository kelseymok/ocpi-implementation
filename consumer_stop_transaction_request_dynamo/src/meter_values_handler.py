from typing import Dict, List

import pandas as pd
from pandas import DataFrame
from dateutil import parser


class MeterValuesHandler:

    def handle(self, data: List[Dict]) -> List[DataFrame]:
        dfs = self._split_meter_values_to_charging_sessions(self._reshape(data))
        return dfs

    def _reshape(self, data: List[Dict]) -> DataFrame:
        def decorate(d: Dict):
            d["timestamp"] = d["body"]["meter_value"][0]["timestamp"]
            return d

        return pd.DataFrame([ decorate(x) for x in data ])

    def _identify_time_boundary(self, x, threshold: float = 60.0*3):
        # The Threshold should in reality be smarter. We know our mock data for MeterValues comes at a freq of 60 seconds
        # Perhaps this can be the upper-mid quantile of the distribution to properly identify it.
        a = x.tolist()
        result = a[1] - a[0]
        return 0 if result <= threshold else 1

    def _split_meter_values_to_charging_sessions(self, df: DataFrame) -> List[DataFrame]:
        df["timestamp"].astype(str)
        df["timestamp_epoch"] = df["timestamp"].map(lambda x: parser.parse(x).timestamp())
        df["timestamp_epoch"].astype(float)
        sorted_df = df.sort_values(by=['timestamp_epoch'], ignore_index=True)
        result = sorted_df["timestamp_epoch"].rolling(window=2).apply(self._identify_time_boundary, raw=False)
        split_indicies = result[result == 1.0]
        indicies = split_indicies.index.tolist() + [0, len(sorted_df)]
        indicies.sort()
        collect = []
        for n in range(len(indicies) - 1):
            chunk_range = (indicies[n], indicies[n + 1])
            collect.append(sorted_df.iloc[chunk_range[0]:chunk_range[1]])
        return collect


    def _flatten(self, data: Dict):
        df = pd.json_normalize(
            data,
            record_path=['body', 'meter_value', 'sampled_value'],
            meta=[
                'message_id',
                'message_type',
                'charge_point_id',
                'action',
                'write_timestamp',
                'write_timestamp_epoch',
                ['body', 'connector_id'], ['body', 'transaction_id'],
                ['body', 'meter_value', 'timestamp']
            ]
        ).rename(columns={
            "body.connector_id": "connector_id",
            "body.meter_value.timestamp": "timestamp",
            "body.transaction_id": "transaction_id"}
        )
        df["value"] = df["value"].astype(float)
        df.columns = [c.replace(".", "_").lower() for c in df.columns]
        return df