from datetime import datetime
from decimal import Decimal
import json
from math import isfinite
import tempfile
import time
from http.client import RemoteDisconnected
from urllib.error import URLError
from urllib.request import Request, urlopen

import numpy as np
from pyspark.sql import Row


class DataMartProtocol:
    STATUS = "status"
    DATA = "data"
    ERROR = "error"

    ROWS = "rows"
    FEATURE_COLS = "featureCols"
    PRODUCT_COLS = "productCols"
    TOTAL_ROWS = "totalRows"
    WORKING_ROWS = "workingRows"

    INPUT_PATH = "inputPath"
    MIN_NON_NULL_RATIO = "minNonNullRatio"
    TARGET_ROWS = "targetRows"
    SEED = "seed"

    PROFILES = "profiles"
    CENTERS = "centers"
    METRICS = "metrics"
    MODEL_INFO = "modelInfo"

    @classmethod
    def training_data_request(cls, input_path: str, min_non_null_ratio: float, target_rows: int, seed: int):
        return {
            cls.INPUT_PATH: input_path,
            cls.MIN_NON_NULL_RATIO: min_non_null_ratio,
            cls.TARGET_ROWS: target_rows,
            cls.SEED: seed,
        }

    @classmethod
    def training_results_request(cls, profiles, centers, metrics: dict, model_info: dict):
        return {
            cls.PROFILES: profiles,
            cls.CENTERS: centers,
            cls.METRICS: metrics,
            cls.MODEL_INFO: model_info,
        }


class DataMartClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def load_prepared_training_data(self, spark, input_path: str, min_non_null_ratio: float, target_rows: int, seed: int):
        response = self._post(
            "/v1/training-data",
            DataMartProtocol.training_data_request(input_path, min_non_null_ratio, target_rows, seed),
        )

        data = response[DataMartProtocol.DATA]
        rows = data[DataMartProtocol.ROWS]
        if not rows:
            raise ValueError("Витрина вернула пустой набор данных для обучения")

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".jsonl", delete=False) as f:
            path = f.name
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")

        return (
            spark.read.json(path),
            data[DataMartProtocol.FEATURE_COLS],
            data[DataMartProtocol.PRODUCT_COLS],
            int(data[DataMartProtocol.TOTAL_ROWS]),
            int(data[DataMartProtocol.WORKING_ROWS]),
        )

    def save_training_results(self, profiles_df, centers_df, metrics: dict, model_info: dict) -> None:
        self._post(
            "/v1/training-results",
            DataMartProtocol.training_results_request(
                profiles=self._dataframe_rows(profiles_df),
                centers=self._dataframe_rows(centers_df),
                metrics=self._clean(metrics),
                model_info=self._clean(model_info),
            ),
        )

    def _post(self, path: str, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        parsed = None
        last_error = None
        for _ in range(30):
            try:
                with urlopen(request, timeout=300) as response:
                    parsed = json.loads(response.read().decode("utf-8"))
                break
            except (URLError, RemoteDisconnected, ConnectionError) as error:
                last_error = error
                time.sleep(2)

        if parsed is None:
            raise RuntimeError(f"Витрина данных недоступна: {last_error}")

        if parsed.get(DataMartProtocol.STATUS) != "ok":
            raise RuntimeError(parsed.get(DataMartProtocol.ERROR, "Data mart request failed"))

        return parsed

    def _dataframe_rows(self, df):
        return [self._clean(row) for row in df.toLocalIterator()]

    def _clean(self, value):
        if isinstance(value, Row):
            return {k: self._clean(v) for k, v in value.asDict(recursive=True).items()}
        if isinstance(value, dict):
            return {k: self._clean(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._clean(v) for v in value]
        if isinstance(value, tuple):
            return [self._clean(v) for v in value]
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value)
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, float) and not isfinite(value):
            return None
        return value
