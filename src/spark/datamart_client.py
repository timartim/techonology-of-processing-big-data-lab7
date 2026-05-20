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


class DataMartClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def load_prepared_training_data(self, spark, input_path: str, min_non_null_ratio: float, target_rows: int, seed: int):
        response = self._post("/v1/training-data", {
            "inputPath": input_path,
            "minNonNullRatio": min_non_null_ratio,
            "targetRows": target_rows,
            "seed": seed,
        })

        data = response["data"]
        rows = data["rows"]
        if not rows:
            raise ValueError("Витрина вернула пустой набор данных для обучения")

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".jsonl", delete=False) as f:
            path = f.name
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")

        return (
            spark.read.json(path),
            data["featureCols"],
            data["productCols"],
            int(data["totalRows"]),
            int(data["workingRows"]),
        )

    def save_training_results(self, profiles_df, centers_df, metrics: dict, model_info: dict) -> None:
        self._post("/v1/training-results", {
            "profiles": self._dataframe_rows(profiles_df),
            "centers": self._dataframe_rows(centers_df),
            "metrics": self._clean(metrics),
            "modelInfo": self._clean(model_info),
        })

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

        if parsed.get("status") != "ok":
            raise RuntimeError(parsed.get("error", "Data mart request failed"))

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
