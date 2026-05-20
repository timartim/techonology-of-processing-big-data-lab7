from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StructType,
)
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer


class FoodPreprocessor:
    def __init__(self, min_non_null_ratio: float = 0.9, imputer_strategy: str = "median"):
        self.min_non_null_ratio = min_non_null_ratio
        self.imputer_strategy = imputer_strategy

    def build_product_name_columns(self, df):
        if "product_name" not in df.columns:
            return [], []

        dtype = df.schema["product_name"].dataType

        if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            element_fields = {field.name for field in dtype.elementType.fields}
            exprs = []
            names = []

            if "lang" in element_fields:
                exprs.append(
                    F.concat_ws(" | ", F.expr("transform(product_name, x -> x.lang)")).alias("product_name_langs")
                )
                names.append("product_name_langs")

            if "text" in element_fields:
                exprs.append(
                    F.concat_ws(" | ", F.expr("transform(product_name, x -> x.text)")).alias("product_name_texts")
                )
                names.append("product_name_texts")

            return exprs, names

        if isinstance(dtype, StructType):
            exprs = []
            names = []
            for field in dtype.fields:
                name = f"product_name_{field.name}"
                exprs.append(F.col(f"product_name.{field.name}").cast("string").alias(name))
                names.append(name)
            return exprs, names

        return [F.col("product_name").cast("string").alias("product_name")], ["product_name"]

    def fit_transform(self, df, feature_cols):
        imputed_cols = [f"{c}_imp" for c in feature_cols]

        imputer = Imputer(
            inputCols=feature_cols,
            outputCols=imputed_cols,
            strategy=self.imputer_strategy
        )
        imputer_model = imputer.fit(df)
        df_imp = imputer_model.transform(df)

        assembler = VectorAssembler(
            inputCols=imputed_cols,
            outputCol="features_raw"
        )
        assembled = assembler.transform(df_imp)

        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=True,
            withStd=True
        )
        scaler_model = scaler.fit(assembled)

        prepared = scaler_model.transform(assembled).cache()

        return prepared, imputer_model, scaler_model, imputed_cols

    def prepare_inference_frame(self, raw, feature_cols, expected_product_cols):
        product_exprs, _ = self.build_product_name_columns(raw)

        feature_exprs = []
        for c in feature_cols:
            if c in raw.columns:
                feature_exprs.append(F.col(c).cast("double").alias(c))
            else:
                feature_exprs.append(F.lit(None).cast("double").alias(c))

        flat_df = raw.select(*product_exprs, *feature_exprs)

        product_selects = []
        for c in expected_product_cols:
            if c in flat_df.columns:
                product_selects.append(F.col(c))
            else:
                product_selects.append(F.lit(None).cast("string").alias(c))

        df = flat_df.select(*product_selects, *[F.col(c) for c in feature_cols]).dropDuplicates()

        return df

    def transform_with_models(self, df, feature_cols, imputer_model, scaler_model):
        imputed_cols = [f"{c}_imp" for c in feature_cols]

        df_imp = imputer_model.transform(df)

        assembler = VectorAssembler(
            inputCols=imputed_cols,
            outputCol="features_raw"
        )
        assembled = assembler.transform(df_imp)

        prepared = scaler_model.transform(assembled)

        return prepared
