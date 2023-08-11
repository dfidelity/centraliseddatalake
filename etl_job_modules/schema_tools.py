import json
from pyspark.sql.types import StructType


class SchemaTools:
    def __init__(self, spark):
        self.spark = spark

    def dump(self, df):
        return df.schema.json()

    def load(self, path):
        schema_json = self.spark.read.text(path).first()[0]
        return StructType.fromJson(json.loads(schema_json))

