
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType


@pytest.fixture(scope="session", autouse=True)
def spark():
    spark = SparkSession.builder.appName("test") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[2]").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def sample_df(spark):
    """"Create a sample DataFrame with the following schema"""

    data = [
        ("2022-03-01 00:00:00", 1, 11.8, 169, 95.7),
        ("2022-03-01 00:00:00", 1, 11.8, 169, 95.8),
        ("2022-03-01 00:00:00", 1, 11.8, 169, 95.9),
        ("2022-03-01 00:00:00", 1, 11.8, 169, None),  # Null value
        ("2022-03-01 00:00:00", 1, 11.8, 169, 120.0), 
        ("2022-03-01 10:00:00", 1, 11.8, 169, 115.7),
        ("2022-03-01 10:00:00", 1, 11.8, 169, 105.8),
        ("2022-03-01 10:00:00", 1, 11.8, 169, 110.9),
        ("2022-03-01 10:00:00", 1, 11.8, 169, 90.0),  
        ("2022-03-01 00:00:00", 1, 11.8, 169, 97.0),
        ("2022-03-01 00:00:00", 1, 11.8, 169, 200.0)  # Outlier
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", LongType(), True),
        StructField("power_output", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)