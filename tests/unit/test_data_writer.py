import pytest
from src.repository.data_writer import DataWriter
from src.service.data_cleaner import DataCleaner
from src.service.stats_calculator import StatsCalculator
from unittest.mock import Mock
from pyspark.sql.types import *
from src.domain.turbine_schema import TableType, TurbineStatsDataPuddle, TurbineCleanedDataPuddle
from pyspark.sql.functions import lit, col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import current_date, year, month, dayofmonth

def test_write_clean_data(spark):
    """
    Test the write_to_table function in the DataWriter class.
    This test checks if the write_data function writes data to the specified location.
    """
    
    # Create a sample DataFrame
    data = [
        ("2025-02-03T12:00:00", 1, 5.2, 45, 150.0),
        ("2025-02-03T12:05:00", 2, 6.5, 90, 160.0)
    ]

    schema = StructType([
        StructField("timestamp", StringType(), True), 
        StructField("turbine_id", IntegerType(), False),   
        StructField("wind_speed", DoubleType(), False),     
        StructField("wind_direction", LongType(), False),   
        StructField("power_output", DoubleType(), False)
    ])
    df = spark.createDataFrame(data, schema) \
                .withColumn("filename", lit("test.csv")) \
                .withColumn("year", year(current_date())) \
                .withColumn("month", month(current_date())) \
                .withColumn("day", dayofmonth(current_date())) \
                .withColumn("insert_timestamp", current_timestamp()) \
                .withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # Create a DataWriter instance and mock the 'write_to_table' method
    writer = DataWriter()

    # Create the mock
    turbine_cleaned_data_puddle_mock = Mock()

    # Configure the mock methods
    turbine_cleaned_data_puddle_mock.get_file_format.return_value = TableType.DELTALAKE
    turbine_cleaned_data_puddle_mock.get_config.return_value = {
        "data_path": "./tmp/test_clean_data_table"
    }
    turbine_cleaned_data_puddle_mock.get_schema.return_value = TurbineCleanedDataPuddle().get_schema()

    # Try to call the process_data method
    try:
        writer.write_data(df, turbine_cleaned_data_puddle_mock, mode="overwrite")
    except Exception as e:
        pytest.fail(f"Exception was raised: {e}")  # Fail the test if any exception is raised

def test_write_stats_data(sample_df):
    """
    Test the writing of turbine statistics data to the specified location.
    """

    # Initialize the DataCleaner instance
    cleaner = DataCleaner()
    
    # Apply the cleaning process to the input DataFrame
    cleaned_df = cleaner.clean_data(sample_df) 

    stats_calc = StatsCalculator()

    stats = stats_calc.calculate_summary_stats(cleaned_df) \
                .withColumn("year", year(current_date())) \
                .withColumn("month", month(current_date())) \
                .withColumn("day", dayofmonth(current_date())) \
                .withColumn("insert_timestamp", current_timestamp()) 
    
    # Create a DataWriter instance and mock the 'write_to_table' method
    writer = DataWriter()

    # Create the mock
    turbine_stats_data_puddle_mock = Mock()

    # Configure the mock methods
    turbine_stats_data_puddle_mock.get_file_format.return_value = TableType.DELTALAKE
    turbine_stats_data_puddle_mock.get_config.return_value = {
        "data_path": "./tmp/test_stats_table"
    }
    turbine_stats_data_puddle_mock.get_schema.return_value = TurbineStatsDataPuddle().get_schema()

    # Try to call the process_data method
    try:
        writer.write_data(stats, turbine_stats_data_puddle_mock, mode="overwrite")
    except Exception as e:
        pytest.fail(f"Exception was raised: {e}")  # Fail the test if any exception is raised