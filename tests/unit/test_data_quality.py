import pytest
from src.service.data_quality import DataQuality
from src.repository.data_writer import DataWriter
from src.domain.turbine_schema import TurbineAnomalyDataPuddle, TurbineRejectedDataPuddle,TurbineRawPuddle
from pyspark.sql.functions import current_timestamp,lit,col
from pyspark.sql.functions import current_date, year, month, dayofmonth

def test_detect_anomalies(sample_df):
    """
    Unit test for the AnomalyDetector class's detect_anomalies method.

    Args:
        sample_df (pyspark.sql.DataFrame): Input DataFrame containing turbine data.

    Test Steps:
    1. Instantiate the AnomalyDetector class.
    2. Apply the `detect_anomalies` method to identify anomalies in the data.
    3. Validate that exactly one anomaly is detected.
    4. Verify that the detected anomaly has a `power_output` value of 200.0.

    Assertions:
    - Ensures that the count of detected anomalies is exactly 1.
    - Ensures that the first detected anomaly has a `power_output` value of 200.0.
    """
    
    # Initialize the AnomalyDetector instance
    dq = DataQuality()
    
    # Apply anomaly detection to the input DataFrame
    anomalies = dq.detect_anomalies(sample_df)

    # Assertion to check that exactly one anomaly is detected
    assert anomalies.count() == 1, f"Expected 1 anomaly, but found {anomalies.count()}"

    # Retrieve the first detected anomaly
    first_anomaly = anomalies.first()

    # Assertion to verify that the detected anomaly has a power output of 200.0
    assert first_anomaly["power_output"] == 200.0, \
        f"Expected anomaly power output to be 200.0, but found {first_anomaly['power_output']}"
    

def test_detect_null_value(sample_df):
    """
    Unit test case to verify that the detect_invalid_rows method correctly identifies 
    rows containing null values in the given DataFrame.

    Args:
        sample_df (pyspark.sql.DataFrame): Input DataFrame containing turbine data.

    Assertions:
    - Ensures that the count of detected invalid rows is exactly 1.

    """
    dq = DataQuality()  # Initialize the DataQuality class instance

    # Call the method to detect rows with null values
    rows_with_nulls = dq.detect_rejected_rows(sample_df)

    # Get the count of rows identified as invalid (having null values)
    null_count = rows_with_nulls.count()  # Avoid multiple .count() calls

    # Assert that exactly one row with null values is detected
    assert null_count == 1, f"Expected 1 anomaly, but found {null_count}"

def test_data_correctness(spark):
    """
    Unit test case to verify that the detect_invalid_rows method correctly identifies
    rows with invalid data in the given DataFrame.

    Args:
        sample_df (pyspark.sql.DataFrame): Input DataFrame containing turbine data.       
    """
    dq = DataQuality()  # Initialize the DataQuality class instance

    # Read the CSV file into a DataFrame
    df = spark.read \
    .option("header", "true") \
    .schema(TurbineRawPuddle().get_schema()) \
    .csv("tests/test_data.csv")\
        .withColumn("year", year(current_date())) \
        .withColumn("month", month(current_date())) \
        .withColumn("day", dayofmonth(current_date())) \
        .withColumn("insert_timestamp", current_timestamp()) \
        .withColumn("filename", lit("test.csv")) \
        .withColumn("timestamp",col("timestamp").cast("timestamp"))

    # Call the data quality method to valid the data
    result_df = dq.validate_data_correctness(df)

    # Assert that all files have all 5 turbine's data in it
    assert result_df.filter(col("is_valid") == False).count() == 0, f"Expected correctness, but found error"

def test_data_correctness_invalid_data(sample_df):
    """
    Unit test case to verify that the detect_invalid_rows method correctly identifies
    rows with invalid data in the given DataFrame.

    Args:
        sample_df (pyspark.sql.DataFrame): Input DataFrame containing turbine data.       
    """

    dq = DataQuality()  # Initialize the DataQuality class instance

    # Call the data quality method to valid the data
    result_df = dq.validate_data_correctness(sample_df)

    # Assert that some files have missing turbine's data in it
    assert result_df.filter(col("is_valid") == False).count() == 1, f"Expected error, but found 0 error"    
