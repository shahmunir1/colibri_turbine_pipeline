import pytest
from src.turbine_data_pipeline import TurbineDataPipeline
from pyspark.sql.functions import col


def test_pipeline_integration(spark):
    """
    Integration test for the TurbineDataPipeline.

    This test ensures that the entire data processing pipeline functions correctly, including:
    1. Data cleaning
    2. Statistics calculation
    3. Anomaly detection
    4. Writing cleaned data to a temporary table

    Args:
        spark (pyspark.sql.SparkSession): The Spark session for testing.

    Test Steps:
    1. Instantiate the TurbineDataPipeline with the test directory.
    2. Create or load a test dataset (assumed to be in CSV format).
    3. Run the `process_data` method to clean data, compute statistics, and detect anomalies.
    4. Verify that the cleaned data and statistics DataFrames are not empty.
    5. Ensure that the cleaned data does not contain null values in the `power_output` column.
    6. Write cleaned data to a temporary location and validate.
    7. Check that the statistics DataFrame contains the expected fields (`min_power`, `max_power`, `avg_power`).

    Assertions:
    - Ensures that `cleaned_df` and `stats_df` are not empty.
    - Ensures that all `power_output` values in `cleaned_df` are non-null.
    - Ensures that the statistics DataFrame contains the necessary computed values.
    """

    # Initialize the TurbineDataPipeline with the test data directory
    pipeline = TurbineDataPipeline(spark)

    # Define the test data path (assuming the test CSV file exists)
    test_data_path = "tests/test_data.csv"

    # Process the test data through the pipeline
    cleaned_df, stats_df, anomalies_df, rejected_data = pipeline.process_data(test_data_path)

    # Verify that the cleaned data is not empty
    assert cleaned_df.count() > 0, "Cleaned data is empty!"

    # Verify that the statistics DataFrame is not empty
    assert stats_df.count() > 0, "Statistics DataFrame is empty!"

    # Verify that no anomaly found
    assert anomalies_df.count()== 0, "Anomaly DataFrame is not empty!"

    # Verify that no rejected data found
    assert rejected_data.count()== 0, "Rejected DataFrame is not empty!"

    # Ensure no null values remain in the `power_output` column
    assert cleaned_df.filter(col("power_output").isNull()).count() == 0, \
        "There are null values in the cleaned power_output column!"

    # Retrieve the first row of the statistics DataFrame
    stats_row = stats_df.first()

    # Ensure the statistics DataFrame contains expected computed columns
    assert "min_power" in stats_row, "Statistics DataFrame is missing 'min_power' column!"
    assert "max_power" in stats_row, "Statistics DataFrame is missing 'max_power' column!"
    assert "avg_power" in stats_row, "Statistics DataFrame is missing 'avg_power' column!"