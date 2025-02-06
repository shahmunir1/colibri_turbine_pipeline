import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.service.data_cleaner import DataCleaner
from src.service.stats_calculator import StatsCalculator
from src.service.data_quality import DataQuality
from src.domain.turbine_schema import *

# Bronze layer - Initial ingestion
@dlt.table(
    name="bronze_turbine_data",
    comment="Raw turbine data ingested from source",
    partition_cols=["year", "month", "day"]
)
def get_bronze_turbine_data():
    """
    Reads raw turbine data from the source location and adds timestamp and partition columns.
    
    Returns:
        DataFrame: The raw ingested data with additional date-based partitioning.
    """

    # Replace the read logic based on your actual source (CSV, Parquet, JSON, etc.)
    return (
        add_current_date_columns(spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")  # Modify as per actual data format
            .load(TurbineRawPuddle().get_config()['data_path'])  # Load data from the specified source path
        )
    )

# Silver layer - Cleaned and processed data
@dlt.table(
    name="cleaned_turbine_data",
    comment="Cleaned and processed turbine data",
    partition_cols=["year", "month", "day"]
)
def get_silver_turbine_data():
    """
    Cleans the raw turbine data by removing nulls, outliers, and applying necessary transformations.
    
    Returns:
        DataFrame: The cleaned and structured turbine data.
    """

    # Read raw data from the Bronze table
    raw_df = dlt.read("bronze_turbine_data")

    # Initialize data cleaning class
    cleaner = DataCleaner()

    # Apply data cleaning steps (e.g., handling missing values, filtering invalid records)
    cleaned_df = cleaner.clean_data(raw_df)

    return cleaned_df

# Gold layer - Aggregated statistics
@dlt.table(
    name="turbine_data_stats",
    comment="Statistics of turbine data",
    partition_cols=["year", "month", "day"]
)
@dlt.expect("valid timestamp", "timestamp > '2012-01-01'")  # Ensure only valid timestamps are considered
def get_turbine_data_stats():
    """
    Computes statistical summaries (min, max, avg, etc.) of the cleaned turbine data.

    Returns:
        DataFrame: Data containing statistical aggregations.
    """

    # Read cleaned data from the Silver layer
    clean_df = dlt.read("cleaned_turbine_data")

    # Initialize statistics calculator class
    stats_calculator = StatsCalculator()

    # Compute summary statistics (e.g., min, max, average)
    stats_df = add_current_date_columns(stats_calculator.calculate_summary_stats(clean_df))

    return stats_df

# Data Quality layer - Anomaly detection
@dlt.table(
    name="anomaly_detection",
    comment="Anomaly detection in turbine data",
    partition_cols=["year", "month", "day"]
)
def get_silver_turbine_anomaly_data():
    """
    Detects anomalies in turbine data based on predefined criteria.

    Returns:
        DataFrame: A dataset containing identified anomalies.
    """

    # Read raw data from the Bronze layer
    raw_df = dlt.read("bronze_turbine_data")

    # Initialize data quality check class
    data_quality = DataQuality()

    # Apply anomaly detection rules
    anomaly_df = data_quality.detect_anomalies(raw_df)

    return anomaly_df

# Data Quality layer - Rejected records
@dlt.table(
    name="rejected_records",
    comment="Records that failed quality checks in turbine data",
    partition_cols=["year", "month", "day"]
)
def get_silver_turbine_rejected_data():
    """
    Identifies and stores rejected records that fail validation rules.

    Returns:
        DataFrame: A dataset containing rejected records.
    """

    # Read raw data from the Bronze layer
    raw_df = dlt.read("bronze_turbine_data")

    # Initialize data quality check class
    data_quality = DataQuality()

    # Identify records that should be rejected due to validation failures
    rejected_df = data_quality.detect_rejected_rows(raw_df)

    return rejected_df

def add_current_date_columns(df):
    """
    Adds current timestamp, year, month, and day columns to the given DataFrame.

    Args:
        df (DataFrame): The input PySpark DataFrame.

    Returns:
        DataFrame: The DataFrame with additional date-based columns.
    """
    return df.withColumn("insert_timestamp", current_timestamp()) \
        .withColumn("year", year(current_date())) \
        .withColumn("month", month(current_date())) \
        .withColumn("day", dayofmonth(current_date())) 
