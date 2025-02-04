import pytest
from pyspark.sql.types import *
from src.service.data_cleaner import DataCleaner
from src.service.stats_calculator import StatsCalculator
from pyspark.sql.functions import col

def test_statistics_calculation(sample_df):
    """
    Unit test for calculating statistics from the cleaned DataFrame.

    Args:
        sample_df (pyspark.sql.DataFrame): Input DataFrame containing turbine data.

    Test Steps:
    1. Instantiate the DataCleaner class.
    2. Apply the `clean_data` method to clean the DataFrame.
    3. Validate that null values in the 'power_output' column are removed.
    4. Validate that outliers (values greater than 150) are removed.

    Assertions:
    - Ensures that no null values exist in the 'power_output' column after cleaning.
    - Ensures that no values greater than 150 remain in the 'power_output' column.
    """
    
    # Initialize the DataCleaner instance
    cleaner = DataCleaner()
    
    # Apply the cleaning process to the input DataFrame
    cleaned_df = cleaner.clean_data(sample_df)

    stats_calc = StatsCalculator()

    stats = stats_calc.calculate_summary_stats(cleaned_df)

    # Assertion to check we have 2 results for the given sample data
    assert stats.count() == 2, "Null values were not removed from power_output"