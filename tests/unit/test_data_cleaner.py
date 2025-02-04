import pytest
from pyspark.sql.types import *
from src.service.data_cleaner import DataCleaner
from pyspark.sql.functions import col

def test_clean_data(sample_df):
    """
    Unit test for the DataCleaner class's clean_data method.

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

    # Assertion to check that all null values in the 'power_output' column are removed
    assert cleaned_df.filter(col("power_output").isNull()).count() == 0, "Null values were not removed from power_output"

    # Assertion to check that outliers (values > 150) in the 'power_output' column are removed
    assert cleaned_df.filter(col("power_output") > 150).count() == 0, "Outliers were not removed from power_output"