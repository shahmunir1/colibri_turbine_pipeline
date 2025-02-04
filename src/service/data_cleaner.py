from pyspark.sql.functions import col, mean, stddev
from src.config.config import CONFIG
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

import logging

class DataCleanerException(Exception):
    """Custom exception for DataCleaner errors"""
    pass

class DataCleaner:
    """
    Class for cleaning turbine data.
    
    This class is responsible for removing null values and outliers from turbine data.
    Outliers are defined as values that are more than a specified number of standard deviations 
    away from the mean for each turbine.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def clean_data(self, df) -> DataFrame:
        """
        Cleans the turbine data by removing null values and outliers.
        
        Args:
            df (DataFrame): The input DataFrame containing turbine data.
        
        Returns:
            DataFrame: The cleaned DataFrame with outliers and nulls removed.

        Raises:
            DataCleanerException: If there are issues with data cleaning process
            ValueError: If input DataFrame is None or empty
            KeyError: If required configuration is missing    
        
        Example:
            cleaned_df = DataCleaner().clean_data(df)
        """
        try:
            self.logger.info("Started cleaning data...")
            
            # Validate input DataFrame
            if df.isEmpty():
                raise ValueError("Input DataFrame cannot be empty")

            # Step 1: Remove rows with null values
            cleaned_df = df.dropna()
            
            # Check if all data was filtered out
            if cleaned_df.rdd.isEmpty():
                raise DataCleanerException("All rows contained null values, resulting in empty DataFrame")
            
            # Step 2: Calculate statistics
            try:
                stats = cleaned_df.groupBy("turbine_id").agg(
                    mean("power_output").alias("mean_power"),
                    stddev("power_output").alias("stddev_power")
                )
            except AnalysisException as e:
                raise DataCleanerException(f"Error calculating statistics: {str(e)}")
            
            # Validate configuration
            if "std_dev_threshold" not in CONFIG:
                raise KeyError("Missing 'std_dev_threshold' in configuration")
            
            threshold = CONFIG["std_dev_threshold"]
            
            if not isinstance(threshold, (int, float)) or threshold <= 0:
                raise ValueError("std_dev_threshold must be a positive number")
            
            # Step 3: Remove outliers
            try:
                cleaned_df = cleaned_df.join(stats, "turbine_id") \
                    .where(
                        (col("power_output") <= col("mean_power") + threshold * col("stddev_power")) &
                        (col("power_output") >= col("mean_power") - threshold * col("stddev_power"))
                    ) \
                    .drop("mean_power", "stddev_power")
                
                # Check if all data was filtered out
                if cleaned_df.isEmpty():
                    raise DataCleanerException("All rows were identified as outliers, resulting in empty DataFrame")
                
            except IllegalArgumentException as e:
                raise DataCleanerException(f"Error during outlier removal: {str(e)}")
            
            self.logger.info("Successfully cleaned data")
            return cleaned_df
        except Exception as e:
            self.logger.error(f"Error in clean_data: {str(e)}")
            raise DataCleanerException(f"Failed to clean data: {str(e)}") from e
