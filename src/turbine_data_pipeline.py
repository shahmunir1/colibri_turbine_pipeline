from src.repository.data_reader import DataReader
from src.service.data_cleaner import DataCleaner
from src.service.stats_calculator import StatsCalculator
from src.service.data_quality import DataQuality
from src.repository.data_writer import DataWriter
from src.domain.turbine_schema import *
from pyspark.sql import DataFrame
from typing import Tuple
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import current_date, year, month, dayofmonth


import logging


class TurbineDataPipeline:
    """Pipeline for processing turbine data.
    
    This class orchestrates the reading, cleaning, processing, and writing of wind turbine data through
    various stages, including anomaly detection and statistics calculation.
    """
    
    def __init__(self, spark):
        """
        Initializes the turbine data pipeline with necessary components.
        """
         # Get the SparkSession instance using the SparkManager singleton
        self.spark = spark

        # Initialize the different components of the pipeline
        self.reader = DataReader(self.spark)
        self.cleaner = DataCleaner()
        self.stats_calculator = StatsCalculator()
        self.data_qulaity = DataQuality()
        self.writer = DataWriter()
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Run the pipeline for a specific group of data.
        
        Args:
            group_number (int): The number of the turbine data group to process.
        """
        self.logger.info(f"Started processing turbine pipeline.")
        try:

            # Process the data for the given group (reading, cleaning, statistics, anomaly detection, rejected data)
            cleaned_df, stats_df, anomalies_df, rejected_df = self.process_data(TurbineRawPuddle().get_config()['data_path'])

            # Write the processed data to Delta tables
            self.writer.write_data(cleaned_df, TurbineCleanedDataPuddle() , mode="append")
            self.writer.write_data(stats_df, TurbineStatsDataPuddle(), mode="append")

            if(anomalies_df.count() > 0):
                self.logger.warning("Anomalies detected: %s", anomalies_df.count())  # Log the number of anomalies detected
                self.writer.write_data(anomalies_df, TurbineAnomalyDataPuddle(), mode="append")   
            else:  # If no anomalies are detected, log a message
                self.logger.info("No anomalies detected.")

            if(rejected_df.count() > 0):
                self.logger.warning("Rows with null values identified: %s", rejected_df.count())  # Log the number of rows with null values
                self.writer.write_data(rejected_df, TurbineRejectedDataPuddle(), mode="append")
            else: # If no rows with null values are identified, log a message
                self.logger.info("No rows with null values identified.")                      

            cleaned_df.sparkSession.catalog.clearCache()  # Clear the cache to free up memory

            logging.info(f"Finished processing turbine pipeline")
        except Exception as e:
            self.logger.error(f"Failed to run turbine pipeline: {str(e)}")
            raise RuntimeError(f"Data processing pipeline failed: {str(e)}")    

    def process_data(self, path: str) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """Process the data from the given path.

        This method orchestrates reading, cleaning, calculating statistics, and anomaly detection.

        Args:
            path (str): The file path to the data.
        
        Returns:
            tuple: A tuple containing the cleaned DataFrame, the statistics DataFrame, 
                   and the anomalies DataFrame.
        """

        if not path:
            raise ValueError("Path cannot be None or empty")
        
        self.logger.info(f"Processing data from {path}")

        try:
            # Read raw data from CSV and attach date columns
            raw_df = self.add_current_date_columns(self.reader.read_csv(path)).cache() # Cache the raw DataFrame for reuse

            # Clean the data (remove nulls, outliers, etc.)
            cleaned_df = self.cleaner.clean_data(raw_df).cache() # Cache the cleaned DataFrame for reuse

            # Calculate statistics like min, max, avg, etc.
            stats_df = self.add_current_date_columns(self.stats_calculator.calculate_summary_stats(cleaned_df))

            # Detect anomalies based on predefined criteria and invalid data from the raw data
            anomalies_df, rejected_df = self.data_qulaity.validate_data_quality(raw_df)

            self.logger.info(f"Finished processing data from {path}, returning cleaned, stats, and anomalies dataframes")

            raw_df.unpersist() # Unpersist the raw DataFrame to free up memory

            return cleaned_df, stats_df, anomalies_df, rejected_df
        except Exception as e:
            self.logger.error(f"Failed to process data from {path}: {str(e)}")
            raise RuntimeError(f"Data processing pipeline failed: {str(e)}")

    def add_current_date_columns(self, df):
        """
        Adds current year, month, and day as new columns to the given DataFrame.

        Args:
            df (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with added columns for the current year, month, and day.
        """
        return df.withColumn("insert_timestamp", current_timestamp()) \
                .withColumn("year", year(current_date())) \
                .withColumn("month", month(current_date())) \
                .withColumn("day", dayofmonth(current_date()))    
