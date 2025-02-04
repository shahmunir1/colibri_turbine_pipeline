from src.repository.data_reader import DataReader
from src.service.data_cleaner import DataCleaner
from src.service.stats_calculator import StatsCalculator
from src.service.data_quality import DataQuality
from src.repository.data_writer import DataWriter
from src.domain.turbine_schema import *
from pyspark.sql import DataFrame
from typing import Tuple

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

    def run(self, group_number: int):
        """Run the pipeline for a specific group of data.
        
        Args:
            group_number (int): The number of the turbine data group to process.
        """
        self.logger.info(f"Started processing data group {group_number}")
        try:
            group_path = str(TurbineRawPuddle.get_data_path / f"data_group_{group_number}.csv")  # Generate the file path for the data group

            # Process the data for the given group (reading, cleaning, statistics, anomaly detection, rejected data)
            cleaned_df, stats_df, anomalies_df, rejected_df = self.process_data(group_path)

            # Write the processed data to Delta tables
            self.writer.write_data(cleaned_df, TurbineCleanedDataPuddle() , mode="append")
            self.writer.write_data(stats_df, TurbineStatsDataPuddle(), mode="append")
            self.writer.write_data(anomalies_df, TurbineAnomalyDataPuddle(), mode="append")
            self.writer.write_data(rejected_df, TurbineRejectedDataPuddle(), mode="append")

            cleaned_df.sparkSession.catalog.clearCache()  # Clear the cache to free up memory

            logging.info(f"Finished processing data group {group_number}")
        except Exception as e:
            self.logger.error(f"Failed to run pipeline for data group {group_number}: {str(e)}")
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
            # Read raw data from CSV
            raw_df = self.reader.read_csv(path).cache() # Cache the raw DataFrame for reuse

            # Clean the data (remove nulls, outliers, etc.)
            cleaned_df = self.cleaner.clean_data(raw_df).cache() # Cache the cleaned DataFrame for reuse

            # Calculate statistics like min, max, avg, etc.
            stats_df = self.stats_calculator.calculate_summary_stats(cleaned_df)

            # Detect anomalies based on predefined criteria
            anomalies_df = self.data_qulaity.detect_anomalies(raw_df)

            # Detect invalid data from the raw data
            rejected_df = self.data_qulaity.detect_invalid_rows(raw_df)

            self.logger.info(f"Finished processing data from {path}, returning cleaned, stats, and anomalies dataframes")

            raw_df.unpersist() # Unpersist the raw DataFrame to free up memory

            return cleaned_df, stats_df, anomalies_df, rejected_df
        except Exception as e:
            self.logger.error(f"Failed to process data from {path}: {str(e)}")
            raise RuntimeError(f"Data processing pipeline failed: {str(e)}")