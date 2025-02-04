from src.domain.turbine_schema import TurbineRawPuddle
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from pyspark.sql import SparkSession

import logging

class DataReaderException(Exception):
    """Custom exception for DataReader errors"""
    pass

class DataReader:
    """
    Class to handle reading data from various file formats into Spark DataFrames.
    
    This class provides methods to read data, specifically CSV files, and can be extended to support other formats.
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the DataReader instance and gets the SparkSession from the SparkManager.
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def read_csv(self, file_path: str) -> DataFrame:
        """
        Reads a CSV file into a Spark DataFrame with a predefined schema.

        Args:
            file_path (str): The path to the CSV file to be read.

        Returns:
            DataFrame: The Spark DataFrame created from the CSV file.

        Example:
            reader = DataReader()
            df = reader.read_csv("path/to/file.csv")
        """
        if not file_path:
            raise DataReaderException("File path cannot be empty")

        try:
            self.logger.info(f"Reading CSV file from {file_path}")
            return self.spark.read \
                .option("header", "true") \
                .schema(TurbineRawPuddle().get_schema()) \
                .csv(file_path)

        except AnalysisException as e:
            error_msg = f"Failed to analyze CSV file at {file_path}: {str(e)}"
            self.logger.error(error_msg)
            raise DataReaderException(error_msg)

        except IllegalArgumentException as e:
            error_msg = f"Invalid argument while reading CSV file at {file_path}: {str(e)}"
            self.logger.error(error_msg)
            raise DataReaderException(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error while reading CSV file at {file_path}: {str(e)}"
            self.logger.error(error_msg)
            raise DataReaderException(error_msg)