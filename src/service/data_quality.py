from pyspark.sql.functions import col, mean, stddev,reduce,exists,array
from src.config.config import CONFIG
import logging
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException, IllegalArgumentException


class DataQualityError(Exception):
    """Custom exception for DataQuality related errors"""
    pass

class DataQuality:
    """
    Class to detect anomalies in turbine data.
    
    This class identifies anomalies in turbine power output by comparing each value to the mean and 
    standard deviation of the turbine's power output. Values that are more than a specified number of 
    standard deviations away from the mean are considered anomalies.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.required_columns = ["turbine_id", "power_output", "timestamp", "wind_speed", "wind_direction"]


    def _validate_dataframe(self, df: DataFrame) -> None:
        """Validates if the input DataFrame has the required columns"""
        if not isinstance(df, DataFrame):
            raise DataQualityError("Input must be a Spark DataFrame")
        
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            raise DataQualityError(f"Missing required columns: {', '.join(missing_columns)}")    
    
    def detect_anomalies(self, df) -> DataFrame:
        """
        Detects anomalies in turbine data based on the power output.
        
        Anomalies are defined as values that are more than a specified number of standard deviations 
        away from the mean for each turbine.
        
        Args:
            df (DataFrame): The input DataFrame containing turbine data.
        
        Returns:
            DataFrame: A DataFrame containing anomalies with relevant information such as timestamp, 
                       turbine_id, and power output along with statistics.

        Raises:
            DataQualityError: If there are issues with input validation or processing
            Exception: For any unexpected errors during processing
        
        Example:
            anomalies_df = DataQuality().detect_anomalies(df)
        """
        
        try:
            # Validate input DataFrame
            self._validate_dataframe(df)     

            self.logger.info(f"Detecting anomalies and invalid rows in turbine data using threshold {CONFIG['std_dev_threshold']} standard deviations.")
            
            # Step 1: Calculate the mean and standard deviation of power output for each turbine
            stats = df.groupBy("turbine_id").agg(
                mean("power_output").alias("mean_power"),  # Calculate the mean power output for each turbine
                stddev("power_output").alias("stddev_power")  # Calculate the standard deviation for each turbine
            )
            
            # Step 2: Define the threshold for anomaly detection from the configuration
            threshold = CONFIG["std_dev_threshold"]  # The threshold for how many standard deviations will be considered anomalous
            
            # Step 3: Identify anomalies by comparing the power output to the mean ± threshold * standard deviation
            anomalies = df.join(stats, "turbine_id") \
                .where(
                # Anomalies are power_output values outside the range of mean ± threshold * stddev
                (col("power_output") > col("mean_power") + threshold * col("stddev_power")) |
                (col("power_output") < col("mean_power") - threshold * col("stddev_power"))
            ) \
            .select(
                "timestamp",  # Include timestamp for the anomaly
                "turbine_id",  # Include turbine ID for the anomaly
                "power_output",  # Include the power output value for the anomaly
                "mean_power",  # Include the calculated mean power for reference
                "stddev_power",  # Include the calculated standard deviation for reference
                "wind_speed", 
                "wind_direction"
            )
            
            if(anomalies.count() > 0):
                self.logger.warning("Anomalies detected: %s", anomalies.count())  # Log the number of anomalies detected
            else:  # If no anomalies are detected, log a message
                self.logger.info("No anomalies detected.") 
            
            # Step 4: Return the anomalies and rows that have null values
            return anomalies
        except AnalysisException as e:
            error_msg = f"Spark analysis error during anomaly detection: {str(e)}"
            self.logger.error(error_msg)
            raise DataQualityError(error_msg) from e
        except IllegalArgumentException as e:
            error_msg = f"Invalid argument error during anomaly detection: {str(e)}"
            self.logger.error(error_msg)
            raise DataQualityError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during anomaly detection: {str(e)}"
            self.logger.error(error_msg)
            raise DataQualityError(error_msg) from e
   
    
    def detect_invalid_rows(self, df) -> DataFrame:
        """
        Detects all the rows having null values in turbine data based on the power output.
        
        Args:
            df (DataFrame): The input DataFrame containing turbine data.
        
        Returns:
            DataFrame: A DataFrame containing all the rows having null values in any column for further insepctions.

        Raises:
            DataQualityError: If there are issues with input validation or processing
            Exception: For any unexpected errors during processing        
        
        Example:
            rows_with_nulls = DataQuality().detect_invalid_rows(df)
        """

        try:
            # Validate input DataFrame
            self._validate_dataframe(df)

            # Step 1: Identify rows with null values
            self.logger.info("Identifying rows with null values in turbine data.")  

            # Create condition to check for nulls across all columns
            null_conditions = [df[col].isNull() for col in df.columns]

            # Combine conditions with OR
            combined_condition = null_conditions[0]
            for condition in null_conditions[1:]:
                combined_condition = combined_condition | condition

            # Filter dataframe to get only rows with nulls
            rows_with_nulls = df.filter(combined_condition)

            if(rows_with_nulls.count() > 0):
                self.logger.warning("Rows with null values identified: %s", rows_with_nulls.count())  # Log the number of rows with null values
            else: # If no rows with null values are identified, log a message
                self.logger.info("No rows with null values identified.") 
        
            # Step 2: Return the rows that have null values
            return rows_with_nulls
        except AnalysisException as e:
            error_msg = f"Spark analysis error during null detection: {str(e)}"
            self.logger.error(error_msg)
            raise DataQualityError(error_msg) from e
        except IllegalArgumentException as e:
            error_msg = f"Invalid argument error during null detection: {str(e)}"
            self.logger.error(error_msg)
            raise DataQualityError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during null detection: {str(e)}"
            self.logger.error(error_msg)
            raise DataQualityError(error_msg) from e