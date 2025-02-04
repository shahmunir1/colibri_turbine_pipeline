from pyspark.sql.functions import window, min, max, avg, stddev, col
from src.config.config import CONFIG
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
import logging
from typing import Optional

class StatsCalculator:
    """
    Class to calculate summary statistics for wind turbine data.
    
    This class computes various summary statistics, including the minimum, maximum, average, 
    and standard deviation of the power output for each turbine within a specified time window.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.required_columns = ['turbine_id', 'timestamp', 'power_output']

    def _validate_dataframe(self, df: Optional[DataFrame]) -> None:
        """
        Validates the input DataFrame for required columns and data types.
        
        Args:
            df: Input DataFrame to validate
            
        Raises:
            ValueError: If DataFrame is None or missing required columns
            TypeError: If DataFrame is not a PySpark DataFrame
        """
        if df is None:
            raise ValueError("Input DataFrame cannot be None")
            
        if not isinstance(df, DataFrame):
            raise TypeError("Input must be a PySpark DataFrame")

        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame is missing required columns: {missing_columns}")

    def _validate_config(self) -> None:
        """
        Validates the configuration parameters.
        
        Raises:
            ValueError: If window_duration is missing or invalid
        """
        if 'window_duration' not in CONFIG:
            raise ValueError("Configuration missing 'window_duration' parameter")
        
        if not isinstance(CONFIG['window_duration'], str) or not CONFIG['window_duration'].strip():
            raise ValueError("Invalid window_duration configuration. Must be a non-empty string")
    
    
    def calculate_summary_stats(self, df) -> DataFrame:
        """
        Calculates the summary statistics for power output grouped by turbine and within a time window.
        
        Args:
            df (DataFrame): The input DataFrame containing wind turbine data with a timestamp, turbine ID, 
                            and power output.
        
        Returns:
            DataFrame: A DataFrame containing the calculated statistics for each turbine within the window.

        Raises:
            ValueError: If input validation fails
            TypeError: If input is not a PySpark DataFrame
            RuntimeError: If calculation fails    
        
        Example:
            stats_df = StatsCalculator().calculate_summary_stats(df)
        """
        if df is None:
            raise ValueError("Input DataFrame cannot be None")
        
        try:
            # Validate inputs
            self._validate_dataframe(df)
            self._validate_config()

            self.logger.info(f"Calculating summary statistics for wind turbine data, using window duration: {CONFIG['window_duration']}")

            # Group data by turbine_id and time window, then calculate statistics on power_output
            result_df = df.groupBy(
                "turbine_id",  # Grouping by turbine ID to calculate stats per turbine
                window("timestamp", CONFIG["window_duration"])  # Grouping by time window (e.g., 24 hours)
            ).agg(
                min("power_output").alias("min_power"),  # Minimum power output
                max("power_output").alias("max_power"),  # Maximum power output
                avg("power_output").alias("avg_power"),  # Average power output
                stddev("power_output").alias("stddev_power")  # Standard deviation of power output
            ).select("turbine_id", "window.start", "window.end", "min_power", "max_power", "avg_power", "stddev_power")

            # Verify the result is not empty
            if result_df.isEmpty():
                self.logger.warning("Calculation resulted in empty DataFrame")

            return result_df

        except AnalysisException as e:
            self.logger.error(f"PySpark analysis error: {str(e)}")
            raise RuntimeError(f"Failed to analyze data: {str(e)}")
        except ValueError as e:
            self.logger.error(f"Validation error: {str(e)}")
            raise
        except TypeError as e:
            self.logger.error(f"Type error: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error while calculating summary statistics: {str(e)}")
            raise RuntimeError(f"Failed to calculate summary statistics: {str(e)}")
    