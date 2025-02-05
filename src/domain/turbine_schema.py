from enum import Enum
from abc import ABC, abstractmethod
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType, LongType

# Enum defining supported file formats for data storage
class TableType(Enum):
    CSV = "csv"
    DELTALAKE = "deltalake"
    JDBC = "jdbc"
    PARQUET = "parquet"

# Abstract base class defining the interface for a data puddle
class TurbinePuddle(ABC):
    """
    Abstract base class to manage the metadata of different types of turbine data puddles.
    Defines the required methods that all puddle types must implement.
    """

    @abstractmethod
    def get_schema(self) -> StructType:
        """
        Returns the schema of the dataset.

        Returns:
            StructType: A PySpark StructType object representing the dataset schema.
        """
        pass

    @abstractmethod    
    def get_config(self) -> dict[str, str]:
        """
        Returns the configuration details, such as file paths or database credentials.

        Returns:
            dict: A dictionary containing key-value pairs of configuration details.
        """
        pass

    @abstractmethod
    def get_file_format(self) -> TableType:
        """
        Returns the storage format of the dataset.

        Returns:
            TableType: An enum representing the file format (CSV, JDBC, etc.).
        """
        pass

# Concrete class representing raw turbine data storage metadata
class TurbineRawPuddle(TurbinePuddle):
    """
    Represents metadata for the raw turbine data stored in CSV format.
    """

    def get_file_format(self) -> TableType:
        return TableType.CSV

    def get_schema(self) -> StructType:
        """
        Defines the schema for raw turbine data, including:
        - timestamp: When the reading was recorded
        - turbine_id: Unique identifier for the turbine
        - wind_speed: Measured wind speed
        - wind_direction: Wind direction in degrees
        - power_output: Power output in kW/MW
        """
        return StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("turbine_id", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", LongType(), True),
            StructField("power_output", DoubleType(), True)
        ])

    def get_config(self) -> dict[str, str]:
        """
        Returns the storage path for raw turbine data.
        Modify this path based on actual data lake location.
        """
        return {"data_path": "/path/to/the/datalake/turbine/raw"}

# Concrete class representing rejected turbine data
class TurbineRejectedDataPuddle(TurbinePuddle):
    """
    Represents metadata for turbine data that was rejected due to validation failures.
    """

    def get_file_format(self) -> TableType:
        return TableType.CSV

    def get_schema(self) -> StructType:
        """
        Schema is identical to raw data as rejected data still contains full records.
        """
        return StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("turbine_id", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", LongType(), True),
            StructField("power_output", DoubleType(), True),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("insert_timestamp", TimestampType(), False),
            StructField("filename", StringType(), False)
        ])

    def get_config(self) -> dict[str, str]:
        """
        Returns the storage path for rejected turbine data.
        """
        return {"data_path": "/path/to/the/datalake/turbine/rejected"}

# Concrete class representing anomaly detection results
class TurbineAnomalyDataPuddle(TurbinePuddle):
    """
    Represents metadata for turbine data that contains anomalies detected through data analysis.
    """

    def get_file_format(self) -> TableType:
        return TableType.CSV

    def get_schema(self) -> StructType:
        """
        Schema includes all raw fields plus calculated mean and standard deviation of power output.
        """
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("turbine_id", IntegerType(), False),
            StructField("power_output", DoubleType(), False),
            StructField("mean_power", DoubleType(), False),  # Mean power over a period
            StructField("stddev_power", DoubleType(), False),  # Standard deviation of power
            StructField("wind_speed", DoubleType(), False),
            StructField("wind_direction", LongType(), False),
            StructField("filename", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("insert_timestamp", TimestampType(), False)
        ])

    def get_config(self) -> dict[str, str]:
        return {"data_path": "/path/to/the/datalake/turbine/anomaly"}

# Concrete class representing cleaned turbine data that is stored in a database
class TurbineCleanedDataPuddle(TurbinePuddle):
    """
    Represents metadata for cleaned turbine data, which is stored in a relational database using JDBC.
    """

    def get_file_format(self) -> TableType:
        return TableType.JDBC

    def get_schema(self) -> StructType:
        """
        Schema is similar to raw data but ensures there are no missing values.
        """
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("turbine_id", IntegerType(), False),
            StructField("wind_speed", DoubleType(), False),
            StructField("wind_direction", LongType(), False),
            StructField("power_output", DoubleType(), False),
            StructField("filename", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("insert_timestamp", TimestampType(), False)
        ])

    def get_config(self) -> dict[str, str]:
        """
        Returns database connection details.
        Modify these credentials and connection settings as per deployment.
        """
        return {
            "username": "username",
            "password": "password",
            "driver": "driver",
            "url": "connection_url",
            "dbtable": "dbtable"
        }

# Concrete class representing aggregated turbine statistics stored in a database
class TurbineStatsDataPuddle(TurbinePuddle):
    """
    Represents metadata for aggregated turbine statistics such as min, max, average, and standard deviation.
    Stored in a relational database using JDBC.
    """

    def get_file_format(self) -> TableType:
        return TableType.JDBC

    def get_schema(self) -> StructType:
        """
        Schema includes statistical aggregates calculated over time windows.
        """
        return StructType([
            StructField("turbine_id", IntegerType(), False),
            StructField("start", TimestampType(), False),  # Start of the time window
            StructField("end", TimestampType(), False),  # End of the time window
            StructField("min_power", DoubleType(), False),
            StructField("max_power", DoubleType(), False),
            StructField("avg_power", DoubleType(), False),
            StructField("stddev_power", DoubleType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("insert_timestamp", TimestampType(), False)
        ])

    def get_config(self) -> dict[str, str]:
        """
        Returns database connection details for storing turbine statistics.
        """
        return {
            "username": "username",
            "password": "password",
            "driver": "driver",
            "url": "connection_url",
            "dbtable": "dbtable"
        }
