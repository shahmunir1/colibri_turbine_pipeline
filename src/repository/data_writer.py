from pyspark.sql import DataFrame
from src.domain.turbine_schema import TurbinePuddle
from src.domain.turbine_schema import TableType
from pyspark.sql.types import *
import logging
from pyspark.sql.utils import AnalysisException, StreamingQueryException
from py4j.protocol import Py4JJavaError


class DataWriterException(Exception):
    """Custom exception for DataWriter errors"""
    pass


class DataWriter:
    
    """
    Class to handle writing DataFrames to different formats, including Delta tables.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def write_data(self, df: DataFrame, puddle: TurbinePuddle, mode: str = "overwrite"):
        """
        Writes a Spark DataFrame to the appropriate storage format based on the configuration in the provided puddle.

        Args:
            df (DataFrame): The Spark DataFrame to be written.
            puddle (TurbinePuddle): An object containing metadata and configuration for storage.
            mode (str, optional): The write mode. Default is "overwrite". Options: "append", "overwrite", "ignore", "error".

        Raises:
            Exception: If the schema does not match or if an unsupported file format is provided.
        """

        try:
            if df is None:
                raise DataWriterException("DataFrame cannot be None")

            if puddle is None:
                raise DataWriterException("TurbinePuddle configuration cannot be None")

            # Verify that the schema of the DataFrame matches the expected schema of the puddle
            if not self.verify_schema(df, puddle.get_schema()):
                raise DataWriterException(f"Schema mismatch for puddle: {puddle}")

            # Determine the file format and write the DataFrame accordingly
            match puddle.get_file_format():
                case TableType.DELTALAKE:
                     # Write data to a Delta Lake table
                    self.logger.info("Writing data to Delta Lake")
                    self.write_to_deltalake(df, puddle.get_config()['data_path'], mode)

                case TableType.JDBC:
                    # Write data to a relational database via JDBC
                    self.logger.info("Writing data to JDBC")
                    config = puddle.get_config()
                    required_keys = ['driver', 'url', 'dbtable', 'username', 'password']
                    
                    if not all(key in config for key in required_keys):
                        raise DataWriterException(f"Missing required JDBC configuration. Required keys: {required_keys}")
                    
                    self.write_to_jdbc(
                        df,
                        config['driver'],
                        config['url'],
                        config['dbtable'],
                        config['username'],
                        config['password'],
                        mode
                    )

                case TableType.CSV:
                    # Write data as a CSV file
                    self.logger.info("Writing data to CSV")
                    self.write_to_csv(df, puddle.get_config()['data_path'], mode)

                case TableType.PARQUET:
                    # Write data as a Parquet file
                    self.logger.info("Writing data to Parquet")
                    self.write_to_parquet(df, puddle.get_config()['data_path'], mode)

                case _:
                    raise DataWriterException(f"Unsupported file format: {puddle.get_file_format()}")

        except (AnalysisException, StreamingQueryException) as e:
            self.logger.error(f"Spark error while writing data: {str(e)}")
            raise DataWriterException(f"Failed to write data: {str(e)}") from e
        except Py4JJavaError as e:
            self.logger.error(f"Java error while writing data: {str(e)}")
            raise DataWriterException(f"Java error occurred: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error while writing data: {str(e)}")
            raise DataWriterException(f"Failed to write data: {str(e)}") from e


    def verify_schema(self, df, expected_schema: StructType) -> bool: 
        """
        Verifies that the given Spark DataFrame schema matches the expected schema.
        
        Args:
            df: pyspark.sql.DataFrame - The DataFrame to verify
            
        Returns:
            bool: True if schemas match, False otherwise
            
        Raises:
            ValueError: If the DataFrame is None or empty
        """

        self.logger.info("Verifying schema for the dataframe")

        if df is None:
            raise ValueError("DataFrame cannot be None")
        
        if df.isEmpty():
            raise ValueError("DataFrame cannot be empty")

        #expected_schema = TurbineRawPuddle.get_schema()
        actual_schema = df.schema
        
        # Compare field names and types
        if len(expected_schema.fields) != len(actual_schema.fields):
            self.logger.error(f"Schema mismatch: expected {len(expected_schema.fields)} fields, but got {len(actual_schema.fields)} fields")
            return False
            
        for expected_field, actual_field in zip(expected_schema.fields, actual_schema.fields):
            if (expected_field.name != actual_field.name or 
                expected_field.dataType != actual_field.dataType):
                self.logger.error(f"Schema mismatch: expected {expected_field}, but got {actual_field}")
                return False
                
        return True        


    def write_to_csv(self, df: DataFrame, table_path: str, mode: str = "overwrite"):
        """
        Writes a DataFrame to a CSV table at the specified path.
        """
        # Writing the DataFrame to a Delta table at the specified path
        try:
            if not table_path:
                raise DataWriterException("Table path cannot be empty")

            df.write.partitionBy("year","month","day").format("csv").mode(mode).save(table_path)
        except Exception as e:
            self.logger.error(f"Error writing to CSV: {str(e)}")
            raise DataWriterException(f"Failed to write to CSV: {str(e)}") from e


    def write_to_parquet(self, df: DataFrame, table_path: str, mode: str = "overwrite"):
        """
        Writes a DataFrame to a Parquet table at the specified path.
        """
        # Writing the DataFrame to a Delta table at the specified path
        try:
            if not table_path:
                raise DataWriterException("Table path cannot be empty")

            df.write.partitionBy("year","month","day").format("parquet").mode(mode).save(table_path)
        except Exception as e:
            self.logger.error(f"Error writing to Parquet: {str(e)}")
            raise DataWriterException(f"Failed to write to Parquet: {str(e)}") from e    


    def write_to_deltalake(self, df: DataFrame, table_path: str, mode: str = "overwrite"):
        """
        Writes a DataFrame to a Delta table at the specified path.
        """

        # Writing the DataFrame to a Delta table at the specified path
        try:
            if not table_path:
                raise DataWriterException("Table path cannot be empty")

            self.logger.info(f"Writing to Delta Lake at path: {table_path}")
            df.write.partitionBy("year","month","day").format("delta").mode(mode).save(table_path)
        except Exception as e:
            self.logger.error(f"Error writing to Delta Lake: {str(e)}")
            raise DataWriterException(f"Failed to write to Delta Lake: {str(e)}") from e


    def write_to_jdbc(self, df: DataFrame, driver: str, url: str, table_name: str, user: str, password: str, mode: str = "append"):
        """
        Writes a Spark DataFrame to a relational database using JDBC.

        Args:
            df (DataFrame): The Spark DataFrame to be written to the database.
            url (str): The JDBC URL of the database (e.g., "jdbc:mysql://host:port/database").
            table_name (str): The name of the table where data should be inserted.
            user (str): The database username.
            password (str): The database password.
            mode (str, optional): The write mode. Options: "append", "overwrite", "ignore", "error". Default is "append".

        Example Usage:
            write_to_database(df, 
                            "jdbc:mysql://localhost:3306/mydb", 
                            "turbine", 
                            "user123", 
                            "password123", 
                            mode="append")
        """
        try:
            if not all([driver, url, table_name, user, password]):
                raise DataWriterException("All JDBC connection parameters must be provided")

            jdbc_properties = {
                "user": user,
                "password": password,
                "driver": driver
            }

            df.write \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", table_name) \
                .options(**jdbc_properties) \
                .mode(mode) \
                .save()
        except Exception as e:
            self.logger.error(f"Error writing to JDBC: {str(e)}")
            raise DataWriterException(f"Failed to write to JDBC: {str(e)}") from e