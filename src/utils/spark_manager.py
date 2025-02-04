from pyspark.sql import SparkSession

class SparkManager:
    """
    Singleton class to manage the SparkSession instance for the application.
    
    This ensures that only one SparkSession is created and used throughout the lifecycle of the application.
    """
    
    # Class-level variable to hold the single instance of SparkSession
    _instance = None

    @classmethod
    def get_instance(cls):
        """
        Returns the singleton instance of the SparkSession. If the instance doesn't exist, it is created.
        
        This method ensures that only one SparkSession is created and reused across the application.
        
        Returns:
            SparkSession: The SparkSession instance.
        """
        if cls._instance is None:
            # Create and configure the SparkSession
            cls._instance = SparkSession.builder \
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .appName("WindTurbineDataProcessor") \
                .getOrCreate()  # Get the existing session or create a new one
        return cls._instance

    @classmethod
    def stop_instance(cls):
        """
        Stops the SparkSession and sets the instance to None.
        
        This method can be called to stop the SparkSession when the application is done.
        """
        if cls._instance:
            cls._instance.stop()  # Stop the SparkSession
            cls._instance = None  # Reset the instance to None