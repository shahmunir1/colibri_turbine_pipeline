from src import TurbineDataPipeline
from src.utils.spark_manager import SparkManager

class SparkPipelineManager:
    """
    Manages the execution of the Spark-based data pipeline for turbine data processing.
    """

    def __init__(self):
        """
        Initializes the SparkPipelineManager by creating a Spark session.
        """
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """
        Creates or retrieves a Spark session using the SparkManager.

        Returns:
            SparkSession: An active Spark session instance.
        """
        return SparkManager.get_instance()
            
    def run_pipeline(self):
        """
        Executes the turbine data processing pipeline.

        - Retrieves the number of data groups from the configuration.
        - Iterates through each data group and processes it using the TurbineDataPipeline.
        - Ensures proper cleanup of the Spark session after execution.
        """
        try:
            # Instantiate the TurbineDataPipeline with the active Spark session
            pipeline = TurbineDataPipeline(self.spark)
            pipeline.run()  # Run the pipeline for the current group
        finally:
            # Ensure Spark session is stopped after execution to free resources
            self.spark.stop()

# Run the pipeline if the script is executed directly
if __name__ == "__main__":
    manager = SparkPipelineManager()
    manager.run_pipeline()
