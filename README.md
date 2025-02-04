# Wind Turbine Data Processing Pipeline

### Overview:

This project provides a data processing pipeline for analyzing wind turbine data. It includes modules for data cleaning, anomaly detection, statistics calculation, and data writing using Apache Spark and Delta Lake.

### Features:

- Data Cleaning: Removes null values and filters outliers based on statistical thresholds.

- Anomaly Detection: Identifies turbines whose power output deviates beyond two standard deviations from the mean.

- Statistics Calculation: Computes summary statistics (min, max, average, standard deviation) for each turbine.

- Data Writing: Writes the processed data to different formats like Deltalake, JDBC, Parquet, and CSV.

- Persist the rejected and anomalies data to CSV for further analysis.

- Scalability: Uses Apache Spark for processing large datasets.

### This will:

- Read raw turbine data from the configured input path.

- Clean and preprocess the data.

- Compute statistical summaries.

- Detect anomalies.

- Write the processed data to Delta tables.