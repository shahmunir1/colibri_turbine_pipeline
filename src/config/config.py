
# Configuration settings for the pipeline or application
CONFIG = {

    # Number of data groups or categories to process
    "data_groups": 3, 
    
    # Threshold for anomaly detection based on standard deviation
    # This value defines the number of standard deviations from the mean to consider as an anomaly
    "std_dev_threshold": 2, 
    
    # Duration of the time window for processing or aggregating data
    # This could be used for time-based calculations or rolling windows (e.g., 24-hour aggregation)
    "window_duration": "24 hours"
}