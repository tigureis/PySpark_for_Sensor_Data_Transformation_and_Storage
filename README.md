# IoT Sensor Data Processing with PySpark

This notebook demonstrates the use of PySpark for processing and analyzing IoT sensor data.

## Description

The notebook performs the following tasks:

1. **Data Loading:** Loads sensor data from a CSV file (`sensores-iot.csv`) into a PySpark DataFrame.
2. **Data Exploration:** Calculates basic statistics, such as mean temperature and humidity.
3. **Data Cleaning:** Handles missing values by filling them with calculated means.
4. **Data Transformation:** Extracts date components (year, month, day) from the timestamp column.
5. **Data Storage:** Writes the processed data to Parquet files using different compression techniques (`snappy`, `gzip`) and partitioning by `device_id`.

## Requirements

* Google Colab environment
* PySpark library (`!pip install pyspark`)

## Usage

1. Open this notebook in Google Colab.
2. Run all the code cells in sequential order.
3. You can explore the data and modify the code to suit your needs.

## Notes

* Ensure that the `sensores-iot.csv` file is uploaded to the Colab environment before running the notebook.
* The notebook uses the Parquet file format for efficient data storage and retrieval.
* Different compression techniques are used to optimize storage space and read/write performance.
* Partitioning the data by `device_id` improves query performance when filtering by device.
