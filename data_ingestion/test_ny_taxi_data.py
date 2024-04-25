# test_ny_taxi_data.py

import pytest
import os
from load_data import ny_taxi_data
from pyspark.sql import SparkSession

@pytest.fixture
def sample_data():
    month = [1]
    year = [2019]
    color = ['yellow']
    return ny_taxi_data(month, year, color)

def test_download_data(sample_data, tmp_path):
    # Test if the download_data method downloads files as expected
    sample_data.download_data()

    # Check if the files are downloaded
    downloaded_files = [
        f"{col}/yellow_tripdata_{year}-{month:02}.csv.gz"
        for col in sample_data.color
        for year in sample_data.year
        for month in sample_data.month
    ]

    for file_path in downloaded_files:
        assert os.path.exists(file_path)

def test_create_spark_session(sample_data):
    # Test if the create_spark_session method returns a SparkSession object
    spark = sample_data.create_spark_session()
    assert isinstance(spark, SparkSession)
    assert spark.conf.get('spark.jars') == 'postgresql-42.7.3.jar'

def test_read_data_load(sample_data):
    # Test if the read_data_load method loads data into the database
    sample_data.read_data_load()

    # Add assertions here to check if the data is loaded into the database as expected
    # For example, you could check if the tables are created in the database
    # You could also check if the tables contain expected data by querying them
    # Here we just check if the expected table is created
    assert sample_data.spark.catalog._jcatalog.tableExists("yellow_tripdata")
