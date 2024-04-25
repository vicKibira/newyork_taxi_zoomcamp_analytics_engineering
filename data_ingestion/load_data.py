import os
import logging

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import types

class ny_taxi_data():
    def __init__(self, month, year, color):
        self.month = month
        self.year = year
        self.color = color
        self.download_data()
        self.spark = self.create_spark_session()
        self.read_data_load()

    def download_data(self):
        for yr in self.year:
            for mon in self.month:
                for col in self.color:
                    try:
                        filename = f"{col}/{col}_tripdata_{yr}-{mon:02}.csv.gz"
                        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{col}/{col}_tripdata_{yr}-{mon:02}.csv.gz"
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        os.system(f"wget {url} -O {filename}")

                    except Exception as e:
                        logging.info(f"an error occurred: {str(e)}")

    def create_spark_session(self):
        conf = SparkConf()
        conf.set('spark.jars', 'postgresql-42.7.3.jar')

        spark = SparkSession.builder \
                .master("local[1]") \
                .appName("data ingestion") \
                .config(conf=conf) \
                .getOrCreate()
        return spark
        
    def read_data_load(self):
        for col in self.color:
            df = self.spark.read.options(header='true', sep=',').csv(f"{col}/{col}_tripdata_{self.year[0]}-{self.month[0]:02}.csv.gz")
            schema = self.green_schema() if col == 'green' else self.yellow_schema()

            df.write.format("jdbc") \
                .options(url="jdbc:postgresql://172.18.0.2:5432/walmart_db",
                         dbtable=f"{col}_tripdata",
                         user="root",
                         password="root",
                         driver="org.postgresql.Driver") \
                .mode('overwrite') \
                .save()

    def green_schema(self):
        return types.StructType([
            types.StructField('VendorID', types.IntegerType(), True),
            types.StructField('lpep_pickup_datetime', types.TimestampType(), True), 
            types.StructField('lpep_dropoff_datetime', types.TimestampType(), True), 
            types.StructField('store_and_fwd_flag', types.StringType(), True),
            types.StructField('RatecodeID', types.IntegerType(), True), 
            types.StructField('PULocationID', types.IntegerType(), True), 
            types.StructField('DOLocationID', types.IntegerType(), True), 
            types.StructField('passenger_count', types.IntegerType(), True), 
            types.StructField('trip_distance', types.DoubleType(), True),
            types.StructField('fare_amount', types.DoubleType(), True), 
            types.StructField('extra', types.DoubleType(), True), 
            types.StructField('mta_tax', types.DoubleType(), True), 
            types.StructField('tip_amount', types.DoubleType(), True), 
            types.StructField('tolls_amount', types.DoubleType(), True), 
            types.StructField('ehail_fee', types.DoubleType(), True),
            types.StructField('improvement_surcharge', types.DoubleType(), True), 
            types.StructField('total_amount', types.DoubleType(), True), 
            types.StructField('payment_type', types.IntegerType(), True), 
            types.StructField('trip_type', types.IntegerType(), True), 
            types.StructField('congestion_surcharge', types.DoubleType(), True)
        ])                    

    def yellow_schema(self):
        return types.StructType([
            types.StructField('VendorID', types.IntegerType(), True), 
            types.StructField('tpep_pickup_datetime', types.TimestampType(), True),
            types.StructField('tpep_dropoff_datetime', types.TimestampType(), True),
            types.StructField('passenger_count', types.IntegerType(), True), 
            types.StructField('trip_distance', types.DoubleType(), True), 
            types.StructField('RatecodeID', types.IntegerType(), True), 
            types.StructField('store_and_fwd_flag', types.StringType(), True), 
            types.StructField('PULocationID', types.IntegerType(), True), 
            types.StructField('DOLocationID', types.IntegerType(), True), 
            types.StructField('payment_type', types.IntegerType(), True), 
            types.StructField('fare_amount', types.DoubleType(), True), 
            types.StructField('extra', types.DoubleType(), True), 
            types.StructField('mta_tax', types.DoubleType(), True), 
            types.StructField('tip_amount', types.DoubleType(), True), 
            types.StructField('tolls_amount', types.DoubleType(), True), 
            types.StructField('improvement_surcharge', types.DoubleType(), True), 
            types.StructField('total_amount', types.DoubleType(), True), 
            types.StructField('congestion_surcharge', types.DoubleType(), True)
        ])

month = range(1, 2)
year = [2019, 2020]
color = ['yellow', 'green']

load_data = ny_taxi_data(month, year, color)