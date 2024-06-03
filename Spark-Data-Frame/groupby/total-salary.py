import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *



# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Salary Department Analysis") \
    .getOrCreate()


result = worker.groupby('department').agg(F.sum('salary').alias('sum')).toPandas()