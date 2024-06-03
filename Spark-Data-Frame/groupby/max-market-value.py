# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Start writing code
spark = SparkSession.builder \
    .appName("Forbes Global") \
    .getOrCreate()

# Assuming forbes_global_2010_2014 is your Spark DataFrame
df=None
final_df= df.groupBy(col('sector'))\
.agg(max('marketvalue').alias('max_marketvalue'))\
.orderBy(col('max_marketvalue').desc())

# print(final_df)

# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()
