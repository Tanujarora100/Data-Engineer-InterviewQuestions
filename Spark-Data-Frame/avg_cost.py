from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Uber Ride Requests") \
    .getOrCreate()

result_df = uber_ride_requests.groupBy("request_status") \
    .agg(avg("monetary_cost").alias("average_cost")) \
    .orderBy("average_cost", ascending=True)

# Convert the final PySpark DataFrame to a Pandas DataFrame
result_pandas_df = result_df.toPandas()

# Show the Pandas DataFrame
# print(result_df)÷