# Import your libraries
from pyspark.sql.functions import col, month, countDistinct
from pyspark.sql.types import *

fact_events = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("time_id", TimestampType(), True),
    StructField("event_type", StringType(), True),
])
fact_events = fact_events.withColumn(
    "month", month(col("time_id")).alias("month"))
fact_events = fact_events.groupBy("client_id", "month").agg(
    countDistinct("user_id").alias("distinct_user_count")
)

fact_events.toPandas()
