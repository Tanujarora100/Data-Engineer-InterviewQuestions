# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

worker = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("worker_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("joining_date", TimestampType(), True),
        StructField("department", StringType(), True),
        StructField("worker_id", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
    ]
)
df = worker
df = df.withColumn("joining_month", month(col("joining_date")))
df = df.filter(col("joining_month") == 2)
final_df = df.filter(col("worker_id") % 2 != 0).drop("joining_month").toPandas()
