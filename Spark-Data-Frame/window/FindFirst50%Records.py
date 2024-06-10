from pyspark.sql.functions import *
from pyspark.sql.window import Window
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
window_spec = Window.orderBy("worker_id")
df = df.withColumn("rnk", row_number().over(window_spec))
maximum_rows = df.select(max("rnk")).collect()[0][0]

df = df.filter(col("rnk") <= maximum_rows / 2).drop("rnk").toPandas()
