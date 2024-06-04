from pyspark.sql.functions import *
from pyspark.sql.types import *

worker = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("worker_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("joining_date", StringType(), True),
        StructField("department", StringType(), True),
    ]
)
df = worker
df = worker.select(col("salary")).distinct()
final_df = df.orderBy("salary", ascending=False).limit(5)
final_df.toPandas()
