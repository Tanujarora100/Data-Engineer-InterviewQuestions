# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

worker = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("worker_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("joining_date", TimestampType(), True),
        StructField("department", StringType(), True),
    ]
)
df = worker
window_spec = Window.orderBy(desc(df["salary"]))
df = df.withColumn("rank", rank().over(window_spec))
filtered_df = df.filter(df["rank"] <= 10)
final_df = filtered_df.select("department", "worker_id", "salary").drop(df["rank"])
final_df.toPandas()


# To validate your solution, convert your final pySpark df to a pandas df
# worker.toPandas()
