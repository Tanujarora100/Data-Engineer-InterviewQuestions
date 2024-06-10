from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

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
df = (
    df.filter(col("first_name") == "Amitah")
    .select(instr(col("first_name"), "a").alias("results"))
    .toPandas()
)

# To validate your solution, convert your final pySpark df to a pandas df
# worker.toPandas()
