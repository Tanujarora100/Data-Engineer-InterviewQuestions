from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    TimestampType,
)

# Start writing code
worker = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("worker_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("joining_date", TimestampType(), True),
        StructField("department", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("marital_status", StringType(), True),
        StructField("birth_date", DateType(), True),
        StructField("blood_group", StringType(), True),
        StructField("height", IntegerType(), True),
        StructField("weight", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("profession", StringType(), True),
        StructField("local", StringType(), True),
    ]
)
window_spec = Window.partitionBy("department").orderBy(desc("salary"))
df = worker.withColumn("rank", dense_rank().over(window_spec))
final_df = df.filter(col("rank") == 1).select(
    col("department"),
    col("salary"),
    concat_ws(" ", col("first_name"), col("last_name")).alias("employee_name"),
)
final_df.toPandas()
