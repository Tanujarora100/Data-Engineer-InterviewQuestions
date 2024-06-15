# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

window = Window.orderBy(desc("salary"))

worker = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("worker_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("joining_date", TimestampType(), True),
    ]
)
df = worker

df = df.withColumn("rank", rank().over(window))
filtered_df = df.filter(col("rank") == 5).select("salary")
filtered_df.toPandas()

sql_query = """
with cte as(
select *, rank() over(order by salary desc) as rank
from worker)
select salary from worker where rank=5
"""
