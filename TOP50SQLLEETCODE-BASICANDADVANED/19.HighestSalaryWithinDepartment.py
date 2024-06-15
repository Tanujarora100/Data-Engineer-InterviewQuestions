# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

window = Window.partitionBy("department").orderBy(desc("salary"))

employee = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("worker_title", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("joining_date", TimestampType(), True),
    ]
)
df = employee
df = df.withColumn("rank", row_number().over(window))
filtered_df = df.filter(col("rank") == 1).select("department", "first_name", "salary")
filtered_df.toPandas()

sql_query = """
with cte as(
select *, row_number() over(partition by department order by salary desc) as rank
from worker
)
select department, first_name, salary
from cte
where rank=1
"""
