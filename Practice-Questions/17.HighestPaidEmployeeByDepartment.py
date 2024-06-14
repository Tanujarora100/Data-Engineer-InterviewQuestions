# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
window= Window.partitionBy('department').orderBy(desc('salary'))

worker= StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("worker_title", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("joining_date", TimestampType(), True),
])
df= worker
df= df.withColumn('rank',dense_rank().over(window))
filtered_df=df.filter(col('rank')==1).select('department','worker_id','salary')
filtered_df.toPandas()

sql_query="""
with cte as(
select *, dense_rank() over(partition by department order by salary desc) as rank
from worker
)
select department, worker_id, salary
from cte
where rank=1
"""

