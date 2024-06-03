# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
worker= StructType([
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('worker_title', StringType(), True),
    StructField('salary', IntegerType(), True),
    StructField('joining_date', StringType(), True),
    StructField('department', StringType(), True)
])
df = worker
df = df.withColumn('joining_date', to_date(col('joining_date')))
df = df.filter(month(col('joining_date')) == 6)
final_df = df.filter((col('worker_id') % 2 == 0)).toPandas()
# worker.toPandas()