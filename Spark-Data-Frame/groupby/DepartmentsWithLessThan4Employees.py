# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
# Start writing code
worker= StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("worker_title", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("joining_date", TimestampType(), True),
    StructField("department", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("race", StringType(), True),
    StructField("date_of_birth", TimestampType(), True),
    StructField("bank_account_number", IntegerType(), True),
    StructField("bank_name", StringType(), True),
    StructField("bank_branch_name", StringType(), True),
    StructField("bank_branch_code", IntegerType(), True),
])


df= worker
df=df.groupBy('department').agg(count(col('worker_id')).alias('total_workers'))
final_df= df.filter(col('total_workers')<4)
final_df.select('department','total_workers').toPandas()