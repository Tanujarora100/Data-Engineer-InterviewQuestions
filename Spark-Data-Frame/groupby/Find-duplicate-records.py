# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

title = StructType([
    StructField("worker_ref_id", IntegerType(), True),
    StructField("worker_title", StringType(), True),
    StructField("affected_from", TimestampType(), True)
])
df= title

df= df.select('worker_title','affected_from')
df= df.groupBy('worker_title','affected_from').agg(count(col('worker_title')).alias('n_affected'))
final_df= df.filter(col('n_affected')!=1)
final_df.toPandas()