# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start writing code
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

title = StructType(
    [
        StructField("worker_ref_id", IntegerType(), True),
        StructField("worker_title", StringType(), True),
    ]
)
df = worker.join(title, title.worker_ref_id == worker.worker_id, "inner")
# df.show()
# To validate your solution, convert your final pySpark df to a pandas df
filtered_df = df.filter(lower(col("worker_title")).like("%manager%"))
# filtered_df.show()
final_df = filtered_df.select("first_name", "worker_title")
final_df.show()
final_df.toPandas()
# final_df.show()
# worker.toPandas()
