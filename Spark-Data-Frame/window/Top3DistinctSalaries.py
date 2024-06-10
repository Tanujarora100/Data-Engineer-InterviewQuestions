# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

twitter_employee = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("joining_date", TimestampType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("joining_date", TimestampType(), True),
        StructField("department", StringType(), True),
    ]
)
df = twitter_employee
distinct = df.dropDuplicates(["department", "salary"])
window_spec = Window.partitionBy(col("department")).orderBy(desc(col("salary")))

top3 = (
    distinct.withColumn("rank", dense_rank().over(window_spec))
    .filter(col("rank") <= 3)
    .orderBy("department", desc("salary"))
)

final_df = top3.select("department", "salary").toPandas()
