# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

my_uber_drives = StructType([
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("category", StringType(), True),
    StructField("start", StringType(), True),
    StructField("stop", StringType(), True),
    StructField("miles", FloatType(), True),
    StructField("purpose", StringType(), True)
])
df= my_uber_drives
df= df.filter(col('category')=='Business')
grouped_df= df.groupBy(col('purpose')).agg(sum(col('miles')).alias('miles_sum'))
final_df= grouped_df.orderBy(col('miles_sum').desc()).limit(3).toPandas()
