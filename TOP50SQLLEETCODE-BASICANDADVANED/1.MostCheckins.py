from pyspark.sql.functions import col, sum
from pyspark.sql.types import *

yelp_checkin = StructType(
    [
        StructField("business_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("checkins", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("minute", IntegerType(), True),
        StructField("second", IntegerType(), True),
        StructField("weekday", StringType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("day_of_month", IntegerType(), True),
        StructField("day_of_year", IntegerType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("week_of_month", IntegerType(), True),
    ]
)
df = yelp_checkin
df = df.groupBy("business_id").agg(sum("checkins").alias("total_checkins"))
final_df = df.orderBy("total_checkins", ascending=False).limit(5)
final_df.toPandas()

sql_query = """
SELECT business_id, SUM(checkins) AS TOTAL_CHECKINS
FROM yelp_checkin
GROUP BY business_id
ORDER BY TOTAL_CHECKINS DESC
LIMIT 5

"""
